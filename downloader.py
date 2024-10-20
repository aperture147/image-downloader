import pymysql
from configparser import ConfigParser
import argparse
import boto3
import requests
from requests.exceptions import HTTPError
from requests.adapters import HTTPAdapter, Retry

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time, perf_counter, sleep
import shutil
import math
import random
import re
import csv
import phpserialize
from bs4 import BeautifulSoup

URL_UNSAFE_CHARACTER_REGEX = r'[^a-zA-Z0-9\-_\.]'

IDS_FILE = 'ids.txt'
CHECKPOINT_FILE = 'checkpoint.txt'
POST_IMAGE_CSV_FILE = 'post_image.csv'
POST_META_IMAGE_CSV_FILE = 'post_meta_image.csv'
POST_CONTENT_CSV_FILE = 'post_content.csv'
CHUNK_SIZE = 50

MAX_POOL_SIZE = 50

session = requests.Session()
retries = Retry(backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries, pool_maxsize=MAX_POOL_SIZE)
session.mount('https://', adapter)
session.mount('http://', adapter)

parser = argparse.ArgumentParser(
    prog='Image Downloader',
    description='Image Downloader from Wordpress Database'
)
parser.add_argument(
    '--all', action='store_true',
    help='download images from all posts')
parser.add_argument(
    '--dryrun', action='store_true',
    help="No update database")
args = parser.parse_args()
download_all = args.all
dry_run = args.dryrun

config = ConfigParser()
config.read('config.ini')

mysql_config = config['mysql']
s3_config = config['s3']
downloader_config = config['downloader']

post_content_image = bool(int(downloader_config['post_content_image']))
post_meta_image = bool(int(downloader_config['post_meta_image']))
post_attachment_image = bool(int(downloader_config['post_attachment_image']))
allowed_extensions_str = downloader_config['allowed_extensions']
allowed_extensions_set = set(f'.{x.strip()}' for x in allowed_extensions_str.split(','))

db_conn = pymysql.connect(
    host=mysql_config['host'], port=int(mysql_config['port']),
    user=mysql_config['user'], passwd=mysql_config['password'], 
    db=mysql_config['db_name'],
    connect_timeout=31536000,
    autocommit=False
)
table_prefix = mysql_config['table_prefix']
print('db connected')

s3_cdn_url = s3_config['cdn_url']
s3_bucket_name = s3_config['bucket_name']
s3_client = boto3.client(
    's3',
    endpoint_url = s3_config['endpoint_url'],
    aws_access_key_id = s3_config['access_key_id'],
    aws_secret_access_key = s3_config['secret_access_key']
)
print('s3 client created')

def get_taxonomy(post_id_list):
    with db_conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT t.term_id, tt.term_taxonomy_id, tt.parent, t.name, t.slug, tt.taxonomy
            FROM {table_prefix}terms AS t
            JOIN {table_prefix}term_taxonomy AS tt ON tt.term_id = t.term_id
        """)
        
        term_taxonomy_result = cur.fetchall()
        print('all terms fetched')
        term_taxonomy_dict = {
            term_id: taxonomy_id
            for term_id, taxonomy_id, *_ in term_taxonomy_result
        }
        taxonomy_dict = {
            taxonomy_id: (term_name, term_slug, taxonomy_name, term_taxonomy_dict.get(parent_id))
            for _, taxonomy_id, parent_id, term_name, term_slug, taxonomy_name in term_taxonomy_result
        }
        print('taxonomy adjacent list built')
        cur.execute(f"""
            SELECT object_id, term_taxonomy_id
            FROM {table_prefix}term_relationships
            WHERE object_id IN %s
        """, (post_id_list,))
        term_relationship_list = cur.fetchall()
        print('term relationship fetched')
        post_taxonomy_dict = {}
        for post_id, taxonomy_id in term_relationship_list:
            post_taxonomy = post_taxonomy_dict.setdefault(post_id, {})
            _, _, taxonomy_name, _ = taxonomy_dict[taxonomy_id]
            post_taxonomy.setdefault(taxonomy_name, []).append(taxonomy_id)
        
        print('post taxonomy list has been built')

        return taxonomy_dict, post_taxonomy_dict

def get_post_name(post_id_list):
    with db_conn.cursor() as cur:
        cur.execute(f"""
            SELECT p.id, p.post_name
            FROM {table_prefix}posts AS p
            WHERE p.id IN %s
        """, (post_id_list,))

        post_name = cur.fetchall()
    return post_name

def get_external_image_list(post_id_list):
    with db_conn.cursor() as cur:
        cur.execute(f"""
            SELECT p.id, pm.meta_id, pm.meta_value
            FROM {table_prefix}posts AS p
            JOIN {table_prefix}postmeta AS pm
                ON p.ID = pm.post_id
                AND pm.meta_key = '_external_images'
            WHERE p.id IN %s
        """, (post_id_list,))

        external_image_list = cur.fetchall()
    return external_image_list

def get_post_content_list(post_id_list):
    with db_conn.cursor() as cur:
        cur.execute(f"""
            SELECT p.id, p.post_content
            FROM {table_prefix}posts AS p
            WHERE p.id IN %s
        """, (post_id_list,))

        post_thumb_list = cur.fetchall()
    return post_thumb_list

def get_image_attachment_list(post_id_list):
    with db_conn.cursor() as cur:
        cur.execute(f"""
            SELECT p.id, image_p.id, image_p.guid
            FROM {table_prefix}posts AS p
            JOIN {table_prefix}posts AS image_p
                ON p.ID = image_p.post_parent
                AND image_p.post_type = 'attachment'
                AND image_p.post_mime_type LIKE %s
            WHERE p.post_type IN ('post', 'product') AND p.id IN %s
        """, ('image/%', post_id_list))
        
        attachment_list = cur.fetchall()
    return attachment_list

def backup_id_and_checkpoint(now = int(time())):
    if os.path.isfile(IDS_FILE):
        shutil.copy(IDS_FILE, f'backup-{now}-{IDS_FILE}')
        os.remove(IDS_FILE)
    if os.path.isfile(CHECKPOINT_FILE):
        shutil.copy(CHECKPOINT_FILE, f'backup-{now}-{CHECKPOINT_FILE}')
        os.remove(CHECKPOINT_FILE)

def backup_post_image_csv(now = int(time())):
    if os.path.isfile(POST_IMAGE_CSV_FILE):
        shutil.copy(POST_IMAGE_CSV_FILE, f'backup-{now}-{POST_IMAGE_CSV_FILE}')

def init_post_image_csv():
    with open(POST_IMAGE_CSV_FILE, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'old_link', 'new_link'])

def backup_post_meta_image_csv(now = int(time())):
    if os.path.isfile(POST_META_IMAGE_CSV_FILE):
        shutil.copy(POST_META_IMAGE_CSV_FILE, f'backup-{now}-{POST_META_IMAGE_CSV_FILE}')

def init_post_meta_image_csv():
    with open(POST_META_IMAGE_CSV_FILE, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'old_data', 'new_data'])

def init_post_content_csv():
    with open(POST_CONTENT_CSV_FILE, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'old_content', 'new_content'])

def backup_post_content_csv(now = int(time())):
    if os.path.isfile(POST_CONTENT_CSV_FILE):
        shutil.copy(POST_CONTENT_CSV_FILE, f'backup-{now}-{POST_CONTENT_CSV_FILE}')

def append_post_image_csv(post_list):
    with open(POST_IMAGE_CSV_FILE, 'a') as f:
        writer = csv.writer(f)
        writer.writerows(post_list)

def append_post_meta_image_csv(meta_list):
    with open(POST_META_IMAGE_CSV_FILE, 'a') as f:
        writer = csv.writer(f)
        writer.writerows(meta_list)

def append_post_content_csv(content_list):
    with open(POST_CONTENT_CSV_FILE, 'a') as f:
        writer = csv.writer(f)
        writer.writerows(content_list)

def get_full_post_id_list():
    print('fetching all post')
    with db_conn.cursor() as cur:
        cur.execute(f"""
            SELECT id FROM {table_prefix}posts
            WHERE post_type IN ('post', 'product') AND post_status = 'publish'
        """)
        result = cur.fetchall()
    id_list = [x for x, in result]
    now = int(time())
    backup_id_and_checkpoint(now)
    backup_post_image_csv(now)
    backup_post_meta_image_csv(now)
    backup_post_content_csv(now)
    init_post_image_csv()
    init_post_meta_image_csv()
    init_post_content_csv()
    with open(IDS_FILE, 'a') as f:
        for post_id in id_list[:-1]:
            f.write(f'{post_id}\n')
        f.write(f'{id_list[-1]}')
    return id_list

def get_post_id_list_from_file():
    with open(IDS_FILE) as id_f:
        return [post_id.strip() for post_id in id_f.readlines() if post_id]

def write_checkpoint(last_chunk):
    with open(CHECKPOINT_FILE, 'w') as ckpt_f:
        ckpt_f.write(str(last_chunk))

def read_checkpoint():
    if not os.path.isfile(CHECKPOINT_FILE):
        return 0
    try:
        with open(CHECKPOINT_FILE) as ckpt_f:
            last_chunk = ckpt_f.read().strip()
        return int(last_chunk)
    except ValueError as e:
        print('corrupted checkpoint file, reset to 0')
        return 0

def get_ext_from_img_src(img_src, default='.jpg'):
    _, ext = os.path.splitext(img_src)
    return ext or default

def download_and_put_image_to_s3(image_url, s3_object_key):
    print(f'downloading {image_url} to {s3_object_key}')
    try:
        r = session.get(image_url, allow_redirects=True)
        r.raise_for_status()
        img_content = r.content
    except HTTPError as e:
        if e.response.status_code in [400, 401, 403, 404]:
            return False
        raise e
    resp = s3_client.put_object(
        Bucket=s3_bucket_name,
        Key=s3_object_key,
        Body=img_content,
    )
    if resp['ResponseMetadata']['HTTPStatusCode'] >= 300:
        print(resp['ResponseMetadata'])
        raise Exception('failed to put data to S3')
    print(f'image put to {s3_object_key}')
    return True
    # print(s3_object_key)
    # return False

def put_post_image(image_id, image_url, s3_object_key):
    exists = download_and_put_image_to_s3(image_url, s3_object_key)
    if not exists:
        return image_id, image_url, None
    return image_id, image_url, s3_object_key

def check_image_url_and_get_extension(image_url):
    if 'hoaxuyenviet.vn' in image_url:
        print('hoaxuyenviet.vn is dead,', image_url, 'skipped')
        return None
    ext = get_ext_from_img_src(image_url)
    if (ext not in allowed_extensions_set) and (not image_url.startswith('https://drive.google.com/uc')):
        print('not allowed extension', ext, 'in', image_url)
        return None
    return ext

def put_post_content_image(post_id, safe_post_name, image_obj_prefix, post_content):
    soup = BeautifulSoup(post_content, "lxml")

    img_tag_list = soup.find_all('img')
    for index, img_tag in enumerate(img_tag_list):
        if 'src' not in img_tag.attrs:
            print('no image in', str(img_tag))
            continue
        image_url: str = img_tag.attrs['src']
        
        ext = check_image_url_and_get_extension(image_url)
        if not ext:
            continue
        s3_object_key = os.path.join(image_obj_prefix, f'{safe_post_name}-content-{str(index).rjust(3, "0")}{ext}')
        
        exists = download_and_put_image_to_s3(image_url, s3_object_key)
        if not exists:
            continue
        img_tag.attrs['src'] = os.path.join(s3_cdn_url, s3_object_key)
        
    return post_id, post_content, str(soup)

def put_post_meta_image(meta_id, safe_post_name, image_obj_prefix, post_meta_str):
    image_link_dict = phpserialize.loads(post_meta_str.encode(), decode_strings=True)
    data_dict = {}
    for index, image_url in image_link_dict.items():
        ext = check_image_url_and_get_extension(image_url)
        if not ext:
            data_dict[index] = image_url
            continue
        s3_object_key = os.path.join(image_obj_prefix, f'{safe_post_name}-preview-{str(index).rjust(3, "0")}{ext}')
        exists = download_and_put_image_to_s3(image_url, s3_object_key)
        if not exists:
            data_dict[index] = image_url
            continue
        data_dict[index] = os.path.join(s3_cdn_url, s3_object_key)
    new_serialized_meta = phpserialize.dumps(data_dict).decode()
    return meta_id, post_meta_str, new_serialized_meta

def main():
    start_time = time()
    if download_all:
        post_id_list = get_full_post_id_list()
    else:
        post_id_list = get_post_id_list_from_file()
    print('total posts', len(post_id_list))
    chunk_count = math.ceil(len(post_id_list) / CHUNK_SIZE)
    print('total chunk', chunk_count)
    print('post content image:', post_content_image)
    print('post meta image:', post_meta_image)
    print('post attachment image:', post_attachment_image)
    
    last_chunk = read_checkpoint()
    if last_chunk:
        print('last checkpoint chunk:', last_chunk)
        last_chunk += 1
    else:
        backup_post_image_csv()
        backup_post_meta_image_csv()
        backup_post_content_csv()
        init_post_image_csv()
        init_post_meta_image_csv()
        init_post_content_csv()
    
    for i in range(last_chunk, chunk_count):
        print('processing chunk', i)
        with ThreadPoolExecutor() as executor:
            chunk = post_id_list[i * CHUNK_SIZE: (i+1) * CHUNK_SIZE]
            image_attachment_list = get_image_attachment_list(chunk) if post_attachment_image else []
            external_image_list = get_external_image_list(chunk) if post_meta_image else []
            post_content_list = get_post_content_list(chunk) if post_content_image else []
            post_name_list = get_post_name(chunk)

            print('total post:', len(post_name_list))

            start = perf_counter()
            
            taxonomy_dict, post_taxonomy_dict = get_taxonomy(chunk)
            
            params = []
            post_meta_params = []
            post_content_params = []
            post_list_rows = []
            post_meta_rows = []
            post_content_rows = []
            
            post_image_futures = []
            post_meta_image_futures = []
            post_content_futures = []

            image_obj_prefix_dict = {}
            safe_post_name_dict = {}
            
            for post_id, post_name in post_name_list:
                post_taxonomy = post_taxonomy_dict.get(post_id, {})
                post_category_id_list = post_taxonomy.get('category', [])
                if not post_category_id_list:
                    post_category_id_list = post_taxonomy.get('product_cat', [])
                term_slug_list = []
                for category_id in post_category_id_list:
                    term_slug_list.append(re.sub(URL_UNSAFE_CHARACTER_REGEX, '', taxonomy_dict[category_id][1])) # safe slug
                
                safe_post_name = re.sub(URL_UNSAFE_CHARACTER_REGEX, '', post_name)
                safe_post_name_dict[post_id] = safe_post_name
                image_obj_prefix_dict[post_id] = os.path.join('3d-model', *term_slug_list)

            for index, (post_id, image_id, image_link) in enumerate(image_attachment_list):
                ext = check_image_url_and_get_extension(image_link)
                if not ext:
                    continue
                image_obj_prefix = image_obj_prefix_dict[post_id]
                safe_post_name = safe_post_name_dict[post_id]
                image_obj_key = os.path.join(image_obj_prefix, f'{safe_post_name}-{str(index + 1).rjust(3, "0")}{ext}')
                post_image_futures.append(executor.submit(put_post_image, image_id, image_link, image_obj_key))
            for post_id, post_meta_id, post_meta_image_str in external_image_list:
                image_obj_prefix = image_obj_prefix_dict[post_id]
                safe_post_name = safe_post_name_dict[post_id]
                post_meta_image_futures.append(executor.submit(put_post_meta_image, post_meta_id, safe_post_name, image_obj_prefix, post_meta_image_str))
            for post_id, post_content in post_content_list:
                image_obj_prefix = image_obj_prefix_dict[post_id]
                safe_post_name = safe_post_name_dict[post_id]
                post_content_futures.append(executor.submit(put_post_content_image, post_id, safe_post_name, image_obj_prefix, post_content))
            print(f'total post image: {len(post_image_futures)}')
            print(f'total post meta image: {len(post_meta_image_futures)}')
            print(f'total post content image: {len(post_content_futures)}')
            for future in as_completed(post_image_futures):
                image_id, image_url, image_obj_key = future.result()
                if not image_obj_key:
                    print(f'image {image_id} is not found')
                    post_list_rows.append([image_id, image_url, image_url])
                    continue
                new_image_url = os.path.join(s3_cdn_url, image_obj_key)
                post_list_rows.append([image_id, image_url, new_image_url])
                params.append((new_image_url, image_id))

            for future in as_completed(post_meta_image_futures):
                meta_id, old_meta_value, new_meta_value = future.result()
                post_meta_rows.append([meta_id, old_meta_value, new_meta_value])
                post_meta_params.append([new_meta_value, meta_id])

            for future in as_completed(post_content_futures):
                post_id, old_post_content, new_post_content = future.result()
                post_content_rows.append([post_id, old_post_content, new_post_content])
                post_content_params.append([new_post_content, post_id])

        if not dry_run:
            db_conn.ping(reconnect=True)
            with db_conn.cursor() as cur:
                if params:
                    cur.executemany(f'UPDATE {table_prefix}posts SET guid=%s WHERE id=%s', params)
                    print(f'post attachment updated')
                if post_meta_params:
                    cur.executemany(f'UPDATE {table_prefix}postmeta SET meta_value=%s WHERE meta_id=%s', post_meta_params)
                    print(f'post meta updated')
                if post_content_params:
                    cur.executemany(f'UPDATE {table_prefix}posts SET post_content=%s WHERE id=%s', post_content_params)
                    print(f'post content updated')
            
            db_conn.commit()
        end = perf_counter()
        print(f'finished chunk {i}/{chunk_count},', 'elapsed time', end - start, 'seconds')
        write_checkpoint(i)
        print('checkpoint saved')
        append_post_image_csv(post_list_rows)
        append_post_meta_image_csv(post_meta_rows)
        append_post_content_csv(post_content_rows)
        print('post csv updated')
        print('waiting for cooldown on 1-3s')
        sleep(1 + random.randint(0, 2))

    end_time = time()
    if os.path.isfile(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
    backup_id_and_checkpoint()
    print('total elapsed time', end_time - start_time, 'seconds')

if __name__ == '__main__':
    try:
        main()
    finally:
        db_conn.close()