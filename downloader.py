import pymysql
from configparser import ConfigParser
import argparse
import boto3
import requests
# from urllib.parse import urlparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time, perf_counter, sleep
import shutil
import math
import random



IDS_FILE = 'ids.txt'
CHECKPOINT_FILE = 'checkpoint.txt'
CHUNK_SIZE = 100

parser = argparse.ArgumentParser(
    prog='Image Downloader',
    description='Image Downloader from Wordpress Database'
)
parser.add_argument(
    '--all', action='store_true',
    help='download images from all posts')
args = parser.parse_args()
download_all = args.all

config = ConfigParser()
config.read('config.ini')

mysql_config = config['mysql']
s3_config = config['s3']

db_conn = pymysql.connect(
    host=mysql_config['host'], port=int(mysql_config['port']),
    user=mysql_config['user'], passwd=mysql_config['password'], 
    db=mysql_config['db_name'],
    connect_timeout=300,
    autocommit=False
)
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
        cur.execute("""
            SELECT DISTINCT t.term_id, tt.term_taxonomy_id, tt.parent, t.name, t.slug, tt.taxonomy
            FROM wp_terms AS t
            JOIN wp_term_taxonomy AS tt ON tt.term_id = t.term_id
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
        cur.execute("""
            SELECT object_id, term_taxonomy_id
            FROM wp_term_relationships
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
    
def get_thumbnail_link(post_id_list):
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT p.id, p.post_name, image_p.id, image_p.guid
            FROM wp_posts AS p
            JOIN wp_posts AS image_p
                ON p.ID = image_p.post_parent
                AND image_p.post_type = 'attachment'
                AND image_p.post_mime_type LIKE %s
            WHERE p.post_type IN ('post', 'product') AND p.id IN %s
        """, ['image/%', post_id_list])
        
        post_thumb_list = cur.fetchall()
        print('image count:', len(post_thumb_list))
    return post_thumb_list

def backup_id_and_checkpoint():
    now = int(time())
    if os.path.isfile(IDS_FILE):
        shutil.copy(IDS_FILE, f'ids-backup-{now}.txt')
        os.remove(IDS_FILE)
    if os.path.isfile(CHECKPOINT_FILE):
        shutil.copy(CHECKPOINT_FILE, f'checkpoint-backup-{now}.txt')
        os.remove(CHECKPOINT_FILE)

def get_full_post_id_list():
    print('fetching all post')
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT id FROM wp_posts
            WHERE post_type IN ('post', 'product')
        """)
        result = cur.fetchall()
    id_list = [x for x, in result]
    backup_id_and_checkpoint()
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

def put_image(image_id, image_url, s3_object_key):
    print(f'downloading {image_url} to {s3_object_key}')
    with requests.get(image_url, allow_redirects=True) as r:
        r.raise_for_status()
        img_content = r.content
    resp = s3_client.put_object(
        Bucket=s3_bucket_name,
        Key=s3_object_key,
        Body=img_content,
    )
    if resp['ResponseMetadata']['HTTPStatusCode'] >= 300:
        print(resp['ResponseMetadata'])
        raise Exception('failed to put data to S3')
    print(f'image put to {s3_object_key}')
    return image_id, s3_object_key

def main():
    start_time = time()
    if download_all:
        post_id_list = get_full_post_id_list()
    else:
        post_id_list = get_post_id_list_from_file()
    print('total posts', len(post_id_list))
    chunk_count = math.ceil(len(post_id_list) / CHUNK_SIZE)
    print('total chunk', chunk_count)
    last_chunk = read_checkpoint()
    if last_chunk:
        print('last checkpoint chunk:', last_chunk)
    with ThreadPoolExecutor() as executor:
        for i in range(last_chunk, chunk_count):
            chunk = post_id_list[i * CHUNK_SIZE: (i+1) * CHUNK_SIZE]
            post_thumb_list = get_thumbnail_link(chunk)
            print('processing chunk', i)
            start = perf_counter()
            chunk = list(set(post_id for post_id, *_ in post_thumb_list))
            taxonomy_dict, post_taxonomy_dict = get_taxonomy(chunk)

            params = []
            post_image_counter_dict = {}
            futures = []
            for post_id, post_name, image_id, image_link in post_thumb_list:
                post_category_id_list = post_taxonomy_dict.get(post_id, {}).get('product_cat', [])
                term_slug_list = []
                for category_id in post_category_id_list:
                    term_slug_list.append(taxonomy_dict[category_id][1])
                image_number = post_image_counter_dict.setdefault(post_id, 1)
                image_obj_key = os.path.join('3d-model', *term_slug_list, f'{post_name}-{str(image_number).rjust(3, "0")}.jpg')
                post_image_counter_dict[post_id] = image_number + 1
                futures.append(executor.submit(put_image, image_id, image_link, image_obj_key))

            for future in as_completed(futures):
                image_id, image_obj_key = future.result()
                new_image_url = os.path.join(s3_cdn_url, image_obj_key)
                params.append((new_image_url, image_id))
            
            with db_conn.cursor() as cur:
                cur.executemany('UPDATE wp_posts SET guid=%s WHERE id=%s', params)
            
            db_conn.commit()
            end = perf_counter()
            print(f'finished chunk {i}/{chunk_count},', 'elapsed time', end - start, 'seconds')
            write_checkpoint(i)
            print('checkpoint saved')
            print('wait for cooldown on 3-5s')
            
            sleep(3 + random.randint(0, 2))

    end_time = time()
    if os.path.isfile(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
    print('total elapsed time', end_time - start_time, 'seconds')

if __name__ == '__main__':
    try:
        main()
    finally:
        db_conn.close()