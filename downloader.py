import pymysql
from configparser import ConfigParser
import argparse
import boto3
import requests
from urllib.parse import urlparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

parser = argparse.ArgumentParser(
    prog='Image Downloader',
    description='Image Downloader from Wordpress Database'
)
args = parser.parse_args()

config = ConfigParser()
config.read('config.ini')

wordpress_config = config['wordpress']
mysql_config = config['mysql']
s3_config = config['s3']

wordpress_host = wordpress_config['host']

db_conn = pymysql.connect(
    host=mysql_config['host'], port=int(mysql_config['port']),
    user=mysql_config['user'], passwd=mysql_config['password'], 
    db=mysql_config['db_name'],
    connect_timeout=120,
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
    
def get_thumbnail_link():
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.post_name, image_p.id, image_p.guid
            FROM wp_posts AS p
            JOIN wp_posts AS image_p
                ON p.ID = image_p.post_parent
                AND image_p.post_type = 'attachment'
                AND image_p.post_mime_type LIKE 'image/%'
            WHERE p.post_type IN ('post', 'product') LIMIT 100
        """)
        
        post_thumb_list = cur.fetchall()
    return post_thumb_list

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
    post_thumb_list = get_thumbnail_link()
    post_id_list = list(set(post_id for post_id, *_ in post_thumb_list))
    taxonomy_dict, post_taxonomy_dict = get_taxonomy(post_id_list)
    params = []
    post_image_counter_dict = {}
    with ThreadPoolExecutor() as executor:
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

if __name__ == '__main__':
    try:
        main()
    finally:
        db_conn.close()