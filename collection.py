#! /usr/bin/env python

import boto3
import configparser
import json
import os
import sqlite3

def write_markdown(config):
    cur = sqlite3.connect(config['db_path']).cursor()
    data = cur.execute("select contributors.name, albums.title from albums inner join  contributors on albums.contributor=contributors.id").fetchall()
    collection = {}
    for artist, album in data:
        if artist in collection:
            collection[artist].append(album)
        else:
            collection[artist] = [album]

    with open(config['md_file_path'], 'w') as collectfile:
        json.dump(collection, collectfile, ensure_ascii=False, indent=2)

def get_b2_connection(config):
    b2 = boto3.resource(service_name='s3',
                        endpoint_url=config['bucket_endpoint'],
                        aws_access_key_id=config['keyid'],
                        aws_secret_access_key=config['applicationkey'])
    return b2

def upload_file(config, b2):
    file_path = config['md_file_path']
    file_name = os.path.basename(file_path)
    remote_path = config['remote_path'] + '/' + file_name
    bucket_name = config['bucket_name']
    res = b2.Bucket(bucket_name).upload_file(file_path, remote_path)

def main():
    configs = configparser.ConfigParser()
    configs.read('/home/billy/.obs-albums')
    config = configs['default']
    write_markdown(config)
    b2 = get_b2_connection(config)
    upload_file(config, b2)
    
if __name__ == '__main__':
    main()
