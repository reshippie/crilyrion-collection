#! /usr/bin/env python

import argparse
import boto3
import configparser
import json
import logging
import os
import sqlite3


def create_parser():
    '''
        Define the command line arguments.
    '''
    parser = argparse.ArgumentParser()
    default_config = f"{os.environ['HOME']}/.crilyrion"
    parser.add_argument('-c', '--config', default=default_config, help='Where to find your config file.')
    parser.add_argument('-v', '--verbose', action='store_true', help="Do you want to know what's going on?")
    return parser.parse_args()

def write_markdown(config):
    '''
        Dump the data from SQLite into a nice markdown file.
    '''
    logging.info(f"Reading data from {config['db_path']}.")
    cur = sqlite3.connect(config['db_path']).cursor()
    data = cur.execute('select contributors.name, albums.title from albums inner join  contributors on albums.contributor=contributors.id').fetchall()
    collection = {}
    for artist, album in data:
        if artist in collection:
            collection[artist].append(album)
        else:
            collection[artist] = [album]

    logging.info(f"Writing data to {config['md_file_path']}.")
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
    args = create_parser()
    if args.verbose:
        logging.basicConfig(level='INFO')
    configs = configparser.ConfigParser()
    configs.read(args.config)
    config = configs['default']
    write_markdown(config)
    b2 = get_b2_connection(config)
    logging.info(f"Uploading file to {config['bucket_endpoint']}/{config['remote_path']}.")
    upload_file(config, b2)
    try:
        if config['cleanup'].lower() == 'yes':
            logging.info(f"Deleting markdown file {config['md_file_path']}.")
            os.unlink(config['md_file_path'])
        else:
            logging.info(f"Leaving markdown file {config['md_file_path']} alone.")
    except KeyError:
        logging.info(f"Leaving markdown file {config['md_file_path']} alone.")
    
if __name__ == '__main__':
    main()
