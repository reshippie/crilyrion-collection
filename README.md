# crilyrion-collection
It's my Criterion Collection.  Actually, it's my whole collection.
Dump the list of albums from Lyrion and upload to a markdown file in an B2/S3 bucket.
In my case, the bucket contains my Obsidian vault.  Now I can have a list of all CDs I own
synced to my phone.

# Example .crilerion file
```
[default]
keyid = a0a0a0a0a0a0a0a0a0a0a0a0a
applicationkey = Keys2daVault
bucket_endpoint = https://s3.us-east-0005.backblazeb2.com
bucket_name = ObsidianVault
db_path = /path/to/your/lyrion/cache/library.db
md_file_path = /home/username/collection.md
remote_path = cd-bucket
# cleanup = yes # uncomment to remove file
```
