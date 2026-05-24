# crilyrion-collection
It's my Criterion Collection.  Actually, it's my whole collection.

More than once I've bought a CD that I didn't realize I already
owned.
> There has to be a better way!

I use Lyrion to manage all of my CDs, which stores all of its data
in a SQLite db.  This script dumps the list of albums and uploads to a markdown file in a B2/S3 bucket.
In my case, the bucket contains my Obsidian vault.  Now I can have a list of all CDs I own
synced to any place I can read my vault, like my phone.

# Example .crilyrion file
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
