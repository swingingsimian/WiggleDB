Installing WiggleDB
===================

Install WiggleTools
-------------------

```
git clone https://github.com/Ensembl/WiggleTools.git
cd WiggleTools
sudo sh ./easy_install.sh
chmod 755 bin/* python/wiggletools/*
cp -r python/wiggletools/ /usr/local/lib/python-2.7/dist-packages
cd ..
```
Install WiggleDB
----------------

```
git clone https://github.com/Ensembl/WiggleDB.git
cd WiggleDB
chmod 755 python/wiggleDB/* cgi/*
chmod 644 gui/*
cp -r python/wiggletools/ /usr/local/lib/python-2.7/dist-packages
cd ..
```

Prepare data
------------

Prepare a tab-delimited file. The first five columns headers must be:	

```
location	name	type	annotation	assembly
```
All other columns are your business. You may want to select meaningful headers, and remove columns with too many values.

Prepare a tab-delimited file with chromosome lengths and ensure it has 644 permissions, e.g.:

```
chr1	249250621
chr10	135534747
chr11	135006516
chr12	133851895
chr13	115169878
chr14	107349540
chr15	102531392
```

Create SQLite3 database:

```	
wiggleDB.py --database /path/to/database.sqlite3 --load /path/to/datasets.tsv
wiggleDB.py --database /path/to/database.sqlite3 --load assembly_name /path/to/chromosome.lengths
chmod 777 /path/to/database.sqlite3
```

Install aws CLI
---------------

If you wish to push final results to S3 storage, create a config file with credentials, readable by all users.

Prepare server
--------------

1. Create/choose log directory, ensure 777 permissions
2. Create/choose tmp directory, ensure 777 permissions
3. Create your own config file (see example in conf/wiggletools.conf) (Ensure 644 permissions)
4. Copy the content of cgi/ to your Apache CGI directory, and edit the top of CGI file, so that it points to your config file. 
5. Test by running wiggleCGI.py on the command line, without parameters
6. Create a metadata json file (it must be accessible via http):

```
	wiggleDB.py --database /path/to/database.sqlite3 --attributes > attributes.json 
```
7. Copy the content of gui/ to your Apache web directory, and check the URLs at the top of the Javascript file

