#!/usr/bin/env python

import sys
import cgi
import cgitb
import json
import sqlite3
import re
import wiggletools.wiggleDB

DEBUG = False
CONFIG_FILE = '/data/wiggletools/wiggletools.conf'

config = wiggletools.wiggleDB.read_config_file(CONFIG_FILE)
cgitb.enable(logdir=config['logdir'])

#try: # Windows needs stdio set for binary mode.
#    import msvcrt
#    msvcrt.setmode (0, os.O_BINARY) # stdin  = 0
#    msvcrt.setmode (1, os.O_BINARY) # stdout = 1
#except:
#    pass

class WiggleDBOptions(object):
	def __init__(self):
		self.conn = None
		self.wa  = None
		self.working_directory = None
		self.s3 = None
		self.wb = None
		self.a = None
		self.b = None
		self.dry_run = DEBUG
		self.remember = False
		self.db = config['database_location']
		self.config = CONFIG_FILE
		self.userid = None
		

def report_result(result):
	base_url = 'http://s3-%s.amazonaws.com/%s/' % (config['s3_region'], config['s3_bucket'])
	url = re.sub(config['working_directory'], base_url, result['location'])		
	if result['location'][-3:] == ".bw" or result['location'][-3:] == ".bb" or result['location'][-4:] == ".bed":
		ensembl = 'http://%s/%s/Location/View?g=%s;contigviewbottom=url:%s' % (config['ensembl_server'], config['ensembl_species'], config['ensembl_gene'], url)
	else:
		ensembl = url + ".png"
	print json.dumps({'status':result['status'], 'url':url, 'view':ensembl})

def main():
	print "Content-Type: application/json"
	print

	try:
		form = cgi.FieldStorage()
		conn = sqlite3.connect(config['database_location'])
		cursor = conn.cursor()
		if "count" in form:
			params = dict((re.sub("^._", "", X), form.getlist(X)) for X in form if X != "count")
			count = len(wiggletools.wiggleDB.get_dataset_locations(cursor, params))
			print json.dumps({'query':params,'count':count})

		elif 'annotations' in form:
			print json.dumps({"annotations": [X[0] for X in wiggletools.wiggleDB.get_annotations(cursor)]})

		elif 'uploadUrl' in form:
			print json.dumps(wiggletools.wiggleDB.upload_dataset(cursor, config['working_directory'], form['uploadUrl'].value, form['description'].value, form['userid'].value))

		elif 'uploadFile' in form:
			print json.dumps(wiggletools.wiggleDB.save_dataset(cursor, config['working_directory'], form['file'], form['description'].value, form['userid'].value))

		elif 'provenance' in form:
			print json.dumps(wiggletools.wiggleDB.get_annotation_dataset_description(cursor, form['provenance'].value))

		elif 'myannotations' in form:
			print json.dumps(wiggletools.wiggleDB.get_user_datasets(cursor, form['userid'].value))

		elif 'wa' in form:
			options = WiggleDBOptions()
			options.wa = form['wa'].value
			options.working_directory = config['working_directory']
			options.s3 = config['s3_bucket']
			if 'userid' in form:
				options.userid = form['userid'].value
			if 'email' in form:
				options.emails = form.getlist('email')

			if 'wb' in form:
				options.wb = form['wb'].value
			else:
				options.wb = None

			if 'w' in form:
				options.fun_merge = form['w'].value
			else:
				options.fun_merge = None

			options.a = dict((X[2:], form.getlist(X)) for X in form if X[:2] == "A_")
			options.b = dict((X[2:], form.getlist(X)) for X in form if X[:2] == "B_")
			if len(options.b.keys()) == 0:
				options.b = None

			result = wiggletools.wiggleDB.request_compute(conn, cursor, options, config)
			if result['status'] == 'DONE':
				report_result(result)
			else:
				print json.dumps(result)

		else:
			print json.dumps("No params, no output")

		conn.commit()
		conn.close()
	except:
		print ">>>>>>>>>>>>>>>>>>>>>>>>"
		print form
		print form.keys()
		print json.dumps("ERROR")
		raise

if __name__ == "__main__":
	main()
