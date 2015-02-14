#!/usr/bin/env python

# Copyright 2013 EMBL-EBI
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import cgi
import cgitb
import json
import sqlite3
import re
import wiggledb.wiggleDB

DEBUG = False
CONFIG_FILE = '/data/wiggletools/wiggletools.conf'

config = wiggletools.wiggleDB.read_config_file(CONFIG_FILE)
cgitb.enable(logdir=config['logdir'])

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
		self.emails = None
		

def report_result(result):
	base_url = 'http://s3-%s.amazonaws.com/%s/' % (config['s3_region'], config['s3_bucket'])
	url = re.sub(config['working_directory'], base_url, result['location'])		
	if result['location'][-3:] == ".bw" or result['location'][-3:] == ".bb":
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
			print json.dumps({"annotations": [X[1] for X in wiggletools.wiggleDB.get_annotations(cursor)]})

		elif 'uploadUrl' in form:
			print json.dumps(wiggletools.wiggleDB.upload_dataset(cursor, config['working_directory'], form['uploadUrl'].value, form['description'].value))

		elif 'provenance' in form:
			print json.dumps(wiggletools.wiggleDB.get_annotation_dataset_description(cursor, form['provenance'].value))

		elif 'wa' in form:
			options = WiggleDBOptions()
			options.wa = form['wa'].value
			options.working_directory = config['working_directory']
			options.s3 = config['s3_bucket']
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

			if options.a['type'] == 'regions' and options.b['type'] == 'signal':
				tmp = options.b
				options.b = options.a
				options.a = tmp
			
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
                print json.dumps("ERROR")
		raise

if __name__ == "__main__":
	main()
