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
import re
import argparse
import sqlite3
import subprocess
import tempfile
import os
import os.path
import json
import math 
import wigglePlots
import numpy
import matplotlib
import matplotlib.pyplot as pyplot

verbose = False

###########################################
## Configuration file
###########################################

def read_config_file(filename):
	return dict(line.strip().split('\t') for line in open(filename) if line[0] != '#' and len(line) > 1)

###########################################
## Command line interface
###########################################

def normalise_spaces(string):
	if string is None:
		return None
	else:
		return re.sub("^\W+", "", re.sub("\W+", " ", string))

def get_options():
	parser = argparse.ArgumentParser(description='WiggleDB backend.')
	parser.add_argument('--db', '-d', dest='db', help='Database file',required=True)
	parser.add_argument('--wd', dest='working_directory', help='Data directory')
	parser.add_argument('-a',dest='a',help='A set of SQL constraints',nargs='*')
	parser.add_argument('-wa',dest='wa',help='WiggleTools command for A')
	parser.add_argument('-b',dest='b',help='A second set of SQL constraints',nargs='*')
	parser.add_argument('-wb',dest='wb',help='WiggleTools command for B')
	parser.add_argument('--wiggletools','-w',dest='fun_merge',help='Wiggletools command')
	parser.add_argument('--emails','-e',dest='emails',help='List of e-mail addresses for reminder',nargs='*')

	parser.add_argument('--load',dest='load',help='Datasets to load in database')
	parser.add_argument('--load_annotations',dest='load_annots',help='References to load in database')
	parser.add_argument('--clean',dest='clean',help='Delete cached datasets older than X days', type=int)
	parser.add_argument('--cache',dest='cache',help='Dump cache info', action='store_true')
	parser.add_argument('--datasets',dest='datasets',help='Print dataset info', action='store_true')
	parser.add_argument('--clear_cache',dest='clear_cache',help='Reset cache info', nargs='*')
	parser.add_argument('--remember',dest='remember',help='Preserve dataset from garbage collection', action='store_true')
	parser.add_argument('--dry-run',dest='dry_run',help='Do not run the command, print wiggletools command', action='store_true')
	parser.add_argument('--result','-r',dest='result',help='Return status or end result of job', type=int)
	parser.add_argument('--attributes','-t',dest='attributes',help='Print JSON hash of attributes and values', action='store_true')
	parser.add_argument('--verbose','-v',dest='verbose',help='Turn on status output',action='store_true')
	parser.add_argument('--config','-c',dest='config',help='Configuration file')
	parser.add_argument('--annotations','-n',dest='annotations',help='Print list of annotation names', action='store_true')
	parser.add_argument('--jobs','-j',dest='jobs',help='Print list of jobs',nargs='*')
	parser.add_argument('--upload','-u',dest='upload',help='Upload dataset')
	parser.add_argument('--description',dest='description',help='Uploaded dataset description',default='TEST')

	options = parser.parse_args()
	if all(X is None for X in [options.load, options.clean, options.result, options.datasets, options.clear_cache]) and not options.cache and not options.attributes and not options.annotations:
		assert options.a is not None, 'No dataset selection to run on'
		assert options.wa is not None, 'No dataset transformation to run on'
		if options.b is not None:
			assert options.fun_merge is not None, 'No action command (load,clean,compute) specified'

	options.wa = normalise_spaces(options.wa)	
	options.wb = normalise_spaces(options.wb)	
	options.fun_merge = normalise_spaces(options.fun_merge)

	if options.config is not None:
		config = read_config_file(options.config)
		if options.working_directory is None:
			options.working_directory = config['working_directory']
	else:
		config = None

	global verbose
	verbose = options.verbose
	return options, config

###########################################
## Creating a database
###########################################

def create_database(cursor, filename):
	if verbose:
		print 'Creating database'
	create_cache(cursor)
	create_dataset_table(cursor, filename)
	create_annotation_dataset_table(cursor)
	create_user_dataset_table(cursor)

def create_cache(cursor):
	cursor.execute('''
	CREATE TABLE IF NOT EXISTS
	cache
	(
	query varchar(10000) UNIQUE,
	location varchar(1000),
	last_query datetime
	)
	''')

def create_dataset_table(cursor, filename):
	file = open(filename)
	items = file.readline().strip().split('\t')
	assert items[0] == 'location', "Badly formed dataset table, please ensure the first column refers to location:\n" + items[0]
	header = '''
			CREATE TABLE IF NOT EXISTS 
			datasets 
			(
			location varchar(1000),
		 '''
	cursor.execute('\n'.join([header] + [",\n".join(['%s varchar(255)' % X for X in items[1:]])] + [')']))

	cursor.execute('SELECT * FROM datasets').fetchall()
	column_names = [X[0] for X in cursor.description]
	assert column_names == items, 'Mismatch between the expected columns: \n%s\nAnd the columns in file:\n%s' % ("\t".join(column_names), '\t'.join(items))

	for line in file:
		cursor.execute('INSERT INTO datasets VALUES (%s)' % ",".join("'%s'" % X for X in line.strip().split('\t')))
	file.close()

def create_annotation_dataset_table(cursor):
	header = '''
			CREATE TABLE IF NOT EXISTS 
			annotation_datasets 
			(
			location varchar(1000),
			name varchar(1000) UNIQUE,
 			description varchar(100)
			)
		 '''
	cursor.execute(header)

def create_user_dataset_table(cursor):
	header = '''
			CREATE TABLE IF NOT EXISTS 
			user_datasets 
			(
 			name varchar(100) UNIQUE,
			location varchar(1000)
			)
		 '''
	cursor.execute(header)

###########################################
## Garbage cleaning 
###########################################


def clean_database(cursor, days):
	for location in cursor.execute('SELECT location FROM cache WHERE julianday(\'now\') - julianday(last_query) > %i' % days).fetchall():
		if verbose:
			print 'Removing %s' % location[0]
		if os.path.exists(location[0]):
			os.remove(location[0])
	cursor.execute('DELETE FROM cache WHERE julianday(\'now\') - julianday(last_query) > %i' % days)

###########################################
## Search datasets
###########################################

def get_dataset_attributes_2(cursor):
	return [X[1] for X in cursor.execute('PRAGMA table_info(datasets)').fetchall()]

def get_dataset_attributes(cursor):
	return list(set(get_dataset_attributes_2(cursor)) - set(["location"]))

def get_attribute_values_2(cursor, attribute):
	return [X[0] for X in cursor.execute('SELECT DISTINCT %s FROM datasets' % (attribute)).fetchall()]

def get_attribute_values(cursor):
	return dict((attribute, get_attribute_values_2(cursor, attribute)) for attribute in get_dataset_attributes(cursor))

def get_annotations(cursor):
	return cursor.execute('SELECT * FROM annotation_datasets').fetchall()

def get_datasets(cursor):
	res = cursor.execute('SELECT * FROM datasets').fetchall()
	return [[X[0] for X in cursor.description]] + res

def attribute_selector(attribute, params):
	return "( %s )" % " OR ".join("%s=:%s_%i" % (attribute,attribute,index) for index in range(len(params[attribute])))

def denormalize_params(params):
	return dict(("%s_%i" % (attribute, index),value) for attribute in params for (index, value) in enumerate(params[attribute]))

def get_annotation_dataset_locations(cursor, names):
	locations = []
	for name in names:
		res = cursor.execute('SELECT location FROM annotation_datasets WHERE name=?', (name,)).fetchall()
		if len(res) > 0:
			locations.append(res[0][0])
		else:
			return []
	return locations

def get_annotation_dataset_description(cursor, name):
	res = cursor.execute('SELECT description FROM annotation_datasets WHERE name=?', (name,)).fetchall()
	if len(res) > 0:
		return {'name':name, 'description':res[0][0]}
	else:
		return {'ERROR'}

def get_user_dataset_locations(cursor, names):
	locations = []
	for name in names:
		res = cursor.execute('SELECT location FROM user_datasets WHERE name=?', (name,)).fetchall()
		if len(res) > 0:
			locations.append(res[0][0])
		else:
			return []
	return locations

def get_dataset_locations(cursor, params):
	# Quick check that all the keys are purely alphanumeric to avoid MySQL injections
	assert not any(re.match('\W', X) is not None for X in params)
	query = " AND ".join(attribute_selector(X, params) for X in params)
	if len(query) > 0:
		query = ' WHERE ' + query 
	if verbose:
		print 'Query: SELECT location FROM datasets' + query
		print 'Where:' + str(denormalize_params(params))
	res = cursor.execute('SELECT location FROM datasets' + query, denormalize_params(params)).fetchall()
	if verbose:
		print 'Found:\n' + "\n".join(X[0] for X in res)
	return sorted(X[0] for X in res)

def get_locations(cursor, params):
	if 'annot_name' in params:
		return get_annotation_dataset_locations(cursor, params['annot_name'])
	elif 'user_name' in params:
		return get_user_dataset_locations(cursor, params['user_name'])
	else:
		return get_dataset_locations(cursor, params)

def run(cmd):
	p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	ret = p.wait()
	out, err = p.communicate()
	if ret != 0:
		sys.stdout.write("Failed in running %s\n" % (cmd))
		sys.stdout.write("OUTPUT:\n%s\n" % (out))
		sys.stdout.write("ERROR:\n%s\n" % (out))
		raise
	return out

###########################################
## Search cache
###########################################

def reset_time_stamp(cursor, cmd):
	cursor.execute('UPDATE cache SET last_query= date(\'now\') WHERE query = \'%s\'' % cmd)


def get_precomputed_location(cursor, cmd):
	reports = cursor.execute('SELECT location FROM cache WHERE query = ?', (cmd,)).fetchall()
	if len(reports) > 0:
		reset_time_stamp(cursor, cmd)
		if verbose:
			print 'Found pre-computed file for query: %s' % cmd
			print reports[0]
		return reports[0][0]
	else:
		if verbose:
			print 'Did not find pre-computed file for query: %s' % cmd
		return None

def reuse_or_write_precomputed_location(cursor, cmd, working_directory):
	pre_location = get_precomputed_location(cursor, cmd)
	if pre_location is not None:
		return pre_location, pre_location, False
	else:
		fh, destination = tempfile.mkstemp(suffix='.bw',dir=working_directory)
		return 'write %s %s' % (destination, cmd), destination, True

def copy_to_longterm(data, config):
	if 's3_bucket' in config:
		os.environ['AWS_CONFIG_FILE'] = config['aws_config']
		run("aws s3 cp %s s3://%s/%s --acl public-read" % (data, config['s3_bucket'], os.path.basename(data)))

def make_barchart(counts, total, labels, out, format='pdf'):
	ind = numpy.arange(len(labels))    # the x locations for the groups
	width = 0.35       # the width of the bars: can also be len(x) sequence
	heights = [X/float(total) for X in counts]
	assert all(X <= 1 and X >= 0 for X in heights)
	errors = [ math.sqrt(2*X*(1-X) / total) for X in heights ]
	pyplot.bar(ind, heights, width, yerr=errors)
	pyplot.xticks(ind+width/2., labels)
	pyplot.savefig(out, format=format)

def launch_quick_compute(conn, cursor, fun_merge, fun_A, data_A, fun_B, data_B, options, config):
	cmd_A = " ".join([fun_A] + data_A + [':'])

	if data_B is not None:
		merge_words = fun_merge.split(' ')

		assert fun_merge is not None
		if fun_B is not None:
			cmd_B = " ".join([fun_B] + data_B + [':'])
		else:
			cmd_B = " ".join(data_B)

		if merge_words[0] == 'overlaps':
			assert "annot_name" in options.b
			fh, destination = tempfile.mkstemp(suffix='.txt',dir=options.working_directory)
			total = int(run("wiggletools write_bg - %s | wc -l" % cmd_A))
			if total > 0:
				counts = []
				for annotation in data_B:
					counts.append(int(run('wiggletools write_bg - overlaps %s %s | wc -l' % (annotation, cmd_A)).strip()))
				out = open(destination, "w")
				for name, count in zip(options.b['annot_name'], counts):
					out.write("\t".join(map(str, [name, count])) + "\n")
				out.write("\t".join(['ALL', str(total)]) + "\n")
				out.close()
				make_barchart(counts, total, options.b['annot_name'], destination + '.png', format='png')
		else:
			fh, destination = tempfile.mkstemp(suffix='.bed',dir=options.working_directory)
			run(" ".join(['wiggletools','write', destination, fun_merge, cmd_A, cmd_B]))
	else:
		fh, destination = tempfile.mkstemp(suffix='.bed',dir=options.working_directory)
		run(" ".join(['wiggletools','write', destination, cmd_A]))

	return destination

def make_normalised_form(fun_merge, fun_A, data_A, fun_B, data_B):
	cmd_A = " ".join([fun_A] + data_A)
	if data_B is not None:
		if fun_B is not None:
			cmd_B = " ".join([fun_B] + data_B)
		else:
			cmd_B = " ".join(data_B)
		res = "; ".join([fun_merge, cmd_A, cmd_B])
	else:
		res = cmd_A

	if verbose:
		print 'CMD A: ' + cmd_A
		print 'CMD B: ' + str(cmd_B)
		print 'CMD  : ' + res

	return res

def request_compute(conn, cursor, options, config):
	fun_A = options.wa 
	data_A = get_locations(cursor, options.a)
	options.countA = len(data_A)
	if len(data_A) ==  0:
		 return {'status':'INVALID'}
	cmd_A = " ".join([fun_A] + data_A)

	if options.b is not None:
		fun_B = options.wb
		data_B = get_locations(cursor, options.b)
		options.countB = len(data_B)
		if len(data_B) ==  0:
			 return {'status':'INVALID'}
	else:
		data_B = None
		fun_B = None
		cmd_B = None

	normalised_form = make_normalised_form(options.fun_merge, fun_A, data_A, fun_B, data_B)
	prior_result = get_precomputed_location(cursor, normalised_form)
	if prior_result is None:
		destination = launch_quick_compute(conn, cursor, options.fun_merge, fun_A, data_A, fun_B, data_B, options, config)
		if os.stat(destination).st_size == 0:
			return {'status':'EMPTY'}
		else:
			copy_to_longterm(destination, config)
			if destination[-4:] == ".txt":
				copy_to_longterm(destination + ".png", config)
		cursor.execute('INSERT INTO cache (query,last_query,location) VALUES ("%s",date("now"),"%s")' % (normalised_form, destination))
		return {'location': destination, 'status':'DONE'}
	else:
		reset_time_stamp(cursor, normalised_form)
		return {'location':prior_result, 'status':'DONE'}

###########################################
## Upload file
###########################################

def wget_dataset(url, dir):
	fh, destination = tempfile.mkstemp(suffix=url.split('.')[-1],dir=dir)
	run("wget %s -O %s" % (url, destination))
	return destination

def check_file_integrity(file):
	suffix = file.split('.')[-1]
	if suffix == 'bed' or suffix == 'txt':
		try:
			elems = []
			for line in open(file):
				items = line.split('\t')
				elems.append(items[0], int(items[1]), int(items[2]))
			fh = open(file, "w")
			for elem in sorted(elems):
				fh.write("\t".join(elems) + "\n")
			fh.close()
		except:
			return {'status':'MALFORMED_INPUT', 'format':'flatfile'}
	elif suffix == 'bb':
		try:
			run("bigBedInfo %s" % (file))
		except:
			return {'status':'MALFORMED_INPUT', 'format':'bigBed'}
		
	return None

def register_user_dataset(cursor, file, description): 
	cursor.execute('INSERT INTO user_datasets VALUES ("%s","%s")' % (file, description))

def upload_dataset(cursor, dir, url, description):
	try:
		file = wget_dataset(url, dir);
		if file is None:
			raise
		ret = check_file_integrity(file)
		if ret is not None:
			return ret
		register_user_dataset(cursor, file, description)
		return {'status':'UPLOADED'}
	except:
		raise
		return {'status':'UPLOAD_FAILED','url':url}

###########################################
## Main
###########################################

def main():
	options, config = get_options()
	conn = sqlite3.connect(options.db)
	cursor = conn.cursor()

	if options.load is not None:
		create_database(cursor, options.load)
	elif options.load_annots is not None:
		for line in open(options.load_annots):
			items = line.strip().split('\t')
			assert len(items) == 3
			cursor.execute('INSERT INTO annotation_datasets VALUES (?,?,?)',  items)
	elif options.clean is not None:
		clean_database(cursor, options.clean)
	elif options.cache:
		for entry in cursor.execute('SELECT * FROM cache').fetchall():
			print entry
	elif options.clear_cache is not None:
		if len(options.clear_cache) == 0:
			cursor.execute('DROP TABLE cache')
			create_cache(cursor)
		else:
			remove_jobs(cursor, options.clear_cache)
	elif options.attributes:
		print json.dumps(get_attribute_values(cursor))
	elif options.datasets:
		print "\n".join("\t".join(map(str, X)) for X in get_datasets(cursor))
	elif options.annotations:
		print "\n".join("\t".join(map(str, X)) for X in get_annotations(cursor))
	elif options.upload is not None:
		print json.dumps(upload_dataset(cursor, options.working_directory, options.upload, options.description))
	else:
		if options.a is not None:
			res = dict()
			for constraint in options.a:
				attribute, value = constraint.split("=")
				if attribute not in res:
					res[attribute] = []
				res[attribute].append(value)
			options.a = res
		if options.b is not None:
			res = dict()
			for constraint in options.b:
				attribute, value = constraint.split("=")
				if attribute not in res:
					res[attribute] = []
				res[attribute].append(value)
			options.b = res
		print json.dumps(request_compute(conn, cursor, options, config))

	conn.commit()
	conn.close()

if __name__=='__main__':
	main()
