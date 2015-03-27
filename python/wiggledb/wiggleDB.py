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
	parser.add_argument('--user_datasets',dest='user_datasets',help='Print user dataset info', nargs='*')
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
	parser.add_argument('--userid',dest='userid',help='User name')
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
	create_chromosome_table(cursor)

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
			name varchar(1000) UNIQUE,
			location varchar(1000),
 			description varchar(100)
			)
		 '''
	cursor.execute(header)

def create_user_dataset_table(cursor):
	header = '''
			CREATE TABLE IF NOT EXISTS 
			user_datasets 
			(
 			name varchar(100),
			location varchar(1000),
			userid varchar(100)
			)
		 '''
	cursor.execute(header)

def create_chromosome_table(cursor):
	header = '''
			CREATE TABLE IF NOT EXISTS 
			chromosomes
			(
 			name varchar(1000) UNIQUE
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

def get_annotation_dataset_locations(cursor, names, userid = None):
	locations = []
	for name in names:
		res = cursor.execute('SELECT location FROM annotation_datasets WHERE name=?', (name,)).fetchall()
		if len(res) > 0:
			locations.append(res[0][0])
		elif userid is not None:
			res = cursor.execute('SELECT location FROM user_datasets WHERE name=? AND userid=?', (name,userid)).fetchall()
			if len(res) > 0:
				locations.append(res[0][0])
	return locations

def get_annotation_dataset_description(cursor, name):
	res = cursor.execute('SELECT description FROM annotation_datasets WHERE name=?', (name,)).fetchall()
	if len(res) > 0:
		return {'name':name, 'description':res[0][0]}
	else:
		return {'ERROR'}

def get_chromosomes(cursor):
	return [X[0] for X in cursor.execute('SELECT name FROM chromosomes').fetchall()]

def insert_chromosome(cursor, name):
	cursor.execute('INSERT INTO chromosomes (name) VALUES (?)', (name, ))

def run(cmd):
	p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	ret = p.wait()
	out, err = p.communicate()
	if ret != 0:
		sys.stdout.write("Failed in running %s\n" % (cmd))
		sys.stdout.write("OUTPUT:\n%s\n" % (out))
		sys.stdout.write("ERROR:\n%s\n" % (out))
		raise BaseException
	return out

def add_annotation_dataset(cursor, location, name, description):
	cursor.execute('INSERT INTO annotation_datasets (name,location,description) VALUES (?,?,?)', (name, location, description))
	chromosomes = run('wiggletools write_bg - %s | cut -f1 | uniq' % (location)).split('\n')
	old_chromosome = get_chromosomes(cursor)
	for chromosome in filter(lambda X: X not in old_chromosome and len(X) > 0, chromosomes):
		insert_chromosome(cursor, chromosome)

def get_user_dataset_locations(cursor, names, userid):
	locations = []
	for name in names:
		res = cursor.execute('SELECT location FROM user_datasets WHERE name=? AND userid=?', (name,userid)).fetchall()
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

def get_locations(cursor, params, userid):
	if 'annot_name' in params:
		return get_annotation_dataset_locations(cursor, params['annot_name'], userid)
	elif 'user_name' in params:
		return get_user_dataset_locations(cursor, params['user_name'])
	else:
		return get_dataset_locations(cursor, params)

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
	ind = numpy.arange(len(labels)) # the x locations for the groups
	width = 0.35 # the width of the bars: can also be len(x) sequence
	heights = [X/float(total) for X in counts]
	assert all(X <= 1 and X >= 0 for X in heights), (counts, total, heights)
	errors = [ math.sqrt(2*X*(1-X) / total) for X in heights ]
	pyplot.bar(ind, heights, width, yerr=errors)
	pyplot.xticks(ind+width/2., labels)
	pyplot.savefig(out, format=format)

def substitute_reference_locations(cursor, string, userid):
	items = string.split(" ")
	index = 0
	while index < len(items) and (items[index] == 'overlaps' or items[index] == 'noverlaps'):
		if items[index + 1] == 'extend':
			items[index + 3] = get_annotation_dataset_locations(cursor, [items[index + 3]], userid)[0]
			index += 4
		else:
			items[index + 1] = get_annotation_dataset_locations(cursor, [items[index + 1]], userid)[0]
			index += 2

	return " ".join(items)

def launch_quick_compute(conn, cursor, fun_merge, fun_A, data_A, fun_B, data_B, options, config):
	cmd_A = " ".join([substitute_reference_locations(cursor, fun_A, options.userid)] + data_A + [':'])

	if data_B is not None:
		merge_words = fun_merge.split(' ')

		assert fun_merge is not None
		if fun_B is not None:
			cmd_B = " ".join([substitute_reference_locations(cursor, fun_B, options.userid)] + data_B + [':'])
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
				assert counts[-1] <= total, 'wiggletools write_bg - overlaps %s %s | wc -l' % (annotation, cmd_A)
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
		os.remove(destination)
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
	data_A = get_locations(cursor, options.a, options.userid)
	options.countA = len(data_A)
	if len(data_A) == 0:
		 return {'status':'INVALID'}
	cmd_A = " ".join([fun_A] + data_A)

	if options.b is not None:
		fun_B = options.wb
		data_B = get_locations(cursor, options.b, options.userid)
		options.countB = len(data_B)
		if len(data_B) == 0:
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

def fetch_user_datasets(cursor, userid):
	return cursor.execute('SELECT * FROM user_datasets WHERE userid=?', (userid,)).fetchall()

def get_user_datasets(cursor, userid):
	return {'files': [X[0] for X in cursor.execute('SELECT name FROM user_datasets WHERE userid=?', (userid,)).fetchall()]}

def wget_dataset(url, dir):
	fh, destination = tempfile.mkstemp(suffix="." + url.split('.')[-1],dir=dir)
	run("cp %s %s" % (url, destination))
	return destination

def check_file_integrity(file, cursor):
	suffix = file.split('.')[-1]
	if suffix == 'bed' or suffix == 'txt':
		try:
			chromosomes = get_chromosomes(cursor)
			bad_assembly = True
			found_chromosomes = set()
			elems = []
			fh = open(file, "r")
			for line in fh:
				if len(line.strip()) == 0 or line[0] == '#' or line[0] == '-':
					continue
				items = line.strip().split('\t')
				if len(items) < 3:
					return {'status':'MALFORMED_INPUT', 'format':'BED', 'line': line}
				if items[0] in chromosomes:
					bad_assembly = False
				found_chromosomes.add(items[0])
				elems.append((items[0], int(items[1]), int(items[2])))
			fh.close()
			if bad_assembly:
				return {'status':'WRONG_ASSEMBLY', 'found_chromosomes':list(found_chromosomes), 'expected_chromosomes': chromosomes}
			fh = open(file, "w")
			for elem in sorted(elems):
				fh.write("\t".join(map(str, elem)) + "\n")
			fh.close()
			return
		except:
			return {'status':'MALFORMED_INPUT', 'format':'BEDX'}
	elif suffix == 'bb':
		try:
			run("bigBedInfo %s" % (file))
		except:
			return {'status':'MALFORMED_INPUT', 'format':'bigBed'}
		
	return {'status':'MALFORMED_INPUT','format':'unrecognized'}

def register_user_dataset(cursor, file, description, userid): 
	cursor.execute('INSERT INTO user_datasets (name,location,userid) VALUES ("%s","%s","%s")' % (description, file, userid))

def name_already_used(cursor, description, userid):
	return len(get_user_dataset_locations(cursor, [description], userid)) > 0 or len(get_annotation_dataset_locations(cursor, [description])) > 0

def upload_dataset(cursor, dir, url, description, userid):
	if name_already_used(cursor, description, userid):
		return {'status':'NAME_USED','name':description}

	try:
		file = wget_dataset(url, dir);
		if file is None:
			raise
		ret = check_file_integrity(file, cursor)
		if ret is not None:
			return ret
		register_user_dataset(cursor, file, description, userid)
		return {'status':'UPLOADED','name':description}
	except:
		raise
		return {'status':'UPLOAD_FAILED','url':url}

def save_dataset(cursor, dir, fileitem, description, userid):
	if name_already_used(cursor, description, userid):
		return {'status':'NAME_USED','name':description}

	try:
		fh, destination = tempfile.mkstemp(suffix=".bed",dir=dir)
		file = open(destination, "w")
		file.write(fileitem.file.read())
		file.close()
		ret = check_file_integrity(destination, cursor)
		if ret is not None:
			return ret
		register_user_dataset(cursor, destination, description, userid)
		return {'status':'UPLOADED','name':description}
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
			add_annotation_dataset(cursor, items[0], items[1], items[2])
	elif options.clean is not None:
		clean_database(cursor, options.clean)
	elif options.cache:
		for entry in cursor.execute('SELECT * FROM cache').fetchall():
			print entry
	elif options.clear_cache is not None:
		if len(options.clear_cache) == 0:
			cursor.execute('DROP TABLE cache')
			create_cache(cursor)
			cursor.execute('DROP TABLE user_datasets')
			create_user_dataset_table(cursor)
		else:
			remove_jobs(cursor, options.clear_cache)
	elif options.attributes:
		print json.dumps(get_attribute_values(cursor))
	elif options.datasets:
		print "\n".join("\t".join(map(str, X)) for X in get_datasets(cursor))
	elif options.annotations:
		print "\n".join("\t".join(map(str, X)) for X in get_annotations(cursor))
	elif options.upload is not None:
		print json.dumps(upload_dataset(cursor, options.working_directory, options.upload, options.description, options.userid))
	elif options.user_datasets is not None:
		if len(options.user_datasets) == 0:
			print "\n".join("\t".join(map(str, X)) for X in cursor.execute("SELECT * FROM user_datasets").fetchall())
		else:
			for user in options.user_datasets:
				print json.dumps(fetch_user_datasets(cursor, user))
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
