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

import wiggletools.parallelWiggleTools
import wiggletools.multiJob

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

	parser.add_argument('--load','-l',dest='load',help='Datasets to load in database')
	parser.add_argument('--load_assembly','-la',dest='load_assembly',help='Assembly name and path to file with chromosome lengths',nargs=2)
	parser.add_argument('--assembly','-y',dest='assembly',help='File with chromosome lengths')
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

	options = parser.parse_args()
	if all(X is None for X in [options.load, options.clean, options.result, options.load_assembly, options.datasets, options.clear_cache]) and not options.cache and not options.attributes and not options.annotations:
		assert options.a is not None, 'No dataset selection to run on'
		assert options.wa is not None, 'No dataset transformation to run on'
		assert options.assembly is not None, 'No assembly name specified'
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
	create_assembly_table(cursor)
	create_cache(cursor)
	create_job_table(cursor)
	create_dataset_table(cursor, filename)

def create_assembly_table(cursor):
	cursor.execute('''
	CREATE TABLE IF NOT EXISTS
	assemblies
	(
	name varchar(255),
	location varchar(1000)
	)
	''')

def create_job_table(cursor):
	cursor.execute('''
	CREATE TABLE IF NOT EXISTS
	jobs
	(
	job_id INTEGER PRIMARY KEY AUTOINCREMENT,
	lsf_id int,
	lsf_id2 int,
	temp varchar(1000),
	status varchar(255)
	)
	''')

def create_cache(cursor):
	cursor.execute('''
	CREATE TABLE IF NOT EXISTS
	cache
	(
	job_id int,
	query varchar(10000),
	location varchar(1000),
	remember bit,
	primary_loc bit,
	last_query datetime
	)
	''')

def create_dataset_table(cursor, filename):
	file = open(filename)
	items = file.readline().strip().split('\t')
	assert items[:5] == list(('location','name','type','annotation','assembly')), "Badly formed dataset table, please ensure the first five columns refer to location, name, type, annotation and assembly"
	header = '''
			CREATE TABLE IF NOT EXISTS 
			datasets 
			(
			location varchar(1000),
 			name varchar(100), 
			type varchar(100), 
			annotation bit, 
			assembly varchar(100),
		 '''
	cursor.execute('\n'.join([header] + [",\n".join(['%s varchar(255)' % X for X in items[5:]])] + [')']))

	cursor.execute('SELECT * FROM datasets').fetchall()
	column_names = [X[0] for X in cursor.description]
	assert column_names == items, 'Mismatch between the expected columns: \n%s\nAnd the columns in file:\n%s' % ("\t".join(column_names), '\t'.join(items))

	for line in file:
		cursor.execute('INSERT INTO datasets VALUES (%s)' % ",".join("'%s'" % X for X in line.strip().split('\t')))
	file.close()

###########################################
## Loading assembly info
###########################################

def load_assembly(cursor, assembly_name, chrom_sizes):
	if verbose:
		print 'Loading path to assembly chromosome length %s for %s' % (chrom_sizes, assembly_name)
	cursor.execute('INSERT INTO assemblies VALUES(\'%s\',\'%s\')' % (assembly_name, chrom_sizes))

###########################################
## Garbage cleaning 
###########################################

def remove_job(cursor, job):
	cursor.execute('DELETE FROM cache WHERE job_id = ?', (job,))
	cursor.execute('DELETE FROM jobs WHERE job_id = ?', (job,))

def remove_jobs(cursor, jobs):
	for job in jobs:
		remove_job(cursor, job)

def clean_database(cursor, days):
	for location in cursor.execute('SELECT location FROM cache WHERE julianday(\'now\') - julianday(last_query) > %i AND remember = 0' % days).fetchall():
		if verbose:
			print 'Removing %s' % location[0]
		if os.path.exists(location[0]):
			os.remove(location[0])
	cursor.execute('DELETE FROM cache WHERE julianday(\'now\') - julianday(last_query) > %i AND remember = 0' % days)

	for temp in cursor.execute('SELECT temp FROM jobs WHERE status="DONE" OR status="EMPTY"').fetchall():
		if verbose:
			print 'Removing %s and derived files' % temp[0]
		multiJob.clean_temp_file(temp[0])

	cursor.execute('DELETE FROM cache WHERE job_id IN (SELECT job_id FROM jobs WHERE status = "ERROR")' % days)
	cursor.execute('DELETE FROM jobs WHERE status = "ERROR"' % days)

###########################################
## Search datasets
###########################################

def get_dataset_attributes_2(cursor):
	return [X[1] for X in cursor.execute('PRAGMA table_info(datasets)').fetchall()]

def get_dataset_attributes(cursor):
	return list(set(get_dataset_attributes_2(cursor)) - set(["annotation","name","assembly","location","type"]))

def get_attribute_values_2(cursor, attribute):
	return [X[0] for X in cursor.execute('SELECT DISTINCT %s FROM datasets' % (attribute)).fetchall()]

def get_attribute_values(cursor):
	return dict((attribute, get_attribute_values_2(cursor, attribute)) for attribute in get_dataset_attributes(cursor))

def get_annotations(cursor, assembly):
	return cursor.execute('SELECT * FROM datasets WHERE assembly=? AND annotation', (assembly,)).fetchall()

def get_job(cursor, job):
	res = cursor.execute('SELECT * FROM jobs WHERE job_id = ?', (job,)).fetchall()
	if len(res) == 0:
		return [job, None, None, None, None, None]
	else:
		return res[0]

def get_jobs(cursor, joblist):
	if len(joblist) == 0:
		res = cursor.execute('SELECT * FROM jobs').fetchall()
	else:
		res = [get_job(cursor, X) for X in joblist]
	return [[X[0] for X in cursor.description]] + res

def get_datasets(cursor):
	res = cursor.execute('SELECT * FROM datasets').fetchall()
	return [[X[0] for X in cursor.description]] + res

def attribute_selector(attribute, params):
	return "( %s )" % " OR ".join("%s=:%s_%i" % (attribute,attribute,index) for index in range(len(params[attribute])))

def denormalize_params(params):
	return dict(("%s_%i" % (attribute, index),value) for attribute in params for (index, value) in enumerate(params[attribute]))

def get_dataset_locations(cursor, params, assembly):
	# Quick check that all the keys are purely alphanumeric to avoid MySQL injections
	assert not any(re.match('\W', X) is not None for X in params)
	params['assembly'] = [assembly]
	query = " AND ".join(attribute_selector(X, params) for X in params)
	if verbose:
		print 'Query: SELECT location FROM datasets WHERE ' + query
		print 'Where:' + str(denormalize_params(params))
	res = cursor.execute('SELECT location FROM datasets WHERE ' + query, denormalize_params(params)).fetchall()
	if verbose:
		print 'Found:\n' + "\n".join(X[0] for X in res)
	return sorted(X[0] for X in res)

###########################################
## Search cache
###########################################

def reset_time_stamp(cursor, cmd):
	cursor.execute('UPDATE cache SET last_query= date(\'now\') WHERE query = \'%s\'' % cmd)

def get_precomputed_jobID(cursor, cmd):
	reset_time_stamp(cursor, cmd)
	reports = cursor.execute('SELECT job_id FROM cache WHERE query = \'%s\'' % cmd).fetchall()
	if len(reports) == 0:
		if verbose:
			print 'Did not find prior job for query: %s' % cmd
		return None
	else:
		if verbose:
			print 'Found prior job for query: %s' % cmd
			print reports[0][0]
		return reports[0][0]

def get_job_location_2(cursor, jobID):
	reports = cursor.execute('SELECT location FROM cache WHERE job_id = \'%s\' and primary_loc=1' % jobID).fetchall()
	assert len(reports) == 1
	return reports[0][0]

def get_job_location(db, jobID):
	connection = sqlite3.connect(db)
	cursor = connection.cursor()
	res = get_job_location_2(cursor, jobID)
	connection.close()
	return res

def get_precomputed_location(cursor, cmd):
	reports = cursor.execute('SELECT location FROM jobs NATURAL JOIN cache WHERE (status="DONE" OR status="EMPTY") AND query = ?', (cmd,)).fetchall()
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

def launch_compute(conn, cursor, fun_merge, fun_A, data_A, fun_B, data_B, options, normalised_form, batch_system):
	destination = None
	destinationA = None
	destinationB = None
	cmds = None
	options.histogram = None
	options.apply_paste = None

	cmd_A = " ".join([fun_A] + data_A + [':'])
	cmd_A2, destinationA, computeA = reuse_or_write_precomputed_location(cursor, cmd_A, options.working_directory)

	if data_B is not None:
		merge_words = fun_merge.split(' ')

		assert fun_merge is not None
		if fun_B is not None:
			cmd_B = " ".join([fun_B] + data_B + [':'])
			cmd_B2, destinationB, computeB = reuse_or_write_precomputed_location(cursor, cmd_B, options.working_directory)
		else:
			cmd_B2 = " ".join(data_B)
			computeB = False

		if merge_words[0] == 'histogram':
			cmds = []
			if computeA:
				cmds = [cmd_A2]
			if computeB:
				cmds.append(cmd_B2)
			width = merge_words[1]

			fh, destination = tempfile.mkstemp(suffix='.txt',dir=options.working_directory)
			if fun_B is not None:
		        	options.histogram = "histogram %s %s %s mult %s %s" % (destination, width, destinationA, destinationA, destinationB)
			elif data_B is not None:
				options.histogram = "histogram %s %s %s %s" % (destination, width, destinationA, " ".join("mult %s %s" % (destinationA, X) for X in data_B))
		elif merge_words[0] == 'profile':
			fh, destination = tempfile.mkstemp(suffix='.txt',dir=options.working_directory)
			cmds = [" ".join(['profile', destination, merge_words[1], cmd_B2, cmd_A2])]
		elif merge_words[0] == 'profiles':
			fh, destination = tempfile.mkstemp(suffix='.txt',dir=options.working_directory)
			cmds = [" ".join(['profiles', destination, merge_words[1], cmd_B2, cmd_A2])]
		elif merge_words[0] == 'apply_paste':
			cmds = []
			if computeA:
				cmds = [cmd_A2]
			if computeB:
				cmds.append(cmd_B2)
			fh, destination = tempfile.mkstemp(suffix='.txt',dir=options.working_directory)
			assert len(data_B) == 1, "Cannot apply_paste to multiple files %s\n" % " ".join(data_B)
			options.apply_paste = " ".join(['apply_paste', destination, 'AUC', data_B[0], destinationA])
		else:
			fh, destination = tempfile.mkstemp(suffix='.bw',dir=options.working_directory)
			cmds = [" ".join(['write', destination, fun_merge, cmd_A2, cmd_B2])]
	else:
		computeB = False
		cmds = [cmd_A2]
		destination = destinationA
		destinationA = None

	chrom_sizes = get_chrom_sizes(cursor, options.assembly)
	if len(cmds) > 0:
		lsfID, options.temps = parallelWiggleTools.run(cmds, chrom_sizes, batch_system=batch_system, tmp=options.working_directory)
		cursor.execute('INSERT INTO jobs (lsf_id, status) VALUES (?, "LAUNCHED")', (lsfID,))
		jobID = cursor.execute('SELECT LAST_INSERT_ROWID()').fetchall()[0][0]
		cursor.execute('INSERT INTO cache (job_id,primary_loc,query,remember,last_query,location) VALUES (\'%s\',1,\'%s\',\'%i\',date(\'now\'),\'%s\')' % (jobID, normalised_form, int(options.remember), destination))
		if computeA: 
			cursor.execute('INSERT INTO cache (job_id,primary_loc,query,remember,last_query,location) VALUES (\'%s\',0,\'%s\',\'0\',date(\'now\'),\'%s\')' % (jobID, cmd_A, destinationA))
		if computeB:
			cursor.execute('INSERT INTO cache (job_id,primary_loc,query,remember,last_query,location) VALUES (\'%s\',0,\'%s\',\'0\',date(\'now\'),\'%s\')' % (jobID, cmd_B, destinationB))
		conn.commit()
	else:
		lsfID = None
		cursor.execute('INSERT INTO jobs (lsf_id, status) VALUES (NULL, "LAUNCHED")')
		jobID = cursor.execute('SELECT LAST_INSERT_ROWID()').fetchall()[0][0]
		if options.histogram is not None:
			cmd = options.histogram
		elif options.apply_paste is not None:
			cmd = options.apply_paste
		assert cmd is not None and destination is not None
		cursor.execute('INSERT INTO cache (job_id,primary_loc,query,remember,last_query,location) VALUES (\'%s\',1,\'%s\',\'%i\',date(\'now\'),\'%s\')' % (jobID, cmd, int(options.remember), destination))
		options.temps = None

	options.jobID = jobID
	options.data = destination
	if options.histogram is not None:
		if fun_B is not None:
			options.labels = ['Overall', 'Regions']
		elif data_B is not None:
			options.labels = options.b['name']
		else:
			options.labels = ['Overall']
	else:
		options.labels = None

	fh, options_file = tempfile.mkstemp(dir=options.working_directory)
	# To ensure object can be serialised and to avoid side effects
	f = open(options_file, 'w')
	json.dump(options.__dict__, f)
	f.close()
	finishCmd = 'wiggleDB_finish.py ' + options_file
	lsfID2, temp = multiJob.submit([finishCmd], batch_system=batch_system, dependency=lsfID, working_directory=options.working_directory)
	cursor.execute('UPDATE jobs SET lsf_id2=\'%s\',temp=\'%s\' WHERE job_id=\'%s\'' % (lsfID2, temp, jobID))
	return jobID

def get_chrom_sizes(cursor, assembly):
	res = cursor.execute('SELECT location FROM assemblies WHERE name = \'%s\'' % (assembly)).fetchall()
	return res[0][0]

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

def request_compute(conn, cursor, options, config, batch_system):
	fun_A = options.wa 
	data_A = get_dataset_locations(cursor, options.a, options.assembly)
	options.countA = len(data_A)
	if len(data_A) ==  0:
		 return {'status':'INVALID'}
	cmd_A = " ".join([fun_A] + data_A)

	if options.b is not None:
		fun_B = options.wb
		data_B = get_dataset_locations(cursor, options.b, options.assembly)
		options.countB = len(data_B)
		if len(data_B) ==  0:
			 return {'status':'INVALID'}
	else:
		data_B = None
		fun_B = None
		cmd_B = None

	normalised_form = make_normalised_form(options.fun_merge, fun_A, data_A, fun_B, data_B)
	prior_jobID = get_precomputed_jobID(cursor, normalised_form)
	if prior_jobID is not None:
		res = query_result(cursor, prior_jobID, batch_system)
		options.jobID = res['ID']
		if res['status'] == 'DONE':
			options.data = res['location']
			report_to_user(options, config)
		else:
			acknowledge_job_to_user(options, config)
	else:
		res = {'ID':launch_compute(conn, cursor, options.fun_merge, fun_A, data_A, fun_B, data_B, options, normalised_form, batch_system), 'status':'LAUNCHED'}
		options.jobID = res['ID']
		acknowledge_job_to_user(options, config)

	return res


####################################################
## Querying jobs
####################################################

def sge_job_running(lsfID):
	return subprocess.Popen(['qstat','-j',str(lsfID)], stdout=subprocess.PIPE, stderr=subprocess.PIPE).wait() == 0

def sge_job_return_values(lsfID):
	p = subprocess.Popen(['qacct','-j',str(lsfID)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	p.wait()
	(stdout, stderr) = p.communicate()
	assert p.returncode == 0, 'Error when polling SGE job %i' % lsfID
	values = []
	failedTask = False
	for line in stdout.split('\n'):
		items = re.split('\W*', line)
		if items[0] == 'failed' and items[1] != '0':
			values.append(" ".join(items[1:]))
			failedTask = True
		elif items[0] == 'exit_status': 
			if failedTask:
				failedTaskID = False
			else:
				values.append(items[1])
	return values

def mark_job_status2(cursor, jobID, status):
	cursor.execute('UPDATE jobs SET status = \'%s\' WHERE job_id = \'%s\'' % (status, jobID))

def mark_job_status(db, jobID, status):
	conn = sqlite3.connect(db)
	cursor = conn.cursor()
	mark_job_status2(cursor, jobID, status)
	conn.commit()
	conn.close()

def query_result(cursor, jobID, batch_system):
	reports = cursor.execute('SELECT status, lsf_id, lsf_id2 FROM jobs WHERE job_id =?', (jobID,)).fetchall()

	if len(reports) == 0:
		return {'ID':jobID, 'status':'UNKNOWN'}
	else:
		assert len(reports) == 1, 'Found %i status reports for job %s' % (len(reports), jobID)

	status, lsfID, lsfID2 = reports[0]
	if status == 'DONE':
		return {'ID':jobID, 'status':'DONE', 'location':get_job_location_2(cursor, jobID)}
	elif status == 'EMPTY':
		return {'ID':jobID, 'status':'EMPTY'}
	elif status == 'ERROR' or lsfID is None or lsfID2 is None:
		return {'ID':jobID, 'status':'ERROR'}
	elif batch_system == 'LSF':
		p = subprocess.Popen(['bjobs','-noheader',str(lsfID2)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		ret = p.wait()
		(stdout, stderr) = p.communicate()
		assert ret == 0, 'Error when polling LSF job %i' % lsfID2
		values = []
		for line in stdout.split('\n'):
			items = re.split('\W*', line)
			if len(items) > 2:
				values.append(items[2])
		return {'ID':jobID, 'status':"WAITING", 'return_values':values}
	elif batch_system == 'SGE':
		if sge_job_running(lsfID2):
			if sge_job_running(lsfID):
				return {'ID':jobID, 'status':"WAITING", 'LSF_ID':lsfID}
			else:
				values = sge_job_return_values(lsfID)
				if any(X != '0' for X in values):
					mark_job_status2(cursor, jobID, 'ERROR')
					return {'ID':jobID, 'status':"ERROR", 'return_values':values, 'LSF_ID':lsfID}
				else:
					return {'ID':jobID, 'status':"WAITING", 'LSF_ID':lsfID}
		else:
			values = sge_job_return_values(lsfID2)
			if any(X != '0' for X in values):
				mark_job_status2(cursor, jobID, 'ERROR')
				return {'ID':jobID, 'status':"ERROR", 'return_values':values, 'LSF_ID':lsfID2}
			else:
				return {'ID':jobID, 'status':"WAITING", 'LSF_ID':lsfID}
			  
	else:
		raise NameError
		return {'ID':jobID, 'status':'CONFIG_ERROR'}

###########################################
## When a job finishes:
###########################################

def send_SMTP(msg, emails, config):
	import smtplib
	s = smtplib.SMTP(config['smtp_server'], config['smtp_port'])
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login(config['user'], config['password'])
	s.sendmail(config['reply_to'], emails, msg.as_string())
	s.quit()

def send_email(text, title, emails, config):
	from email.mime.text import MIMEText
	msg = MIMEText(text, 'html')
	msg['Subject'] = '[WiggleTools] ' + title
	msg['From'] = config['reply_to']
	msg['To'] = ", ".join(emails)
        msg['sendername'] = config['sendername']
	send_SMTP(msg, emails, config)

def visible_url(location, config):
	if 's3_bucket' in config:
		base_url = 'http://s3-%s.amazonaws.com/%s/' % (config['s3_region'], config['s3_bucket'])
		return re.sub(config['working_directory'], base_url, location)		
	else:
		return location

def job_description(options):
	text = "<table>"
	if options.b is None:
		text += "<tr>"
		text += "<td>"
		text += "Selector"
		text += "</td>"
		text += "<td>"
		text += " AND ".join("(" + " OR ".join("%s=%s" % (X, Y) for Y in options.a[X]) + ")" for X in options.a)
		text += "</td>"
		text += "</tr>"
		text += "<tr>"
		text += "<td>"
		text += "Count"
		text += "</td>"
		text += "<td>"
		text += str(options.countA)
		text += "</td>"
		text += "</tr>"
		text += "<tr>"
		text += "<td>"
		text += "Operation"
		text += "</td>"
		text += "<td>"
		text += options.wa 
		text += "</td>"
		text += "</tr>"
	else:
		text += "<tr>"
		text += "<td>"
		text += "Lefthand selector"
		text += "</td>"
		text += "<td>"
		text += " AND ".join("(" + " OR ".join("%s=%s" % (X, Y) for Y in options.a[X]) + ")" for X in options.a)
		text += "</td>"
		text += "</tr>"
		text += "<tr>"
		text += "<td>"
		text += "Lefthand count"
		text += "</td>"
		text += "<td>"
		text += str(options.countA)
		text += "</td>"
		text += "</tr>"
		text += "<tr>"
		text += "<td>"
		text += "Lefthand operation"
		text += "</td>"
		text += "<td>"
		text += options.wa 
		text += "</td>"
		text += "</tr>"
		
		text += "<tr>"
		text += "<td>"
		text += "Righthand selector"
		text += "</td>"
		text += "<td>"
		text += " AND ".join("(" + " OR ".join("%s=%s" % (X, Y) for Y in options.b[X]) + ")" for X in options.b)
		text += "</td>"
		text += "</tr>"
		text += "<tr>"
		text += "<td>"
		text += "Righthand count"
		text += "</td>"
		text += "<td>"
		text += str(options.countB)
		text += "</td>"
		text += "</tr>"
		text += "<tr>"
		text += "<td>"
		text += "Righthand operation"
		text += "</td>"
		text += "<td>"
		text += options.wb
		text += "</td>"
		text += "</tr>"

		text += "<tr>"
		text += "<td>"
		text += "Final operation"
		text += "</td>"
		text += "<td>"
		text += options.fun_merge
		text += "</td>"
		text += "</tr>"

	text += "</table>"
	return text	

def report_to_user(options, config):
	if options.emails is None:
		return
	else:
		url = visible_url(options.data, config)
		text = "<html>"
		text += "<head>"
		text += "</head>"
		text += "<body>"
		text += "<p>"
		text += "Hello"
		text += "</p>"
		text += "<p>"
		text += "Your job %i is now finished, please refer to the WiggleTools server for your results." % options.jobID
		text += "</p>"
		text += "<p>"
		text += "You can download all the results <a href=%s>here</a>" % url
		if url[-3:] == '.bw' or url[-3:] == ".bb":
			ensembl_link = 'http://%s/%s/Location/View?g=%s;contigviewbottom=url:%s' % (config['ensembl_server'], config['ensembl_species'], config['ensembl_gene'], url)
			text += ", or you can view them directly on <a href=%s>Ensembl</a>" % ensembl_link
		else:
			text += ".</p><p>"
			text += "<center>"
			text += "<a href='%s.png'>" % url
			text += "<img src='%s.png'>" % url
			text += "</a>"
			text += "</center>"
		text += "</p>"
		text += job_description(options)
		text += "<p>"
		text += "Best regards,"
		text += "</p>"
		text += "<p>"
		text += "The WiggleTools team"
		text += "<p>"
		text += "</body>"
		text += "<html>"
		send_email(text, 'Job %i succeeded' % options.jobID, options.emails, config)

def acknowledge_job_to_user(options, config):
	if options.emails is None:
		return
	else:
		text = "<html>"
		text += "<head>"
		text += "</head>"
		text += "<body>"
		text += "<p>"
		text += "Hello"
		text += "</p>"
		text += "<p>"
		text += "Your job %i has been despatched with options:" % options.jobID
		text += "</p>"
		text += job_description(options)
		text += "<p>"
		text += "Best regards,"
		text += "</p>"
		text += "<p>"
		text += "</p>"
		text += "The WiggleTools team"
		text += "<p>"
		text += "</body>"
		text += "</html>"
		send_email(text, 'Job %i dispatched' % options.jobID, options.emails, config)

def report_empty_to_user(options, config):
	if options.emails is None:
		return
	else:
		text = "<html>"
		text += "<head>"
		text += "</head>"
		text += "<body>"
		text += "<p>"
		text += "Hello"
		text += "</p>"
		text += "<p>"
		text += "Your job %i is now finished, but returned empty results.\n\n" % options.jobID
		text += "</p>"
		text += job_description(options)
		text += "<p>"
		text += "Best regards,"
		text += "</p>"
		text += "<p>"
		text += "</p>"
		text += "The WiggleTools team"
		text += "<p>"
		text += "</body>"
		text += "</html>"
		send_email(text, 'Job %i returned an empty result' % options.jobID, options.emails, config)

###########################################
## Main
###########################################

def main():
	options, config = get_options()
	conn = sqlite3.connect(options.db)
	cursor = conn.cursor()

	if config is None or 'batch_system' not in config:
		batch_system = 'SGE'
	else:
		batch_system = config['batch_system']

	if options.load is not None:
		create_database(cursor, options.load)
	elif options.load_assembly is not None:
		load_assembly(cursor, options.load_assembly[0], options.load_assembly[1])
	elif options.clean is not None:
		clean_database(cursor, options.clean)
	elif options.result is not None:
		print json.dumps(query_result(cursor, options.result, batch_system))
	elif options.cache:
		for entry in cursor.execute('SELECT * FROM cache').fetchall():
			print entry
	elif options.clear_cache is not None:
		if len(options.clear_cache) == 0:
			cursor.execute('DROP TABLE cache')
			create_cache(cursor)
			cursor.execute('DROP TABLE jobs')
			create_job_table(cursor)
		else:
			remove_jobs(cursor, options.clear_cache)
	elif options.attributes:
		print json.dumps(get_attribute_values(cursor))
	elif options.jobs is not None:
		print "\n".join("\t".join(map(str, X)) for X in get_jobs(cursor, options.jobs))
	elif options.datasets:
		print "\n".join("\t".join(map(str, X)) for X in get_datasets(cursor))
	elif options.annotations:
		print "\n".join("\t".join(map(str, X)) for X in get_annotations(cursor, options.assembly))
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
		print json.dumps(request_compute(conn, cursor, options, config, batch_system))

	conn.commit()
	conn.close()

if __name__=='__main__':
	main()
