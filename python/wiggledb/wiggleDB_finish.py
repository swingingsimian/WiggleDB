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
import subprocess
import os
import os.path
import json

import wiggleDB.wiggleDB
import wiggletools.multiJob 
import wiggletools.wigglePlots

class Struct(object):
        def __init__(self, **entries):
            self.__dict__.update(entries)

def get_options():
	assert len(sys.argv) == 2
	options = Struct(**(json.load(open(sys.argv[-1]))))
	return options, wiggleDB.wiggleDB.read_config_file(options.config)

def copy_to_longterm(data, config):
	if 's3_bucket' in config:
		os.environ['AWS_CONFIG_FILE'] = config['aws_config']
		cmd = "aws s3 cp %s s3://%s/%s --acl public-read" % (data, config['s3_bucket'], os.path.basename(data))
		if subprocess.call(cmd, shell=True) != 0:
			print "Failed to copy over results"
			print cmd
			sys.exit(100)

def main():
	try:
		options, config = get_options()
		empty = os.path.exists(options.data + ".empty")

		# Optional graphics
		if options.histogram is not None:
			if subprocess.call("wiggletools "  + options.histogram, shell=True):
				print "Failed to construct histogram"
				sys.exit(1)
			if os.path.getsize(options.data) > 0:
				wiggletools.wigglePlots.make_histogram(options.data, options.labels, options.data + ".png", format='png')
			else:
				empty = True

		if options.apply_paste is not None:
			if subprocess.call("wiggletools "  + options.apply_paste, shell=True):
				print "Failed to construct overlap graph"
				sys.exit(1)
			if os.path.getsize(options.data) > 0:
				wiggletools.wigglePlots.make_overlaps(options.data, options.data + ".png", format='png')
			else:
				empty = True

		# Signing off
		if empty:
			wiggleDB.wiggleDB.report_empty_to_user(options, config)
			wiggleDB.wiggleDB.mark_job_status(options.db, options.jobID, 'EMPTY')
		else:
			copy_to_longterm(options.data, config)
			if os.path.exists(options.data + ".png"):
				copy_to_longterm(data + ".png", config)
			wiggleDB.wiggleDB.report_to_user(options, config)
			wiggleDB.wiggleDB.mark_job_status(options.db, options.jobID, 'DONE')

		# Housekeeping
		if options.temps is not None:
			wiggletools.multiJob.clean_temp_files(options.temps)
		os.remove(sys.argv[-1])
	except:
		raise
		sys.exit(100)

if __name__ == "__main__":
	main()
