#!/usr/bin/env bash



# Test for over-writing output file
# Download option, otherwise dry run to just create tsv file


# This working_directory is a bit flat at the moment
# We likely want to subdivide this into datasets, annotations, (user_datasets, user_annotations ?)

# Take a workding_dir override arg?

working_dir=$(grep 'working_directory' ../conf/wiggletools.conf | sed -r 's/working_directory\s+//')

echo -e "Downloading data to working directory:\t$working_dir"
[[ ! -d "${working_dir}homo_sapiens/" ]] && mkdir -p "${working_dir}homo_sapiens/" 

curl -sL ftp://ftp.ebi.ac.uk/pub/databases/blueprint/data_index/homo_sapiens/data.index |  \
  awk -v work_dir="${working_dir}homo_sapiens/" -v download="$1" -f generate_blueprint_datasets.awk \
  > "${working_dir}homo_sapiens/blueprint_datasets.tsv"


