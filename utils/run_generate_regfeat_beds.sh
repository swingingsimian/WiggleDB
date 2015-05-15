#!/usr/bin/env bash

working_dir=$(grep 'working_directory' ../conf/wiggletools.conf | sed -r 's/working_directory\s+//')

echo -e "Downloading data to working directory:\t$working_dir"

wget -N -P "${working_dir}homo_sapiens/" ftp://ftp.ensembl.org/pub/current_regulation/homo_sapiens/RegulatoryFeatures_MultiCell.gff.gz
#This is currently default to no clobber, so is re-downloading with incremented suffixes.
#-N ensures this doesn't happen, and also only fetches if the timestamp on the source has changed

# GFFs will ultimately be dumped as beds in future but still need to split out like this


zcat "${working_dir}homo_sapiens/RegulatoryFeatures_MultiCell.gff.gz" | awk -f generate_regfeat_beds.awk -v work_dir="${working_dir}homo_sapiens/"

# Others:
# Ensembl Genes
# ftp://ftp.ensembl.org/pub/current_gtf/homo_sapiens/Homo_sapiens.GRCh38.79.gtf.gz
# CpG Islands?