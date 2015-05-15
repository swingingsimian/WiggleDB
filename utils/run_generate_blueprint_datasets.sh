#!/usr/bin/env bash


# Name should be unique?
# 

# Need sample NAME/ID in here to make it uniq?
# 7/8 SAMPLE_ID/NAME
# 23/24 CELL_LINE/TYPE
# 14 is EXPERIMENT_TYPE (i.e. FeatureType)
# 43 is FILE


curl -sL ftp://ftp.ebi.ac.uk/pub/databases/blueprint/data_index/homo_sapiens/data.index |  \
  awk -f generate_blueprint_datasets.awk

#\
#  awk -F'\\\t' 'BEGIN {OFS="\t"} {
#    if(NR>1) 
#      print $43, $name, "regions", "FALSE", "GRCh38"
#  }' 