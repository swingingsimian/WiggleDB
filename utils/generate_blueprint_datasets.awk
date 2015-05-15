#!/usr/bin/env awk


# First 5 fields need to be:
# location    name    type    annotation  assembly
# Followed by whatever appropriate filter variables

# Name should be unique?
# 

# Need SAMPLE_NAME in here to make it uniq? (as per EpiExplorer code)
# Ass SAMPLE_ID to filters
# 7/8 SAMPLE_ID/NAME
# 23/24 CELL_LINE/TYPE
# 14 is EXPERIMENT_TYPE (i.e. FeatureType)
# 43 is FILE

# Do we still need the headers in here? For integration into the interface?
# 


BEGIN {
  FS="\t"
  OFS="\t"
} 
{
  if(NR==1){  # Print heder line and set named indeces for readability
    EXPERIMENT_ID=1
    STUDY_ID=2
    #STUDY_NAME=3
    SAMPLE_ID=7
    SAMPLE_NAME=8
    LIBRARY_STRATEGY=13
    EXPERIMENT_TYPE=14
    DISEASE=20
    CELL_LINE=23
    CELL_TYPE=24
    TISSUE_TYPE=30
    TREATMENT=35
    FILE=43
    print "location", "name", "type", "annotation", "assembly", $EXPERIMENT_ID, \
     $STUDY_ID, $SAMPLE_ID, $SAMPLE_NAME, $LIBRARY_STRATEGY, \
     $EXPERIMENT_TYPE, $DISEASE, $CELL_TYPE, $CELL_LINE, $TISSUE_TYPE, $TREATMENT 
  }
  else if($FILE ~ /\.bed\.gz$/){   
    if($CELL_LINE == "-"){
      $CELL_LINE=""
      name=$CELL_TYPE
    }else{
      name=$CELL_LINE
      $CELL_TYPE=""
    }
    name=$EXPERIMENT_TYPE" "name"("$SAMPLE_NAME")"
    print $FILE, name, "regions", "FALSE", "GRCh38", $EXPERIMENT_ID, \
     $STUDY_ID, $SAMPLE_ID, $SAMPLE_NAME, $LIBRARY_STRATEGY, \
     $EXPERIMENT_TYPE, $DISEASE, $CELL_TYPE, $CELL_LINE, $TISSUE_TYPE, $TREATMENT 
  }
}  