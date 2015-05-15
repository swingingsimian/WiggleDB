#!/usr/bin/env awk

BEGIN {
  FS="\t"
  OFS="\t"
  # Validate work_dir or just allow to download in current dir?
  source_root="ftp.ebi.ac.uk/pub/databases/"
} 
{
  if(NR==1){  # Print header line and set named indeces for readability
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

    # First 5 fields need to be:
    # location    name    type    annotation  assembly
    # Followed by whatever appropriate filter variables
    print "location", "name", "type", "annotation", "assembly", $EXPERIMENT_ID, \
     $STUDY_ID, $SAMPLE_ID, $SAMPLE_NAME, $LIBRARY_STRATEGY, \
     $EXPERIMENT_TYPE, $DISEASE, $CELL_TYPE, $CELL_LINE, $TISSUE_TYPE, $TREATMENT 
  }
  else if($FILE ~ /\.bb$/){  #/\.bed\.gz$/){
    # 611 bed.gz files
    # 581 .bb files
    local_file=work_dir""$FILE

    if(download != ""){
      # Test for failure here
      system("wget -x -nH --cut-dirs=2 -P "work_dir" "source_root""$FILE)
      #system("gunzip "local_file)
      #CHECKSUMS?!
    }

    # remove .gz
    #local_file=gensub(/\.gz$/, "", "g", local_file) 

    if($CELL_LINE == "-"){
      $CELL_LINE=""
      name=$CELL_TYPE
    }else{
      name=$CELL_LINE
      $CELL_TYPE=""
    }
    name=$EXPERIMENT_TYPE" "name"("$SAMPLE_NAME")"
    print local_file, name, "regions", "FALSE", "GRCh38", $EXPERIMENT_ID, \
     $STUDY_ID, $SAMPLE_ID, $SAMPLE_NAME, $LIBRARY_STRATEGY, \
     $EXPERIMENT_TYPE, $DISEASE, $CELL_TYPE, $CELL_LINE, $TISSUE_TYPE, $TREATMENT 
  }
}  