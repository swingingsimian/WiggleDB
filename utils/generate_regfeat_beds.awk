#!/usr/bin/env awk

BEGIN {
  # Redefine field separators
  FS="\t"
  OFS="\t"

  # Set index names for readability
  SEQ_NAME=1
  FTYPE=3
  SR_START=4
  SR_END=5
  ATTRS=9

  # Clean old datasets files
  tsv_file=work_dir"Ensembl_Regbuild.tsv"
  system("rm -f "tsv_file)

  # Deal with superset output
  outfiles["RegFeats"] = work_dir"Ensembl_RegFeats.bed"
  system("rm -f "outfiles["RegFeats"])
  # Don't print header as we will just add this to the bottom of dataset.tsv
  # when we cat the tsv files together
  #"location", "name", "type", "annotation", "assembly",
  print outfiles["RegFeats"], "Ensembl_RegFeats", "REGION", "TRUE", "GRCh38" >> tsv_file
} 
{
  if(outfiles[$FTYPE] == ""){  # Specify ftype subset output
    outfiles[$FTYPE] = work_dir"Ensembl_"$FTYPE".bed"
    system("rm -f "outfiles[$FTYPE])
    print outfiles[$FTYPE], "Ensembl_"$FTYPE, "REGION", "TRUE", "GRCh38" >> tsv_file
  }

  name=gensub(/Name.*ID=(ENS[A-Z]+[0-9]+);.*/, "\\1", "g", $ATTRS) 
  # Set score as 1, otherwise some displays may not show these features
  print $SEQ_NAME, ($SR_START - 1), $SR_END, name, "1", "." >> outfiles[$FTYPE]
  print $SEQ_NAME, ($SR_START - 1), $SR_END, name, "1", "." >> outfiles["RegFeats"]

  # These will not be the true ends as they currently do not integrate the flanks/bounds
  # Need to fix the data model and the dumps
}  