#!/usr/bin/env awk

BEGIN {
  # Redefine field separators
  FS="\t"
  OFS="\t"

  # Set index names for readability
  SEQ_NAME=1
  SOURCE=2
  FTYPE=3
  # FTYPE here can be gene, exon, transcript, CDS, Selenocysteine, start_codon, stop_codon, UTR and apparently ''
  # We might want to split this out into exons, all genes and gene classes, dependant on transcript_biotype.

  SR_START=4
  SR_END=5

  ATTRS=9

  # Clean old datasets files
  tsv_file=work_dir"Ensembl_Genebuild.tsv"
  system("rm -f "tsv_file)

  # Deal with superset output
  outfiles["Genes"] = work_dir"Ensembl_Genes.bed"
  outfiles["Exons"] = work_dir"Ensembl_Exons.bed"
  system("rm -f "outfiles["Genes"])
  system("rm -f "outfiles["Exons"])
  # Don't print header as we will just add this to the bottom of dataset.tsv
  # when we cat the tsv files together
  #"location", "name", "type", "annotation", "assembly",
  print outfiles["Genes"], "Ensembl_Genes", "REGION", "TRUE", "GRCh38" >> tsv_file
  print outfiles["Exons"], "Ensembl_Exons", "REGION", "TRUE", "GRCh38" >> tsv_file
} 
{
  if($FTYPE == "gene" ){
    # or maybe gene biotype?
    # won't this have redundant entries for tr
    biotype=gensub(/.*gene_biotype "(.*)";.*/, "\\1", "g", $ATTRS) 


    if(outfiles[biotype] == ""){  # Specify ftype subset output
      outfiles[biotype] = work_dir"Ensembl_"biotype"_Genes.bed"
      system("rm -f "outfiles[biotype])
      print outfiles[biotype], "Ensembl_"biotype"_Genes", "REGION", "TRUE", "GRCh38" >> tsv_file
    }

    name=gensub(/.*gene_id "(ENS[A-Z]+[0-9]+)";.*/, "\\1", "g", $ATTRS) 
    # Set score as 1, otherwise some displays may not show these features
    print $SEQ_NAME, ($SR_START - 1), $SR_END, name, "1", "." >> outfiles[biotype]
    print $SEQ_NAME, ($SR_START - 1), $SR_END, name, "1", "." >> outfiles["Genes"]
  }
  else if($FTYPE == "exon"){
    name=gensub(/.*exon_id "(ENS[A-Z]+[0-9]+)";.*/, "\\1", "g", $ATTRS) 
    print $SEQ_NAME, ($SR_START - 1), $SR_END, name, "1", "." >> outfiles["Exons"]
  } 
}  