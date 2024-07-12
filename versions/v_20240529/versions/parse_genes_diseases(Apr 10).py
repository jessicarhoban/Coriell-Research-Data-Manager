import pandas as pd
import os
import glob
import re
import xmltodict
import json
import uuid
import requests


disgen_df = pd.read_csv("./data/test-app/BIOSNAP Datasets/DG-AssocMiner_miner-disease-gene.tsv", sep="\t")
discat_df = pd.read_csv("./data/test-app/BIOSNAP Datasets/D-DoPathways_diseaseclasses (1).csv")

#----------------------------------------------------------------------------------------------------------------------------------------
# Initialize list to hold DataFrame rows
row_list = []

# Read in JJ's annotations
jj_df = pd.read_csv("./data/test-app/research_data_annotation_2023-02-27.tsv", sep="\t")

# Grab all folder names
folders = [f for f in glob.glob("../../mnt/data/research_data/*") if os.path.isdir(f) == True]

# Iterate over folder names and capture index
for idx, folder in enumerate(folders):
  
  # Grab only folder name
  fname = folder.split("/")[-1]
  
  # Find where JJ annotation matches
  jj_type = jj_df.loc[jj_df["sequencing run"] == fname]["type"]
  jj_desc = jj_df.loc[jj_df["sequencing run"] == fname]["description"]
  
  jj_type = "" if len(jj_type) == 0 else jj_type.values[0]
  jj_desc = "" if len(jj_desc) == 0 else jj_desc.values[0]
  
  #____Initialize row Structure____
  row = {"date": "",
                "folderpath":folder,
                "foldername":fname,
                "reduced_foldername": fname.lower(),
                "reduced_jjdescription": jj_desc.lower(),
                "jj_description": jj_desc,
                "jj_type": jj_type,
                "cbix1/2": "CBIX"}
  
  #____Date parsing____
  date = re.findall("(\d\d\d\d-\d\d-\d\d)", folder)
  
  if len(date) == 0:
    row["date"] = "Null"
  else:
    row["date"] =  date[0]
    row["reduced_foldername"] = row["reduced_foldername"].replace(date[0], "")

  #____Add to row_list____
  row_list.append(row)

#_________________________
# Convert to DataFrame
folder_df = pd.DataFrame(row_list)
folder_df["folder_uuid"] = folder_df.apply(lambda _: str(uuid.uuid4()), axis=1)

folder_searchterms = {folder.split("/")[-1]:set() for folder in folders}

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Gene parsing_________________________
gene_map = [{"search_term": "arsa",   "gene_id_hgnc":"713", "gene_id_ncbi": "410",   "name": "Arylsulfatase A (ARSA)", "organism":"Human","taxonomy_id_ncbi":"9606"},
            {"search_term": "spry3",  "gene_id_hgnc":"11271","gene_id_ncbi": "10251", "name": "Sprouty RTK Signaling Antagonist 3 (SPRY3)", "organism":"Human","taxonomy_id_ncbi":"9606"},
            {"search_term": "spry4",  "gene_id_hgnc":"15533","gene_id_ncbi": "81848", "name": "Sprouty RTK Signaling Antagonist 4 (SPRY4)", "organism":"Human","taxonomy_id_ncbi":"9606"},
            {"search_term": "dbet",   "gene_id_hgnc":"43904","gene_id_ncbi": "100419743", "name": "D4Z4 Binding Element Transcript (DBET)", "organism":"Human","taxonomy_id_ncbi":"9606"},
            {"search_term": "asxl1",  "gene_id_hgnc":"18318","gene_id_ncbi": "171023", "name": "ASXL Transcriptional Regulator 1 (ASXL1)", "organism":"Human","taxonomy_id_ncbi":"9606"},
            {"search_term": "znf251", "gene_id_hgnc":"13045","gene_id_ncbi": "90987", "name": "Zinc Finger Protein 251 (ZNF251)", "organism":"Human","taxonomy_id_ncbi":"9606"},
            {"search_term": "tet2",   "gene_id_hgnc":"25941","gene_id_ncbi": "54790", "name": "Tet Methylcytosine Dioxygenase 2 (TET2)", "organism":"Human","taxonomy_id_ncbi":"9606"}, 
            {"search_term": "dnmt3a", "gene_id_hgnc":"2978","gene_id_ncbi": "1788", "name": "DNA Methyltransferase 3 Alpha (DNMT3A)", "organism":"Human","taxonomy_id_ncbi":"9606"},
            {"search_term": "dlat",   "gene_id_hgnc":"2896","gene_id_ncbi": "1737", "name": "Dihydrolipoamide S-acetyltransferase (DLAT)", "organism":"Human","taxonomy_id_ncbi":"9606"},
            {"search_term": "elov13", "gene_id_hgnc":"18047","gene_id_ncbi": "83401", "name": "ELOVL Fatty Acid Elongase 3 (ELOVL3)", "organism":"Human","taxonomy_id_ncbi":"9606"},
            {"search_term": "p53",    "gene_id_hgnc":"11998","gene_id_ncbi": "7157", "name": "Tumor Protein (P53)", "organism":"Human","taxonomy_id_ncbi":"9606"},
            {"search_term": "yod1",   "gene_id_hgnc":"25035","gene_id_ncbi": "55432", "name": "YOD1 Deubiquitinase (YOD1)", "organism":"Human","taxonomy_id_ncbi":"9606"},
            {"search_term": "pdha1",  "gene_id_hgnc":"8806","gene_id_ncbi": "5160", "name": "Pyruvate Dehydrogenase E1 Subunit Alpha 1 (PDHA1)", "organism":"Human","taxonomy_id_ncbi":"9606"},
            {"search_term": "pdhb",   "gene_id_hgnc":"8808","gene_id_ncbi": "5162", "name": "Pyruvate Dehydrogenase E1 Subunit Beta (PDHB)", "organism":"Human","taxonomy_id_ncbi":"9606"},
            {"search_term": "cdk5r1", "gene_id_hgnc":"1775","gene_id_ncbi": "8851", "name": "Cyclin-Dependent Kinase 5 Regulatory Subunit 1 (CDK5R1)", "organism":"Human","taxonomy_id_ncbi":"9606"}, 
            {"search_term": "cdk9",   "gene_id_hgnc":"1780","gene_id_ncbi": "1025", "name": "Cyclin-Dependent Kinase 9 (CDK9)", "organism":"Human","taxonomy_id_ncbi":"9606"},
            {"search_term": "id3",    "gene_id_hgnc":"5362","gene_id_ncbi": "3399", "name": "Inhibitor Of DNA Binding 3 (ID3)", "organism":"Mouse","taxonomy_id_ncbi":"1758"},
            {"search_term": "k9me3",  "name": "Histone 3 (K9me3) Bivalent Methylation Factor"},
            {"search_term": "ebna",   "name": "EBV Nuclear Antigen (EBNA)"},
            {"search_term": "mmp",    "name": "Matrix metalloproteinases (MMPs)", "organism":"Mouse","taxonomy_id_ncbi":"1758"}, 
            {"search_term": " evi+",  "name": "Ecotropic Viral Integration (EVI)", "organism":"Mouse","taxonomy_id_ncbi":"1758"}, 
            {"search_term": " tet+",  "name": "Ten Eleven Translocation (TET)", "organism":"Mouse","taxonomy_id_ncbi":"1758"}]
     
              
gene_df = pd.DataFrame(gene_map).rename(columns = {"name":"gene_name", "organism":"organism_name"})
gene_df["gene_symbol"] = gene_df["gene_name"].str.extract(r'\((.*?)\)')
gene_df["gene_uuid"] = gene_df.apply(lambda _: str(uuid.uuid4()), axis=1)

# Initialize links
gene_folder_links = []

for index, folder in folder_df.iterrows():
  search_field = folder["foldername"] + " " + folder["jj_description"]
  
  for _, gene in gene_df.iterrows():
    if any(g in search_field.lower() for g in gene["search_term"].split(",")):
      row = {
                "gene_uuid":gene["gene_uuid"],
                "folder_uuid":folder["folder_uuid"],
                "seqvariation_type":""
              }
      gene_folder_links.append(row)
      folder_searchterms[folder["foldername"]].add(gene["search_term"])

gene_folder_df = pd.DataFrame(gene_folder_links)

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Organism parsing_________________________
organism_map = [{"search_term":"human,hek,goals,optin,hwgs", "name":"Human", "latin_name":"Homo sapiens", "taxonomy_id_ncbi":"9606"},
                {"search_term":"gf mice,mouse", "name": "Mouse", "latin_name":"Mus musculus", "taxonomy_id_ncbi":"1758"}]
                  
organism_df = pd.DataFrame(organism_map).rename(columns = {"name":"organism_name"})
organism_df["organism_uuid"] = organism_df.apply(lambda _: str(uuid.uuid4()), axis=1)

# Initialize links
organism_folder_links = []

for index, folder in folder_df.iterrows():
  search_field = folder["foldername"] + " " + folder["jj_description"]
  
  for _, organism in organism_df.iterrows():
    if any(g in search_field.lower() for g in organism["search_term"].split(",")):
      row = {
                "organism_uuid":organism["organism_uuid"],
                "folder_uuid":folder["folder_uuid"]
                }
      organism_folder_links.append(row)
      folder_searchterms[folder["foldername"]].add(organism["search_term"])

organism_folder_df = pd.DataFrame(organism_folder_links)

  
                  
#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Sequencing Company parsing_________________________
seqcompany_map = [{"search_term":"gencove", "name":"Gencove"},
                 {"search_term":"novogene", "name": "Novogene"},
                 {"search_term":"10x", "name":"10x"},
                 {"search_term":"bgi", "name":"BGI"},
                 {"search_term":"miseq,novoseq", "name":"Illumina"},
                 {"search_term":"genewiz", "name": "GENEWIZ"},
                 {"search_term":"atavistik", "name":"Atavistik Bio"},
                 {"search_term":"qiagen", "name":"Qiagen"},
                 {"search_term":"zymo", "name":"Zymo Research"},
                 {"search_term":"gain therapeutics", "name":"Gain Therapeutics"}]
   
seqcompany_df = pd.DataFrame(seqcompany_map).rename(columns = {"name":"seqcompany_name"})
seqcompany_df["seqcompany_uuid"] = seqcompany_df.apply(lambda _: str(uuid.uuid4()), axis=1)

# Initialize links
seqcompany_folder_links = []

for index, folder in folder_df.iterrows():
  search_field = folder["foldername"] + " " + folder["jj_description"]
  
  for _, seqcompany in seqcompany_df.iterrows():
    if any(g in search_field.lower() for g in seqcompany["search_term"].split(",")):
      row = {
                "seqcompany_uuid":seqcompany["seqcompany_uuid"],
                "folder_uuid":folder["folder_uuid"]
                }
      seqcompany_folder_links.append(row)
      folder_searchterms[folder["foldername"]].add(seqcompany["search_term"])

seqcompany_folder_df = pd.DataFrame(seqcompany_folder_links)

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________institute parsing_________________________
institute_map = [{"search_term": "cooper,cuh", "name": "Cooper University Hospital (CUH)"},
                 {"search_term": "gsk", "name": "GSK"},
                 {"search_term": "coriell", "name": "Coriell Institute for Medical Research"},
                 {"search_term": "cori", "name": "Camden Opioid Research Initiative (CORI)"},
                 {"search_term": "ninds", "name": "National Institute of Neurological Disorders and Stroke (NINDS)"},
                 {"search_term": "cpmc", "name": "Coriell Personalized Medicine Collaborative"}, 
                 {"search_term": "fels", "name": "Fels Institute for Cancer Research and Molecular Biology"}
                 ]  
   
institute_df = pd.DataFrame(institute_map).rename(columns = {"name":"institute_name"})
institute_df["institute_uuid"] = institute_df.apply(lambda _: str(uuid.uuid4()), axis=1)

# Initialize links
institute_folder_links = []

for index, folder in folder_df.iterrows():
  search_field = folder["foldername"] + " " + folder["jj_description"]
  
  for _, institute in institute_df.iterrows():
    if any(g in search_field.lower() for g in institute["search_term"].split(",")):
      row = {
                "institute_uuid":institute["institute_uuid"],
                "folder_uuid":folder["folder_uuid"]
                }
      institute_folder_links.append(row)
      folder_searchterms[folder["foldername"]].add(institute["search_term"])

institute_folder_df = pd.DataFrame(institute_folder_links)

  
#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Study parsing_________________________
study_map = [{"search_term": "optin", "name": "Optimizing Pain Treatment In New Jersey (OPTIN) Study"},
             {"search_term": "goals", "name": "Genomics of Opioid Addiction Longitudinal Study (GOALS) Study"}]  
   
study_df = pd.DataFrame(study_map).rename(columns = {"name":"study_name"})
study_df["study_uuid"] = study_df.apply(lambda _: str(uuid.uuid4()), axis=1)

# Initialize links
study_folder_links = []

for index, folder in folder_df.iterrows():
  search_field = folder["foldername"] + " " + folder["jj_description"]
  
  for _, study in study_df.iterrows():
    if any(g in search_field.lower() for g in study["search_term"].split(",")):
      row = {"study_uuid":study["study_uuid"],
             "folder_uuid":folder["folder_uuid"]}
      study_folder_links.append(row)
      folder_searchterms[folder["foldername"]].add(study["search_term"])

study_folder_df = pd.DataFrame(study_folder_links)

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Treatment parsing_________________________
treatment_map = [{"search_term": "dmso", "name": "Control: Dimethyl Sulfoxide (DMSO)"},
                   {"search_term": "dac", "name": "Decitabine (DAC)"},
                   {"search_term": "thz1", "name": "(THZ1)"},
                   {"search_term": "bsj ", "name": "(BSJ)"},
                   {"search_term": "bsj4", "name": "(BSJ4)"},
                   {"search_term": "protac", "name": "Proteolysis Targeting Chimera (PROTAC)"},
                   {"search_term": "olaparib", "name": "Olaparib"},
                   {"search_term": "gilterinib", "name": "Gilterinib"},
                   {"search_term": "5aza", "name": "5-Azacytidine (5-aza)"},
                   {"search_term": "azd", "name": "AZD-7648 DNA-PK inhibitor (AZD)"},
                   {"search_term": "bdnf",   "name": "Brain-Derived Neurotrophic Factor (BDNF)"}, # Double check
                   {"search_term": "c10orf125", "name": "C10orf125 Polyclonal Antibody"} # Double check
                   ] 
treatment_df = pd.DataFrame(treatment_map).rename(columns = {"name":"treatment_name"})
treatment_df["treatment_uuid"] = treatment_df.apply(lambda _: str(uuid.uuid4()), axis=1)
treatment_df["treatment_symbol"] = treatment_df["treatment_name"].str.extract(r'\((.*?)\)')

# Initialize links
treatment_folder_links = []

for index, folder in folder_df.iterrows():
  search_field = folder["foldername"] + " " + folder["jj_description"]
  
  for _, treatment in treatment_df.iterrows():
    if any(g in search_field.lower() for g in treatment["search_term"].split(",")):
      row = {"treatment_uuid":treatment["treatment_uuid"],
             "folder_uuid":folder["folder_uuid"]}
      treatment_folder_links.append(row)
      folder_searchterms[folder["foldername"]].add(treatment["search_term"])

treatment_folder_df = pd.DataFrame(treatment_folder_links)


#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Disease parsing_________________________
# Initialize disease df
disease_map = [{"disease_category":"Cancer","disease_name":"Pancreatic ductal adenocarcinoma (PDA)","disease_symbol":"PDA","disease_id_ncit":"C9120"},
               {"disease_category":"Cancer","disease_name":"Colon adenocarcinoma","disease_id_ncit":"C4349","search_term":"crc,colon_tumors,colon_cancer,coloncancer"},
               {"disease_category":"Cancer","disease_name":"Pancreatic serotonin-producing neuroendocrine tumor","disease_id_ncit":"C4446"},
               {"disease_category":"Cancer","disease_name":"Prostate carcinoma","disease_id_ncit":"C4863"},
               {"disease_category":"Cancer","disease_name":"Gastric adenocarcinoma","disease_id_ncit":"C4004"},
               {"disease_category":"Cancer","disease_name":"Adult B acute lymphoblastic leukemia (ALL)","disease_symbol":"ALL","disease_id_ncit":"C9143"},
               {"disease_category":"Disorders of Carbohydrate Metabolism","disease_name":"Maroteaux-Lamy syndrome (MPS 6)","disease_symbol":"MPS 6","disease_id_ncit":"C61264"},
               {"disease_category":"Cancer","disease_name":"Childhood acute monocytic leukemia","disease_id_ncit":"C9163"},
               {"disease_category":"Cancer","disease_name":"Breast adenocarcinoma","disease_id_ncit":"C5214"},
               {"disease_category":"Cancer","disease_name":"Leukemia","disease_id_ncit":"C3161","search_term":"leukemia"},
               {"disease_category":"Neurodegenerative Disease","disease_name":"Parkinson Disease","disease_id_ncit":"C26845","search_term":"parkinson"},
               {"disease_category":"Cancer","disease_name":"Chronic Myeloid Leukemia (CML)","disease_symbol":"CML","search_term":"cml"},
               {"disease_category":"Cancer","disease_name":"Acute Myeloid Leukemia (AML)","disease_symbol":"AML","disease_id_ncit":"C3171","search_term":"aml"}]

disease_df = pd.DataFrame(disease_map)
disease_df["disease_uuid"] = disease_df.apply(lambda _: str(uuid.uuid4()), axis=1)

# Initialize folder links
disease_folder_links = []

for index, folder in folder_df.iterrows():
  search_field = folder["foldername"] + " " + folder["jj_description"]
  
  for _, disease in disease_df[disease_df["search_term"].notnull()].iterrows():
    if any(g in search_field.lower() for g in disease["search_term"].split(",")):
      row = {"disease_uuid":disease["disease_uuid"],
             "folder_uuid":folder["folder_uuid"]}
                
      disease_folder_links.append(row)
      folder_searchterms[folder["foldername"]].add(disease["search_term"])
      
disease_folder_df = pd.DataFrame(disease_folder_links)


#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Cell Line parsing_________________________
cell_line_df = pd.read_csv("./data/test-app/tables/cell_lines.csv").rename(columns = {"name":"cell_line_name"})

# Initialize folder links
cell_line_folder_links = []

for index, folder in folder_df.iterrows():
  search_field = folder["foldername"] + " " + folder["jj_description"]
  
  for _, cell_line in cell_line_df.iterrows():
    if any(g in search_field.lower() for g in cell_line["search_term"].split(",")):
      row = {"cell_line_uuid":cell_line["cell_line_uuid"],
             "folder_uuid":folder["folder_uuid"]}
                
      cell_line_folder_links.append(row)
      folder_searchterms[folder["foldername"]].add(cell_line["search_term"])

cell_line_folder_df = pd.DataFrame(cell_line_folder_links)


#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Researcher parsing_________________________
researcher_map = [{"search_term": "woonbok",  "name": "Woonbok Chung",      "institute": "Coriell",  "lab": "The Issa & Jelenik Lab"},
                  {"search_term": "jj",       "name": "Jaroslav Jelinek",   "institute": "Coriell",  "lab": "The Issa & Jelenik Lab"},
                  {"search_term": "shoghag",  "name": "Shoghag Panjarian",  "institute": "Coriell",  "lab": "The Issa & Jelenik Lab"},
                  {"search_term": "himani",   "name": "Himani Vaidya",      "institute": "Coriell",  "lab": "The Issa & Jelenik Lab"},
                  {"search_term": "peace",    "name": "Peace Park",         "institute": "Coriell",  "lab": "The Issa & Jelenik Lab"},
                  {"search_term": "matt",     "name": "Matthew Walt",       "institute": "Coriell",  "lab": "The Issa & Jelenik Lab"}, #Double Check
                  {"search_term": "jian",     "name": "Jian Huang",         "institute": "Coriell",  "lab": "The Jian Huang Lab"},
                  {"search_term": "zhaorui",  "name": "Zhaorui Lian",       "institute": "Coriell",  "lab": "The Jian Huang Lab"},
                  {"search_term": "zhang",    "name": "Yi Zhang",           "institute": "",  "lab": ""}, #Double Check
                  {"search_term": "engel",    "name": "Nora Engel",         "institute": "Coriell",  "lab": "The Nora Engel Lab"},
                  {"search_term": "diego",    "name": "Diego Morales",      "institute": "Coriell",  "lab": ""},
                  {"search_term": "wang",     "name": "Peng Wang",          "institute": "Coriell",  "lab": ""}, #Double Check
                  {"search_term": "laura",    "name": "Laura Scheinfeldt",  "institute": "Coriell","lab": "The Scheinfeldt Lab"},
                  {"search_term": "platoff",  "name": "Rebecca Platoff",    "institute": "Cooper University Hospital (CUH)","lab": ""},
                  {"search_term": "bela",     "name": "Bela Patel",         "institute": "Fels Institute for Cancer Research and Molecular Biology","lab": ""},
                  {"search_term": "whelan",   "name": "Kelly Whelan",       "institute": "Fels Institute for Cancer Research and Molecular Biology","lab": ""},
                  {"search_term": "italo",    "name": "Italo Tempera",      "institute": "Fels Institute for Cancer Research and Molecular Biology","lab": ""},
                  {"search_term": "tempera",  "name": "Italo Tempera",      "institute": "Fels Institute for Cancer Research and Molecular Biology","lab": ""},
                  {"search_term": "hyeseon",  "name": "HyeSeon",            "institute": "","lab": ""},
                  {"search_term": "sierra",   "name": "Sierra",             "institute": "","lab": ""},
                  {"search_term": "gigi",     "name": "Gigi",               "institute": "","lab": ""},
                  {"search_term": "ying",     "name": "Ying","institute": "","lab": ""},
                  {"search_term": "olivia",   "name": "Olivia","institute": "", "lab": ""},
                  {"search_term": "zach",     "name": "Zach","institute": "","lab": ""},
                  {"search_term": "wes",      "name": "Wes","institute": "","lab": ""}]
                  
                  
researcher_df = pd.DataFrame(researcher_map).rename(columns = {"name":"researcher_name"})
researcher_df["researcher_uuid"] = researcher_df.apply(lambda _: str(uuid.uuid4()), axis=1)

# Initialize links
researcher_folder_links = []

for index, folder in folder_df.iterrows():
  search_field = folder["foldername"] + " " + folder["jj_description"]
  
  for _, researcher in researcher_df.iterrows():
    if any(g in search_field.lower() for g in researcher["search_term"].split(",")):
      row = {
                "researcher_uuid":researcher["researcher_uuid"],
                "folder_uuid":folder["folder_uuid"]
                }
      researcher_folder_links.append(row)
      folder_searchterms[folder["foldername"]].add(researcher["search_term"])

researcher_folder_df = pd.DataFrame(researcher_folder_links)
#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Lab parsing_________________________
lab_map = [{"search_term":"engel",      "name":"The Nora Engel Lab"},
            {"search_term":"jj",          "name":"The Issa & Jelenik Lab"},
            {"search_term":"jian_huang_lab",  "name":"The Jian Huang Lab"},
            {"search_term":"jian",        "name":"The Jian Huang Lab"},
            {"search_term":"chen",        "name":"The Luke Chen Lab"},
            {"search_term":"shumei",      "name":"The Shumei Song Lab"},
            {"search_term":"scheinfeldt", "name":"The Scheinfeldt Lab"}]
            
lab_df = pd.DataFrame(lab_map).rename(columns = {"name":"lab_name"})
lab_df["lab_uuid"] = lab_df.apply(lambda _: str(uuid.uuid4()), axis=1)

# Initialize links
lab_folder_links = []

for index, folder in folder_df.iterrows():
  search_field = folder["foldername"] + " " + folder["jj_description"]
  
  for _, lab in lab_df.iterrows():
    if any(g in search_field.lower() for g in lab["search_term"].split(",")):
      row = {
                "lab_uuid":lab["lab_uuid"],
                "folder_uuid":folder["folder_uuid"]
                }
      lab_folder_links.append(row)
      folder_searchterms[folder["foldername"]].add(lab["search_term"])

lab_folder_df = pd.DataFrame(lab_folder_links)

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Experiment parsing_________________________
experiment_map = [{"search_term":"RNA-seq,rnaseq", "name":"RNA-Seq", "experiment_symbol":"RNA-Seq"},
                  {"search_term":"eukmrnaseq,mrna", "name": "RNA-Seq", "subtype":"mRNA", "experiment_symbol":"RNA-Seq"},
                  {"search_term":"grna", "name": "RNA-Seq", "subtype":"gRNA", "experiment_symbol":"RNA-Seq"},
                  {"search_term":"lncrnaseq,lncrna", "name": "RNA-Seq", "subtype":"Long Noncoding RNA (LncRNA)", "experiment_symbol":"RNA-Seq"},
                  {"search_term":"low pass WGS,hwgs,wgs", "name": "Whole Genome Sequencing (WGS)", "subtype":"Low Pass", "experiment_symbol":"WGS"},
                  {"search_term":"crispr", "name": "CRISPR", "experiment_symbol":"CRISPR"},
                  {"search_term":"scrnaseq,scrna,scrnaseq,sc_rnaseq,scrna-seq,", "name": "RNA-Seq", "subtype":"Single Cell RNA-Seq (scRNA-seq)", "experiment_symbol":"RNA-Seq"},
                  {"search_term":"snatacseq,sn_atacseq", "name": "ATAC-Seq", "subtype": "Single Nucleus ATAC-Seq (snATAC-seq)", "experiment_symbol":"ATAC-Seq"},
                  {"search_term":"sn_rnaseq,snrnaseq", "name": "RNA-Seq", "subtype": "Single Nucleus RNA-Seq (snRNA-seq)", "experiment_symbol":"RNA-Seq"},
                  {"search_term":"sc_atacseq,sc_atac,sc_atac-seq,scatac-seq", "name": "ATAC-Seq", "subtype": "Single Cell ATAC-Seq (scATAC-seq)", "experiment_symbol":"ATAC-Seq"},
                  {"search_term":"rrbs", "name": "Reduced-Representation Bisulfite Sequencing (RRBS-seq)", "experiment_symbol":"RRBS-Seq"},
                  {"search_term":"dream", "name": "DREAM", "experiment_symbol":"Dream"},
                  {"search_term":"16s", "name": "16S rRNA", "experiment_symbol":"16S rRNA"},
                  {"search_term":"cutrun,cut_run,cut&run", "name": "CUT&RUN", "experiment_symbol":"CUT&RUN"},
                  {"search_term":"chip", "name":"Chromatin Immunoprecipitation Sequencing (ChIP-Seq)", "experiment_symbol":"ChIP-Seq"},
                  {"search_term":"atacseq,atac-seq,atac_seq", "name": "ATAC-Seq", "experiment_symbol":"ATAC-Seq"},
                  {"search_term":"bisulfite_amplicon,bisulfite amplicon,bisamp,bis_amplicon,ampliseq", "name": "Bisulfite Amplicon Sequencing (BSAS)", "experiment_symbol":"BSAS"}]
                  
experiment_df = pd.DataFrame(experiment_map).rename(columns = {"name":"experiment_name"})
experiment_df["experiment_uuid"] = experiment_df.apply(lambda _: str(uuid.uuid4()), axis=1)
experiment_df["experiment_category_uuid"] = experiment_df.groupby("experiment_name")["experiment_name"].transform(lambda _: str(uuid.uuid4()))

# Initialize links
experiment_folder_links = []

for index, folder in folder_df.iterrows():
  search_field = folder["foldername"] + " " + folder["jj_description"]
  
  for _, experiment in experiment_df.iterrows():
    if any(g in search_field.lower() for g in experiment["search_term"].split(",")):
      row = {
                "experiment_uuid":experiment["experiment_uuid"],
                "folder_uuid":folder["folder_uuid"]
                }
      experiment_folder_links.append(row)
      folder_searchterms[folder["foldername"]].add(experiment["search_term"])

experiment_folder_df = pd.DataFrame(experiment_folder_links)

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Tissue parsing_________________________
tissue_map =   [{"search_term":"cord blood", "name":"Cord Blood", "tissue_id_bto":"BTO_0004053", "tissue_id_uberon":"UBERON_0012168"},
                {"search_term":"blood", "name":"Blood", "tissue_id_bto":"BTO_0000089", "tissue_id_uberon":"UBERON_0000178"},
                {"search_term":"skeletal muscle", "name":"Skeletal Muscle", "tissue_id_bto":"BTO_0001103", "tissue_id_uberon":"UBERON_0001134"},
                {"search_term":"heart", "name": "Heart", "tissue_id_bto":"BTO_0000562", "tissue_id_uberon":"UBERON_0000948"},
                {"search_term":"kidney", "name": "Kidney", "tissue_id_bto":"BTO_0000671", "tissue_id_uberon":"UBERON_0002113"},
                {"search_term":"saliva", "name":"Saliva", "tissue_id_bto":"BTO_0001202", "tissue_id_uberon":"UBERON_0001836"},
                {"search_term":"spleen", "name": "Spleen", "tissue_id_bto":"BTO_0001281", "tissue_id_uberon":"UBERON_0002106"},
                {"search_term":"colon,crc", "name": "Colon", "tissue_id_bto":"TO_0000269", "tissue_id_uberon":"UBERON_0001155"},
                {"search_term":"organoid", "name": "Organoid"},
                {"search_term":"brainoids", "name": "Brain Organoid"},
                {"search_term":"brain", "name": "Brain", "tissue_id_bto":"BTO_0000142", "tissue_id_uberon":"UBERON_0000955"},
                {"search_term":"esophagus", "name": "Esophagus", "tissue_id_bto":"BTO_0000959", "tissue_id_uberon":"UBERON_0001043"},
                {"search_term":"intestine", "name": "Intestine", "tissue_id_bto":"BTO_0000648", "tissue_id_uberon":"UBERON_0000160"},
                {"search_term":"aorta", "name": "Aorta", "tissue_id_bto":"BTO_0000135", "tissue_id_uberon":"UBERON_0000947"}]
  
tissue_df = pd.DataFrame(tissue_map).rename(columns = {"name":"tissue_name"})
tissue_df["tissue_uuid"] = tissue_df.apply(lambda _: str(uuid.uuid4()), axis=1)

# Initialize links
tissue_folder_links = []
newtissue_map = []

for index, folder in folder_df.iterrows():
  search_field = folder["foldername"] + " " + folder["jj_description"]
  
  for _, tissue in tissue_df.iterrows():
    if any(g in search_field.lower() for g in tissue["search_term"].split(",")):
      row = {
                "tissue_uuid":tissue["tissue_uuid"],
                "folder_uuid":folder["folder_uuid"]
                }
      tissue_folder_links.append(row)
      folder_searchterms[folder["foldername"]].add(tissue["search_term"])

tissue_folder_df = pd.DataFrame(tissue_folder_links)

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Cell parsing_________________________
cell_map =     [{"search_term": "hsc",            "name": "Hematopoietic Stem Cells (HSCs)", "cell_id_uberon":"CL_0000037"},
                {"search_term": "endo",           "name": "Endothelial Cells", "cell_id_uberon":"CL_0000115"}]
                
cell_df = pd.DataFrame(cell_map).rename(columns = {"name":"cell_name"})
cell_df["cell_uuid"] = cell_df.apply(lambda _: str(uuid.uuid4()), axis=1)

# Initialize links
cell_folder_links = []

for index, folder in folder_df.iterrows():
  search_field = folder["foldername"] + " " + folder["jj_description"]
  
  for _, cell in cell_df.iterrows():
    if any(g in search_field.lower() for g in cell["search_term"].split(",")):
      row = {
                "cell_uuid":cell["cell_uuid"],
                "folder_uuid":folder["folder_uuid"]
                }
      cell_folder_links.append(row)
      folder_searchterms[folder["foldername"]].add(cell["search_term"])

cell_folder_df = pd.DataFrame(cell_folder_links)

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Cell Line <-> Disease Links_________________________
# Initialize disease links
cell_line_disease_links = []

for _, cell_line in cell_line_df[cell_line_df["disease_id_ncit"].notnull()].iterrows():
  
  for _, disease in disease_df[disease_df["disease_id_ncit"] == cell_line["disease_id_ncit"]].iterrows():
    
    row = {"disease_uuid":disease["disease_uuid"],
           "disease_type":cell_line["disease_type"],
           "disease_grade":cell_line["disease_grade"],
           "cell_line_uuid":cell_line["cell_line_uuid"]}
                
    cell_line_disease_links.append(row)

cell_line_disease_df = pd.DataFrame(cell_line_disease_links)

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Cell Line <-> Cell Links_________________________
# Initialize cell links
cell_line_cell_links = []
newcell_map = []

for _, cell_line in cell_line_df[cell_line_df["cell_id_uberon"].notnull()].iterrows():
  
  # Check if already in cell_df
  if len(cell_df[cell_df["cell_id_uberon"]==cell_line["cell_id_uberon"]]) > 0:
      cell_uuid = cell_df[cell_df["cell_id_uberon"]==cell_line["cell_id_uberon"]]["cell_uuid"].unique()[0]
  
  else:
      cell_uuid = str(uuid.uuid4())
      cell = {"cell_uuid":cell_uuid, "cell_id_uberon":cell_line["cell_id_uberon"]}
      if not any(d["cell_id_uberon"] == cell_line["cell_id_uberon"] for d in newcell_map):
        newcell_map.append(cell)
    
  row = {"cell_uuid":cell["cell_uuid"],
          "cell_line_uuid":cell_line["cell_line_uuid"]}
                
  cell_line_cell_links.append(row)

cell_line_cell_df = pd.DataFrame(cell_line_cell_links)
cell_df = pd.concat([cell_df,pd.DataFrame(newcell_map)], ignore_index=True)

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Cell Line <-> Tissue Links_________________________
# Initialize tissue links
cell_line_tissue_links = []
newtissue_map = []

for _, cell_line in cell_line_df[cell_line_df["tissue_id_uberon"].notnull()].iterrows():
  
  # Check if already in tissue_df
  if len(tissue_df[tissue_df["tissue_id_uberon"]==cell_line["tissue_id_uberon"]]) > 0:
      tissue_uuid = tissue_df[tissue_df["tissue_id_uberon"]==cell_line["tissue_id_uberon"]]["tissue_uuid"].unique()[0]

  else:
      tissue_uuid = str(uuid.uuid4())
      tissue = {"tissue_uuid":tissue_uuid, "tissue_id_uberon":cell_line["tissue_id_uberon"]}
      if not any(d["tissue_id_uberon"] == cell_line["tissue_id_uberon"] for d in newtissue_map):
          newtissue_map.append(tissue)
  
  row = {"tissue_uuid":tissue_uuid,
         "cell_line_uuid":cell_line["cell_line_uuid"]}
                
  cell_line_tissue_links.append(row)
    
cell_line_tissue_df = pd.DataFrame(cell_line_tissue_links)
tissue_df = pd.concat([tissue_df,pd.DataFrame(newtissue_map)], ignore_index=True)

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Cell Line <-> Organism Links_________________________
# Initialize organism links
cell_line_organism_links = []
neworganism_map = []

for _, cell_line in cell_line_df[cell_line_df["cellosaurus_id"].notnull()].iterrows():
  
  url = "https://api.cellosaurus.org/cell-line/"+ cell_line["cellosaurus_id"] +"?format=txt&fields=id%2Ccc%2Cox"
  response  = requests.get(url)
  taxonomy_id_ncbi = [t for t in response.text.split("\n") if "NCBI_TaxID=" in t][0].split(";")[0].split("=")[1]
  
  # Check if already in organism_df
  if len(organism_df[organism_df["taxonomy_id_ncbi"]==taxonomy_id_ncbi]) > 0:
      organism_uuid = organism_df[organism_df["taxonomy_id_ncbi"]==taxonomy_id_ncbi]["organism_uuid"].unique()[0]

  else:
      organism_uuid = str(uuid.uuid4())
      organism = {"organism_uuid":organism_uuid, "taxonomy_id_ncbi":taxonomy_id_ncbi}
      if not any(d["taxonomy_id_ncbi"] == cell_line["taxonomy_id_ncbi"] for d in neworganism_map):
        neworganism_map.append(organism)
  
  row = {"organism_uuid":organism_uuid,
         "cell_line_uuid":cell_line["cell_line_uuid"]}
                
  cell_line_organism_links.append(row)

for _, cell_line in cell_line_df[cell_line_df["organism"].notnull()].iterrows():
  
  for _, organism in organism_df[organism_df["organism_name"] == cell_line["organism"]].iterrows():
    
    if not any(d["cell_line_uuid"] == cell_line["cell_line_uuid"] for d in cell_line_organism_links):
          
      row = {"organism_uuid":organism["organism_uuid"],
             "cell_line_uuid":cell_line["cell_line_uuid"]}
                  
      cell_line_organism_links.append(row)
      
cell_line_organism_df = pd.DataFrame(cell_line_organism_links)
organism_df = pd.concat([organism_df,pd.DataFrame(neworganism_map)], ignore_index=True)

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Cell Line <-> Gene Links_________________________
# Initialize gene links
cell_line_gene_links = []
newgene_map = []

for _, cell_line in cell_line_df[cell_line_df["cellosaurus_id"].notnull()].iterrows():
  
  url = "https://api.cellosaurus.org/cell-line/"+ cell_line["cellosaurus_id"] +"?format=txt&fields=id%2Ccc%2Cox"
  response  = requests.get(url)
  results = [t for t in response.text.split("\n") if "Sequence variation" in t]
  taxonomy_id_ncbi = [t for t in response.text.split("\n") if "NCBI_TaxID=" in t][0].split(";")[0].split("=")[1]

  for result in results:
    
    seqvariation_type = result.split(";")[0].replace("CC   Sequence variation: ","")
    hgnc_id = result.split(";")[2].strip()
    gene_symbol = result.split(";")[3].strip()
    
    # Check if already in gene_df
    if len(gene_df[gene_df["gene_id_hgnc"]==hgnc_id]) > 0:
      gene_uuid = gene_df[gene_df["gene_id_hgnc"]==hgnc_id]["gene_uuid"].unique()[0]
      gene_df.loc[gene_df["gene_id_hgnc"]==hgnc_id, "taxonomy_id_ncbi"]= taxonomy_id_ncbi

    else:
      gene_uuid = str(uuid.uuid4())
      gene = {"gene_id_hgnc":hgnc_id, "gene_symbol": gene_symbol, "gene_uuid":gene_uuid, "taxonomy_id_ncbi":taxonomy_id_ncbi}
      if not any(d["gene_id_hgnc"] == hgnc_id for d in newgene_map):
        newgene_map.append(gene)
    
    row = {"gene_uuid":gene_uuid,
           "cell_line_uuid":cell_line["cell_line_uuid"],
           "seqvariation_type":seqvariation_type,
           "hgnc_id":hgnc_id}
                
    cell_line_gene_links.append(row)

cell_line_gene_df = pd.DataFrame(cell_line_gene_links)
gene_df = pd.concat([gene_df,pd.DataFrame(newgene_map)], ignore_index=True)

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Gene <-> Organism Links_________________________
# Initialize organism links
gene_organism_links = []

for _, gene in gene_df[gene_df["taxonomy_id_ncbi"].notnull()].iterrows():
  
  for _, organism in organism_df[(organism_df["taxonomy_id_ncbi"] == gene["taxonomy_id_ncbi"]) | (organism_df["organism_name"] == gene["organism_name"])].iterrows():
    
    row = {"organism_uuid":organism["organism_uuid"],
           "gene_uuid":gene["gene_uuid"]}
                
    gene_organism_links.append(row)
    
  
gene_organism_df = pd.DataFrame(gene_organism_links)

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Researcher <-> Institute Links_________________________
# Initialize institute links
researcher_institute_links = []

for _, researcher in researcher_df[researcher_df["institute"].notnull()].iterrows():
  
  for _, institute in institute_df[(institute_df["institute_name"] == researcher["institute"]) | (institute_df["institute_name"] == researcher["institute"])].iterrows():
    
    row = {"institute_uuid":institute["institute_uuid"],
           "researcher_uuid":researcher["researcher_uuid"]}
                
    researcher_institute_links.append(row)
  
researcher_institute_df = pd.DataFrame(researcher_institute_links)


#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Researcher <-> Lab Links_________________________
# Initialize lab links
researcher_lab_links = []

for _, researcher in researcher_df[researcher_df["lab"].notnull()].iterrows():
  
  for _, lab in lab_df[(lab_df["lab_name"] == researcher["lab"]) | (lab_df["lab_name"] == researcher["lab"])].iterrows():
    
    row = {"lab_uuid":lab["lab_uuid"],
           "researcher_uuid":researcher["researcher_uuid"]}
                
    researcher_lab_links.append(row)
  
researcher_lab_df = pd.DataFrame(researcher_lab_links)

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Disease <-> Gene Links_________________________
# Initialize disease links
disease_gene_links = []
newgene_map = []

for _, disease in disease_df[disease_df["disease_id_ncit"].notnull()].iterrows():

  url = "https://api-evsrest.nci.nih.gov/api/v1/concept/ncit/"+disease["disease_id_ncit"]+"?include=full"
  response  = requests.get(url)
  result = json.loads(response.text)
  
  if "inverseRoles" in result:
    assoc_genes = [{k: t[k] for k in ["type","relatedCode"]} for t in result["inverseRoles"] if t["type"] == "Gene_Associated_With_Disease"]
  
    for assoc_gene in assoc_genes:
      url = "https://api-evsrest.nci.nih.gov/api/v1/concept/ncit/"+assoc_gene["relatedCode"]+"?include=full"
      response  = requests.get(url)
      result = json.loads(response.text)
      hgnc_id = [t for t in result["properties"] if t["type"]=="HGNC_ID"]
      hgnc_id = hgnc_id[0]["value"].replace("HGNC:","") if len(hgnc_id)>0 else ""
    
      # Check if already in gene_df
      if len(gene_df[gene_df["gene_id_hgnc"]==hgnc_id]) > 0:
        gene_uuid = gene_df[gene_df["gene_id_hgnc"]==hgnc_id]["gene_uuid"].unique()[0]
        
      else:
        gene_uuid = str(uuid.uuid4())
        gene = {"gene_id_hgnc":hgnc_id, "gene_symbol": result["name"].replace(" Gene",""), "gene_uuid":gene_uuid}
        if not any(d["gene_id_hgnc"] == hgnc_id for d in newgene_map):
          newgene_map.append(gene)
          
      row = {"gene_uuid":gene_uuid,
             "disease_uuid":disease["disease_uuid"]}
                
      disease_gene_links.append(row)

disease_gene_df = pd.DataFrame(disease_gene_links)
gene_df = pd.concat([gene_df,pd.DataFrame(newgene_map)], ignore_index=True)


#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Remove Linker columns from DataFrames________________________
tables = [cell_df,
          cell_folder_df,
          cell_line_cell_df,
          cell_line_df,
          cell_line_disease_df,
          cell_line_folder_df,
          cell_line_gene_df,
          cell_line_organism_df,
          cell_line_tissue_df,
          disease_df,
          disease_folder_df,
          disease_gene_df,
          experiment_df,
          experiment_folder_df,
          folder_df,
          gene_df,
          gene_folder_df,
          gene_organism_df,
          institute_df,
          institute_folder_df,
          lab_df,
          lab_folder_df,
          organism_df,
          organism_folder_df,
          researcher_df,
          researcher_folder_df,
          researcher_institute_df,
          researcher_lab_df,
          seqcompany_df,
          seqcompany_folder_df,
          study_df,
          study_folder_df,
          tissue_df,
          tissue_folder_df,
          treatment_df,
          treatment_folder_df]

tables_str = ["cell_df",
              "cell_folder_df",
              "cell_line_cell_df",
              "cell_line_df",
              "cell_line_disease_df",
              "cell_line_folder_df",
              "cell_line_gene_df",
              "cell_line_organism_df",
              "cell_line_tissue_df",
              "disease_df",
              "disease_folder_df",
              "disease_gene_df",
              "experiment_df",
              "experiment_folder_df",
              "folder_df",
              "gene_df",
              "gene_folder_df",
              "gene_organism_df",
              "institute_df",
              "institute_folder_df",
              "lab_df",
              "lab_folder_df",
              "organism_df",
              "organism_folder_df",
              "researcher_df",
              "researcher_folder_df",
              "researcher_institute_df",
              "researcher_lab_df",
              "seqcompany_df",
              "seqcompany_folder_df",
              "study_df",
              "study_folder_df",
              "tissue_df",
              "tissue_folder_df",
              "treatment_df",
              "treatment_folder_df"]
              
cell_line_gene_cols = ["hgnc_id"]
cell_line_cols = ["organism","disease_category", "disease_name", "disease_type", "disease_grade", "disease_id_ncit", "tissue_type", "tissue_id_uberon", "cell_type", "cell_id_uberon"]
gene_cols = ["organism_name", "taxonomy_id_ncbi"]

cell_line_gene_df = cell_line_gene_df[[col for col in cell_line_gene_df.columns if col not in cell_line_gene_cols]]
cell_line_df = cell_line_df[[col for col in cell_line_df.columns if col not in cell_line_cols]]
gene_df = gene_df[[col for col in gene_df.columns if col not in gene_cols]]

for i in range(0,len(tables)):
  tables[i] = tables[i][[col for col in tables[i].columns if col != "search_term"]]
  path = "./data/test-app/tables/" + tables_str[i] + ".csv"
  tables[i].to_csv(path, index=False)
  print(tables_str[i], ": ", str(tables[i].shape))
  
#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Print DBML for diagram_________________________
with open("./data/test-app/scripts/DBML_script.txt", "w") as text_file:
    
  # Print tables
  for i in range(0,len(tables)):
    
    strs = ["Table "]
    strs.append(tables_str[i])
    strs.append(" {\n")
    for col in tables[i].columns:
      strs.append('"'+col + '" varchar(255) \n')
    strs.append(" }\n")
    for s in strs:
      text_file.write(s)
  
  # Print links to folder
  for i in range(0,len(tables)):
    
    if "_folder" in tables_str[i]:
      
      strs = ["Ref: folder_df.folder_uuid < "]
      strs.append(tables_str[i])
      strs.append(".folder_uuid")
      strs.append("\n")
      for s in strs:
        text_file.write(s)
      
  # Print links to folder
  for i in range(0,len(tables)):
    
    if "_folder" in tables_str[i]:
      
      strs = ["Ref: "]
      strs.append(tables_str[i].replace("_folder",""))
      strs.append(".")
      strs.append(tables_str[i].replace("_folder_df","_uuid"))
      strs.append(" < ")
      strs.append(tables_str[i])
      strs.append(".")
      strs.append(tables_str[i].replace("_folder_df","_uuid"))
      strs.append("\n")
      for s in strs:
        text_file.write(s)
      
  # Print links
  for i in range(0,len(tables)):
    uuids = [c for c in tables[i].columns if ("_uuid" in c) and (c not in ["experiment_category_uuid", "folder_uuid"])]
    
    if len(uuids)>1:
      #print(tables_str[i], uuids)
      
      for uuid in [u for u in uuids if u not in ["experiment_category_uuid", "folder_uuid"]]:
        strs = ["Ref: "]
        strs.append(tables_str[i])
        strs.append(".")
        strs.append(uuid)
        strs.append(" < ")
        strs.append(uuid.replace("_uuid","_df"))
        strs.append(".")
        strs.append(uuid)
        strs.append("\n")
        for s in strs:
          text_file.write(s)
    
  







#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Merge all togehther_________________________
with open("./data/test-app/scripts/merge_script.txt", "w") as text_file:
   
  # Print links to folder
  for i in range(0,len(tables)):
    
    if "_folder" in tables_str[i]:
      
      strs = ["folder_df = folder_df.merge("]
      strs.append(tables_str[i])
      strs.append(", on = 'folder_uuid', how = 'left')\n")
      for s in strs:
        text_file.write(s)
  
  text_file.write("\n")
  
  # Print links to folder
  for i in range(0,len(tables)):
    
    if "_folder" in tables_str[i]:
      
      strs = ["folder_df = folder_df.merge("]
      strs.append(tables_str[i].replace("_folder",""))
      strs.append(", on = '")
      strs.append(tables_str[i].replace("_folder_df","_uuid"))
      strs.append("', how = 'left')\n")
      for s in strs:
        text_file.write(s)
      
  text_file.write("\n")
  
  # Print links
  for i in range(0,len(tables)):
    uuids = [c for c in tables[i].columns if ("_uuid" in c) and (c not in ["experiment_category_uuid", "folder_uuid"])]
    
    if len(uuids)>1:
      for uuid in [u for u in uuids if u not in ["experiment_category_uuid", "folder_uuid"]]:
        strs = ["folder_df = folder_df.merge("]
        strs.append(tables_str[i])
        strs.append(", on = '")
        strs.append(uuid)
        strs.append("', how = 'left')\n")
        for s in strs:
          text_file.write(s)
        


#----------------------------------------------------------------------------------------------------------------------------------------
# TO DO

foldertemp = folder_df[[col for col in folder_df.columns if col not in cell_df.columns]].merge(cell_line_cell_df, on = 'cell_line_uuid', how = 'left').merge(cell_df, on="cell_uuid")
folder3 = pd.concat([folder_df,foldertemp], ignore_index=True)

foldertemp = folder_df[[col for col in folder_df.columns if col not in disease_df.columns]].merge(cell_line_disease_df, on = 'cell_line_uuid', how = 'left').merge(disease_df, on="disease_uuid")
folder3 = pd.concat([folder3,foldertemp], ignore_index=True)

foldertemp = folder_df[[col for col in folder_df.columns if col not in gene_df.columns]].merge(cell_line_gene_df, on = 'cell_line_uuid', how = 'left').merge(gene_df, on="gene_uuid")
folder3 = pd.concat([folder3,foldertemp], ignore_index=True)

foldertemp = folder_df[[col for col in folder_df.columns if col not in organism_df.columns]].merge(cell_line_organism_df, on = 'cell_line_uuid', how = 'left').merge(organism_df, on="organism_uuid")
folder3 = pd.concat([folder3,foldertemp], ignore_index=True)

foldertemp = folder_df[[col for col in folder_df.columns if col not in tissue_df.columns]].merge(cell_line_tissue_df, on = 'cell_line_uuid', how = 'left').merge(tissue_df, on="tissue_uuid")
folder3 = pd.concat([folder3,foldertemp], ignore_index=True)

foldertemp = folder_df[[col for col in folder_df.columns if col not in gene_df.columns]].merge(disease_gene_df, on = 'disease_uuid', how = 'left').merge(gene_df, on="gene_uuid")
folder3 = pd.concat([folder3,foldertemp], ignore_index=True)

foldertemp = folder_df[[col for col in folder_df.columns if col not in organism_df.columns]].merge(gene_organism_df, on = 'gene_uuid', how = 'left').merge(organism_df, on="organism_uuid")
folder3 = pd.concat([folder3,foldertemp], ignore_index=True)

foldertemp = folder_df[[col for col in folder_df.columns if col not in institute_df.columns]].merge(researcher_institute_df, on = 'researcher_uuid', how = 'left').merge(institute_df, on="institute_uuid")
folder3 = pd.concat([folder3,foldertemp], ignore_index=True)

foldertemp = folder_df[[col for col in folder_df.columns if col not in lab_df.columns]].merge(researcher_lab_df, on = 'researcher_uuid', how = 'left').merge(lab_df, on="lab_uuid")
folder3 = pd.concat([folder3,foldertemp], ignore_index=True)


foldertemp1 = folder_df[[col for col in folder_df.columns if col not in gene_df.columns]].merge(cell_line_gene_df, on = 'cell_line_uuid', how = 'left').merge(gene_df, on="gene_uuid")
foldertemp2 = folder_df[[col for col in folder_df.columns if col not in gene_df.columns]].merge(disease_gene_df, on = 'disease_uuid', how = 'left').merge(gene_df, on="gene_uuid")
f3 = foldertemp1[["gene_uuid", "folder_uuid"]].merge(foldertemp2[["gene_uuid", "folder_uuid"]], on = ["gene_uuid","folder_uuid"], how="outer", indicator = True)


#----------------------------------------------------------------------------------------------------------------------------------------
cols = []
for i in range(0,len(tables)):
  cols.extend(tables[i].columns)



#----------------------------------------------------------------------------------------------------------------------------------------

for _, folder in folders_df.iterrows():
  
  
  
  
  
