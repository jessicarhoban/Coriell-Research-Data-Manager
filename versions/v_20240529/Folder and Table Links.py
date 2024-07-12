import pandas as pd
import os
import glob
import re
import xmltodict
import json
import uuid
import requests

#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Get All Folders_________________________
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



#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Folder <-> Table Links (Text Parsing)_________________________
"""

Purpose: 
  
  - Inside a "maps" folder should be various CSV files, one per eventual SQL table:
       Ex: cell_line_map.csv --> cell_line SQL table
           disease_map.csv   --> disease SQL table
           
  - Each of these CSV files contain a column called "search_term", which is a comma-delimited list of 
    phrases found within the "mnt/data/research_data/" folders. These "search_terms" map to a specific
    classificiation (either cell lines, experiment types, etc)
       Ex:  search_term = "crc,colon_cancer" <-->  disease_name = "Colorectal Cancer"
            search_term = "atac"             <-->  experiment_name = "ATAC-Seq"
            
  - This code section:
        1) Reads in all map CSV files as DataFrames, representing future SQL tables (held in map_dfs)
        2) Iterates over all folders in mnt/data/research_data/ (held in folder_df)
        3) Creates new "linker" DataFrames, connecting map DFs to folder DFs
              Ex:  map_df      <-->  folder_df  ==  l_map_folder
                   disease_df  <-->  folder_df  ==  l_disease_folder
        
  - This allows us to search, say, for folders with ATAC-Seq data, or folders containing experiments
    on the cell line MIA PaCa-2. By having a separate "linker" table, this also allows for many-many relationships,
    rather than the 1-1 relationships created by simply linking two column names on their own.

"""
#_________________________Gene parsing_________________________
gene_df= pd.read_csv("./data/test-app/maps/gene_map.csv",dtype=str)

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
organism_df= pd.read_csv("./data/test-app/maps/organism_map.csv",dtype=str)

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
seqcompany_df= pd.read_csv("./data/test-app/maps/seqcompany_map.csv",dtype=str)

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
institute_df= pd.read_csv("./data/test-app/maps/institute_map.csv",dtype=str)

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
study_df= pd.read_csv("./data/test-app/maps/study_map.csv",dtype=str)

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
# Read in text search mappings
treatment_df= pd.read_csv("./data/test-app/maps/treatment_map.csv",dtype=str)

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
# Read in text search mappings
disease_df= pd.read_csv("./data/test-app/maps/disease_map.csv",dtype=str)

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
# Read in text search mappings
cell_line_df= pd.read_csv("./data/test-app/maps/cell_line_map.csv",dtype=str)

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
# Read in text search mappings
researcher_df= pd.read_csv("./data/test-app/maps/researcher_map.csv",dtype=str)

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
# Read in text search mappings
lab_df= pd.read_csv("./data/test-app/maps/lab_map.csv",dtype=str)

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
# Read in text search mappings
experiment_df= pd.read_csv("./data/test-app/maps/experiment_map.csv",dtype=str)

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
# Read in text search mappings
tissue_df= pd.read_csv("./data/test-app/maps/tissue_map.csv",dtype=str)

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
# Read in text search mappings
cell_df= pd.read_csv("./data/test-app/maps/cell_map.csv",dtype=str)

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
#_________________________Gene <-> Organism Links_________________________
# Initialize organism links
gene_organism_links = []

for _, gene in gene_df[gene_df["taxonomy_id_ncbi"].notnull()].iterrows():
  
  for _, organism in organism_df[(organism_df["taxonomy_id_ncbi"] == gene["taxonomy_id_ncbi"])].iterrows():
    
    row = {"organism_uuid":organism["organism_uuid"],
           "gene_uuid":gene["gene_uuid"]}
                
    gene_organism_links.append(row)
    
  
gene_organism_df = pd.DataFrame(gene_organism_links)


#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Researcher <-> Lab Links_________________________
# Initialize lab links
researcher_lab_links = []

for _, researcher in researcher_df[researcher_df["lab"].notnull()].iterrows():
  
  for _, lab in lab_df[(lab_df["lab_name"] == researcher["lab"])].iterrows():
    
    row = {"lab_uuid":lab["lab_uuid"],
           "researcher_uuid":researcher["researcher_uuid"]}
                
    researcher_lab_links.append(row)
  
researcher_lab_df = pd.DataFrame(researcher_lab_links)

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
