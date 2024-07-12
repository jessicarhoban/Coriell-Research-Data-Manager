import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

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
def get_folder_df():
  
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
  
  return(folder_df, folders)


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
def get_map_df(path):

	#____Get all CSV map files____
	df = pd.read_csv(path, dtype=str) # Read in CSV
	df.df_uuid = [col for col in df.columns if "uuid" in col][0] # Grab UUID column
	df.df_name = df.df_uuid.split("_uuid")[0] # Establish DataFrame name
	return(df)



#----------------------------------------------------------------------------------------------------------------------------------------
def get_link_df(map_df, folder_searchterms):
      
  # Initialize rows that will create the link DataFrames
  link_rows = []
  
  # Iterate over folders
  for _, folder in folder_df.iterrows():
    
    # Define the text string that we'll be searching for search_terms
    search_field = folder["foldername"] + " " + folder["jj_description"]
    
    # Iterate over the rows in the map DataFrame
    for _, map_row in map_df[map_df["search_term"].notnull()].iterrows():
      
      # If any of the search terms match
      if any(r in search_field.lower() for r in map_row["search_term"].split(",")):
        
        # Create row for the link DataFrame
        link_row =  {map_df.df_uuid: map_row[map_df.df_uuid],
                    "folder_uuid":  folder["folder_uuid"]}
        # Append to row list
        link_rows.append(link_row)
        # Append to set to show that text string was "used"
        folder_searchterms[folder["foldername"]].add(map_row["search_term"]) 
    
  # Convert link_rows to DataFrame
  link_df = pd.DataFrame(link_rows)
  # Set DataFrame name
  link_df.df_name = "l_" + map_df.df_name + "_folder"
  # Append to list of link DataFrames
  return(link_df, folder_searchterms)



#----------------------------------------------------------------------------------------------------------------------------------------
def get_map_link_df(table1, table2, table1_mergecol, table2_mergecol, 
                    table1_colstobring = [], table2_colstobring = []):
  
  table12_links = []
  table2_newrows = []
  
  for _, table1_row in table1[table1[table1_mergecol].notnull()].iterrows():
    
    mask = table2[table2_mergecol] == table1_row[table1_mergecol]
    
    if len(table2[mask]) > 0:
      for _, table2_row in table2[mask].iterrows():
        
        row = {table2.df_uuid : table2_row[table2.df_uuid],
               table1.df_uuid : table1_row[table1.df_uuid]}
               
        for col in table1_colstobring:
          row[col] = table1_row[col]
          
        for col in table2_colstobring:
          row[col] = table2_row[col]
        
        table12_links.append(row)
      
    else:
      
      if not any(d[table2_mergecol] == table1_row[table1_mergecol] for d in table2_newrows):
        table2_newuuid = str(uuid.uuid4())
        table2_newrow = {table2.df_uuid: table2_newuuid,
                         table2_mergecol: table1_row[table1_mergecol]}
                         
        table2_newrows.append(table2_newrow)
      
      else:
        table2_newuuid = [d for d in table2_newrows if d[table2_mergecol] == table1_row[table1_mergecol]][0][table2.df_uuid]
        
      row = {table2.df_uuid : table2_newuuid,
             table1.df_uuid : table1_row[table1.df_uuid]}
      
      for col in table1_colstobring:
          row[col] = table1_row[col]
          
      table12_links.append(row)
    
  table2_dfname = table2.df_name
  table2_dfuuid = table2.df_uuid
  table2 = pd.concat([table2,pd.DataFrame(table2_newrows)], ignore_index=True)
  table2.df_name = table2_dfname
  table2.df_uuid = table2_dfuuid
  
  table12_df = pd.DataFrame(table12_links)
  table12_df.df_name = table1.df_name.replace("_df","") + table2.df_name
  return(table12_df, table2)
  
  
#----------------------------------------------------------------------------------------------------------------------------------------

# Get folder
folder_df, folders = get_folder_df()

# Create empty sets to hold which search_terms were used per folder
folder_searchterms = {folder.split("/")[-1]:set() for folder in folders}

all_folders = []

# Get map DFs
cell_line_df = get_map_df("./data/test-app/maps/cell_line_map.csv")
cell_df = get_map_df("./data/test-app/maps/cell_map.csv")
disease_df = get_map_df("./data/test-app/maps/disease_map.csv")
experiment_df = get_map_df("./data/test-app/maps/experiment_map.csv")
gene_df = get_map_df("./data/test-app/maps/gene_map.csv")
institute_df = get_map_df("./data/test-app/maps/institute_map.csv")
lab_df = get_map_df("./data/test-app/maps/lab_map.csv")
organism_df = get_map_df("./data/test-app/maps/organism_map.csv")
researcher_df = get_map_df("./data/test-app/maps/researcher_map.csv")
seqcompany_df = get_map_df("./data/test-app/maps/seqcompany_map.csv")
study_df = get_map_df("./data/test-app/maps/study_map.csv")
tissue_df = get_map_df("./data/test-app/maps/tissue_map.csv")
treatment_df = get_map_df("./data/test-app/maps/treatment_map.csv")

# Get map <-> folder
cell_line_folder_df, folder_searchterms = get_link_df(cell_line_df, folder_searchterms)
cell_folder_df, folder_searchterms = get_link_df(cell_df, folder_searchterms)
disease_folder_df, folder_searchterms = get_link_df(disease_df, folder_searchterms)
experiment_folder_df, folder_searchterms = get_link_df(experiment_df, folder_searchterms)
gene_folder_df, folder_searchterms = get_link_df(gene_df, folder_searchterms)
institute_folder_df, folder_searchterms = get_link_df(institute_df, folder_searchterms)
lab_folder_df, folder_searchterms = get_link_df(lab_df, folder_searchterms)
organism_folder_df, folder_searchterms = get_link_df(organism_df, folder_searchterms)
researcher_folder_df, folder_searchterms = get_link_df(researcher_df, folder_searchterms)
seqcompany_folder_df, folder_searchterms = get_link_df(seqcompany_df, folder_searchterms)
study_folder_df, folder_searchterms = get_link_df(study_df, folder_searchterms)
tissue_folder_df, folder_searchterms = get_link_df(tissue_df, folder_searchterms)
treatment_folder_df, folder_searchterms = get_link_df(treatment_df, folder_searchterms)

# Get map <-> map (simple)
researcher_lab_df, lab_df = get_map_link_df(researcher_df, lab_df, "lab", "lab_name")
researcher_institute_df, institute_df = get_map_link_df(researcher_df, institute_df, "institute", "institute_name")
gene_organism_df, organism_df = get_map_link_df(gene_df, organism_df, "organism_id_ncbi", "organism_id_ncbi")
cell_line_cell_df, cell_df = get_map_link_df(cell_line_df, cell_df, "cell_id_uberon", "cell_id_uberon")

#---------
cell_line_organism_links = []
cell_line_tissue_links = []
cell_line_disease_links = []
cell_line_gene_links = []

# Adding in Cell Line data
for index, cell_line in cell_line_df[cell_line_df["cellosaurus_id"].notnull()].iterrows():
  
  fields = ["id","cc","ox","ag","sx","ca","di","dr"]
  url = "https://api.cellosaurus.org/cell-line/"+ cell_line["cellosaurus_id"] +"?format=txt&fields=" + "%2C".join(fields)
  response  = requests.get(url)
  
  #____ Cell Line specific fields ____
  cell_line_name = re.findall("^ID\s*(.*?)\n", response.text)
  coriell_id = re.findall("DR\s*Coriell;\s(.*?)\n",response.text)
  atcc_id = re.findall("DR\s*ATCC;\s(.*?)\n",response.text)  
  age = re.findall("\nAG\s*(.*?)\n", response.text)
  sex = re.findall("\nSX\s*(.*?)\n", response.text)
  population = re.findall("\nCC\s*Population:\s*(.*?)[;\.]", response.text)
  cell_line_category = re.findall("\nCA\s*(.*?)\n", response.text)  
  transformant_id_ncbi = re.findall("CC\s*Transformant: NCBI_TaxID;\s*(.*?);", response.text)
  transformant_name_ncbi = re.findall("CC\s*Transformant: NCBI_TaxID;.*;\s*(.*?)\n",response.text)
  
  cell_line_df.loc[index]["cell_line_name"] = cell_line_name[0] if len(cell_line_name)>0 else ""
  cell_line_df.loc[index]["coriell_id"] = coriell_id[0] if len(coriell_id)>0 else ""
  cell_line_df.loc[index]["atcc_id"] = atcc_id[0] if len(atcc_id)>0 else ""
  cell_line_df.loc[index]["age"] = age[0] if len(age)>0 else ""
  cell_line_df.loc[index]["sex"] = sex[0] if len(sex)>0 else ""
  cell_line_df.loc[index]["population"] = population[0] if len(population)>0 else ""
  cell_line_df.loc[index]["cell_line_category"] = cell_line_category[0] if len(cell_line_category)>0 else ""
  cell_line_df.loc[index]["transformant_id_ncbi"] = transformant_id_ncbi[0] if len(transformant_id_ncbi)>0 else ""
  cell_line_df.loc[index]["transformant_name_ncbi"] = transformant_name_ncbi[0] if len(transformant_name_ncbi)>0 else ""
  
  #____ Fields that link to other tables ____
  # Links to organism
  #taxonomy_name_ncbi = re.findall("\nOX\s*NCBI_TaxID=.*;\s*!\s*(.*?)\n", response.text)
  #cell_line_df.loc[index]["organism_id_ncbi"] = organism_id_ncbi[0] if len(organism_id_ncbi)>0 else ""
  #cell_line_df.loc[index]["taxonomy_name_ncbi"] = taxonomy_name_ncbi[0] if len(taxonomy_name_ncbi)>0 else ""
  organism_id_ncbi = re.findall("\nOX\s*NCBI_TaxID=(.*?);", response.text)
  
  for tax in organism_id_ncbi:
    row = pd.DataFrame({"organism_id_ncbi":organism_id_ncbi, 
                        "cell_line_uuid":cell_line["cell_line_uuid"]})
    cell_line_organism_links.append(row)
    
  # Links to tissue
  #derived_site_name = re.findall("CC\s*Derived from site:.*;\s*(.*?);",response.text)
  #cell_line_df.loc[index]["derived_site_name"] = derived_site_name[0] if len(derived_site_name)>0 else ""
  #cell_line_df.loc[index]["tissue_id_uberon"] = tissue_id_uberon[0] if len(tissue_id_uberon)>0 else ""
  tissue_id_uberon = re.findall("CC\s*Derived from site:.*UBERON=(.*?).\n",response.text)
  
  for tissue in tissue_id_uberon:
    row = pd.DataFrame({"tissue_id_uberon":tissue_id_uberon, 
                        "cell_line_uuid":cell_line["cell_line_uuid"]})
    cell_line_tissue_links.append(row)
    
  # Links to cell
  #bto = re.findall("DR\s*BTO;\s(.*?)\n",response.text)
  #clo = re.findall("DR\s*CLO;\s(.*?)\n",response.text)
  #for tissue in tissue_id_uberon:
  #  row = pd.DataFrame({"tissue_id_uberon":tissue_id_uberon, 
  #                      "cell_line_uuid":cell_line["cell_line_uuid"]})
  #  cell_line_organism_links.append(row)
    
  # Links to disease
  disease_name_ncit = re.findall("\nDI\s*NCIt;.*;\s*(.*?)\n", response.text)
  disease_id_ncit = re.findall("\nDI\s*NCIt;\s*(.*?);", response.text)
  for disease in disease_id_ncit:
    row = pd.DataFrame({"disease_id_ncit":disease_id_ncit, 
                        "disease_name_ncit":disease_name_ncit, 
                        "cell_line_uuid":cell_line["cell_line_uuid"]})
    cell_line_disease_links.append(row)
    
  # Links to gene
  sequence_variation_matches = [s for s in response.text.split("\n") if "Sequence variation:" in s]
  
  for match in sequence_variation_matches:
    var_type = re.findall("CC\s*Sequence variation:\s*(.*?);", match)
    gene_id_hgnc = re.findall("HGNC;\s*(.*?)[;\s]", match)
    gene_symbol_hgnc = re.findall("HGNC;(?:[^;]*;)\s*(.*?)[;\s]", match)
    zygosity = re.findall(".*Zygosity=(.*?)\s", match)
    clinvar = re.findall(".*ClinVar=(.*?);", match)
    
    row =   {
            "var_type":var_type,
            "gene_id_hgnc": gene_id_hgnc,
            "gene_symbol_hgnc": gene_symbol_hgnc,
            "zygosity": zygosity,
            "clinvar": clinvar,
            "cell_line_uuid":cell_line["cell_line_uuid"]}
            }
                          
    cell_line_disease_links.append(row)
    

cell_line_organism_links = []
cell_line_tissue_links = []
cell_line_disease_links = []
cell_line_gene_links = []

cell_line_organism_df, organism_df = get_map_link_df(pd.DataFrame(cell_line_organism_links), organism_df, "organism_id_ncbi", "organism_id_ncbi")
cell_line_disease_df, disease_df = get_map_link_df(pd.DataFrame(cell_line_disease_links), disease_df, "disease_id_ncit", "disease_id_ncit", table1_colstobring=["disease_name_ncbi"])
cell_line_tissue_df, tissue_df = get_map_link_df(pd.DataFrame(cell_line_tissue_links), tissue_df, "tissue_id_uberon", "tissue_id_uberon")
cell_line_gene_df, gene_df = get_map_link_df(pd.DataFrame(cell_line_gene_links), gene_df, "gene_id_hgnc", "gene_id_hgnc")













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
              
cell_line_cols = ["organism","disease_category", "disease_name", "disease_type", "disease_grade", "disease_id_ncit", "tissue_type", "tissue_id_uberon", "cell_type", "cell_id_uberon"]
gene_cols = ["organism_name", "organism_id_ncbi"]

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
  
  
  
 
  # Add in link_generated_by type (API, manual and by who/what time)
    # If link already generated, ignore. If not, look for data and populate
  
  
  
