# %% Import Libraries & Settings
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)   
from bs4 import BeautifulSoup
import pandas as pd
import glob 
import re 
import sys, os
import json
import uuid
import requests
import time
import urllib
#import xmltodict
#import intermine 
#from intermine.webservice import Service

# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')

# Restore
def enablePrint():
    sys.stdout = sys.__stdout__
#----------------------------------------------------------------------------------------------------------------------------------------

# %% get_folder_df()
#_________________________Get All Folders_________________________
def get_folder_df():
  
  # Initialize list to hold DataFrame rows
  row_list = []
  
  # Read in JJ's annotations
  jj_df = pd.read_csv("./research_data_annotation_2023-02-27.tsv", sep="\t")
  
  # Grab all folder names
  folders = [f for f in glob.glob("../../../../mnt/data/research_data/*") if os.path.isdir(f) == True]
  
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

# %% Run get_folder_df()
# Get folder
folder_df, folders = get_folder_df()


# %% get_map_df()
#_________________________Folder <-> Table Links (Text Parsing)_________________________
#_________________________Folders_________________________
def get_map_df(path):

	#____Get all CSV map files____
	df = pd.read_csv(path, dtype=str) # Read in CSV
	df.df_uuid = [col for col in df.columns if "uuid" in col][0] # Grab UUID column
	df.df_name = df.df_uuid.split("_uuid")[0] # Establish DataFrame name
	return(df)
#----------------------------------------------------------------------------------------------------------------------------------------
# %% Run get_map_df()
# Get map DFs
cell_line_df = get_map_df("./maps/cell_line_map.csv")
cell_df = get_map_df("./maps/cell_map.csv")
disease_df = get_map_df("./maps/disease_map.csv")
experiment_df = get_map_df("./maps/experiment_map.csv")
gene_df = get_map_df("./maps/gene_map.csv")
institute_df = get_map_df("./maps/institute_map.csv")
lab_df = get_map_df("./maps/lab_map.csv")
organism_df = get_map_df("./maps/organism_map.csv")
researcher_df = get_map_df("./maps/researcher_map.csv")
seqcompany_df = get_map_df("./maps/seqcompany_map.csv")
study_df = get_map_df("./maps/study_map.csv")
tissue_df = get_map_df("./maps/tissue_map.csv")
treatment_df = get_map_df("./maps/treatment_map.csv")


# %% get_link_df()
#_________________________Folder <-> Table Links (Text Parsing)_________________________
def get_link_df(map_df, folder_searchterms, map_df_uuid):
      
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
        link_row =  {map_df_uuid: map_row[map_df_uuid],
                    "folder_uuid":  folder["folder_uuid"]}
        # Append to row list
        link_rows.append(link_row)
        # Append to set to show that text string was "used"
        folder_searchterms[folder["foldername"]].add(map_row["search_term"]) 
    
  # Convert link_rows to DataFrame
  link_df = pd.DataFrame(link_rows)
  # Append to list of link DataFrames
  return(link_df, folder_searchterms)


# %% Run get_link_df

# Create empty sets to hold which search_terms were used per folder
folder_searchterms = {folder.split("/")[-1]:set() for folder in folders}

# Get map <-> folder
cell_line_folder_df, folder_searchterms = get_link_df(cell_line_df, folder_searchterms, "cell_line_uuid")
cell_folder_df, folder_searchterms = get_link_df(cell_df, folder_searchterms, "cell_uuid")
disease_folder_df, folder_searchterms = get_link_df(disease_df, folder_searchterms, "disease_uuid")
experiment_folder_df, folder_searchterms = get_link_df(experiment_df, folder_searchterms, "experiment_uuid")
gene_folder_df, folder_searchterms = get_link_df(gene_df, folder_searchterms, "gene_uuid")
institute_folder_df, folder_searchterms = get_link_df(institute_df, folder_searchterms, "institute_uuid")
lab_folder_df, folder_searchterms = get_link_df(lab_df, folder_searchterms, "lab_uuid")
organism_folder_df, folder_searchterms = get_link_df(organism_df, folder_searchterms, "organism_uuid")
researcher_folder_df, folder_searchterms = get_link_df(researcher_df, folder_searchterms, "researcher_uuid")
seqcompany_folder_df, folder_searchterms = get_link_df(seqcompany_df, folder_searchterms, "seqcompany_uuid")
study_folder_df, folder_searchterms = get_link_df(study_df, folder_searchterms, "study_uuid")
tissue_folder_df, folder_searchterms = get_link_df(tissue_df, folder_searchterms, "tissue_uuid")
treatment_folder_df, folder_searchterms = get_link_df(treatment_df, folder_searchterms, "treatment_uuid")

# %% get_map_link_df()
#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Folder <-> Folder links_________________________
def get_map_link_df(table1, table2, table1_mergecol, table2_mergecol, 
                    table1_uuid, table2_uuid,
                    table1_colstobring = [], table2_colstobring = [], link_colstobring = []):
  
  table12_links = []
  table2_newrows = []
  
  for _, table1_row in table1[table1[table1_mergecol].notnull()].iterrows():
    
    mask = table2[table2_mergecol] == table1_row[table1_mergecol]
    
    if len(table2[mask]) > 0:
      for _, table2_row in table2[mask].iterrows():
        
        row = {table2_uuid : table2_row[table2_uuid],
               table1_uuid : table1_row[table1_uuid]}
               
        for col in table1_colstobring:
          row[col] = table1_row[col]
          
        for col in table2_colstobring:
          row[col] = table2_row[col]
        
        table12_links.append(row)
      
    else:
      
      if not any(d[table2_mergecol] == table1_row[table1_mergecol] for d in table2_newrows):
        table2_newuuid = str(uuid.uuid4())
        table2_newrow = {table2_uuid: table2_newuuid,
                         table2_mergecol: table1_row[table1_mergecol]}
        
        for col in table1_colstobring:
          table2_newrow[col] = table1_row[col]
          
        table2_newrows.append(table2_newrow)
      
      else:
        table2_newuuid = [d for d in table2_newrows if d[table2_mergecol] == table1_row[table1_mergecol]][0][table2_uuid]
        
      row = {table2_uuid : table2_newuuid,
             table1_uuid : table1_row[table1_uuid]}
      
      for col in table1_colstobring:
          row[col] = table1_row[col]
          
      table12_links.append(row)
    
  table2 = pd.concat([table2,pd.DataFrame(table2_newrows)], ignore_index=True)
  table12_df = pd.DataFrame(table12_links)
  return(table12_df, table2)

# %% Run get_map_link_df
#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Run_________________________
# Get map <-> map (simple)
researcher_lab_df, lab_df = get_map_link_df(researcher_df, lab_df, "lab", "lab_name", "researcher_uuid", "lab_uuid")
researcher_institute_df, institute_df = get_map_link_df(researcher_df, institute_df, "institute", "institute_name", "researcher_uuid", "institute_uuid")
gene_organism_df, organism_df = get_map_link_df(gene_df, organism_df, "organism_id_ncbi", "organism_id_ncbi", "gene_uuid", "organism_uuid")
cell_line_cell_df, cell_df = get_map_link_df(cell_line_df, cell_df, "cell_id_uberon", "cell_id_uberon", "cell_line_uuid", "cell_uuid")

# %% get_clinvar_data()
def get_clinvar_data(row, clinvar):
  #________Use Entrez________
  row["clinvar"] =  clinvar[0] if len(clinvar)>0 else ""
  clinvar_int = str(int(clinvar[0][3:])) 
  url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=clinvar&id=" +  clinvar_int  + "&retmode=json"
  response  = requests.get(url)

  if "result" in response.json().keys():
    
    if len(response.json()["result"][clinvar_int]["variation_set"]) == 1:
      if "variant_type" in response.json()["result"][clinvar_int]["variation_set"][0].keys():
        variant_type = response.json()["result"][clinvar_int]["variation_set"][0]["variant_type"]
        row["variant_type"] = variant_type
    
    if len(response.json()["result"][clinvar_int]["molecular_consequence_list"]) == 1:
      molecular_consequence = response.json()["result"][clinvar_int]["molecular_consequence_list"][0]
      row["molecular_consequence"] = molecular_consequence
    
    elif len(response.json()["result"][clinvar_int]["molecular_consequence_list"]) > 1:
      print(response.json()["result"][clinvar_int]["molecular_consequence_list"])
          
    for desc in ["germline_classification","oncogenicity_classification","clinical_impact_classification"]:
      if desc in response.json()["result"][clinvar_int].keys():
        desc_row = response.json()["result"][clinvar_int][desc]["description"]
        row[desc] = desc_row
    
    #________Web Scrape________
    url = "https://www.ncbi.nlm.nih.gov/clinvar/variation/" + clinvar_int
    response  = requests.get(url)
    
    # Check for Sequence Ontology
    so_sections = re.findall("(<dd>[\s\S]*?Sequence Ontology[\S\s]*?>SO:\d{7})<", str(response.text))
    row["functional_consequence_so"] = []
    row["functional_consequence_so_name"] = []
    
    for so_section in so_sections:
      so_term = re.findall("(SO:\d{7})", so_section)
      so_name = re.findall("<dd>(.*?)(?:\s*?)Sequence Ontology", so_section)
      row["functional_consequence_so"].append(so_term[0])# if len(so_term)>0 else "")
      row["functional_consequence_so_name"].append(so_name[0])# if len(so_name)>0 else "")
    
    # Check for Variation Ontology
    vario_sections = re.findall("(<dd>[\s\S]*?Variation Ontology[\S\s]*?>VariO:\d{4})<", str(response.text))
    row["functional_consequence_vario"] = []
    row["functional_consequence_vario_name"] = []
    
    for vario_section in vario_sections:
      vario_term = re.findall("(VariO:\d{4})", vario_section)
      vario_name = re.findall("<dd>(.*?)(?:\s*?)Variation Ontology", vario_section)
      row["functional_consequence_vario"].append(vario_term[0])
      row["functional_consequence_vario_name"].append(vario_name[0])
        
    return row

  


# %% Get Cell Line links
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
  organism_name = re.findall("\nOX\s*NCBI_TaxID=.*;\s*!\s*(.*?)\n", response.text)
  organism_id_ncbi = re.findall("\nOX\s*NCBI_TaxID=(.*?);", response.text)
  
  for organism in organism_id_ncbi:
    row = {"organism_id_ncbi":organism, 
           "organism_name":organism_name[0] if len(organism_name)>0 else "", 
           "cell_line_uuid":cell_line["cell_line_uuid"]}
    cell_line_organism_links.append(row)
    
  # Links to tissue
  tissue_name = re.findall("CC\s*Derived from site:.*;\s*(.*?);",response.text)
  tissue_id_uberon = re.findall("CC\s*Derived from site:.*UBERON=(.*?).\n",response.text)
  
  for tissue in tissue_id_uberon:
    row = {"tissue_id_uberon":tissue, 
           "tissue_name":tissue_name[0] if len(tissue_name)>0 else "", 
           "cell_line_uuid":cell_line["cell_line_uuid"]}
    cell_line_tissue_links.append(row)
    

  # Links to disease
  disease_name = re.findall("\nDI\s*NCIt;.*;\s*(.*?)\n", response.text)
  disease_id_ncit = re.findall("\nDI\s*NCIt;\s*(.*?);", response.text)
  for disease in disease_id_ncit:
    row = {"disease_id_ncit":disease, 
           "disease_name":disease_name[0] if len(disease_name)>0 else "", 
           "cell_line_uuid":cell_line["cell_line_uuid"]}
    cell_line_disease_links.append(row)
    
  # Links to gene
  sequence_variation_matches = [s for s in response.text.split("\n") if "Sequence variation:" in s]
  
  for match in sequence_variation_matches:
    var_type = re.findall("CC\s*Sequence variation:\s*(.*?);", match)
    gene_id_hgnc = re.findall("HGNC;\s*(.*?)[;\s]", match)
    gene_symbol = re.findall("HGNC;(?:[^;]*;)\s*(.*?)[;\s]", match)
    zygosity = re.findall(".*Zygosity=(.{2,}?)[;\s]", match)
    clinvar = re.findall(".*ClinVar=(.*?);", match)
    
    row =   {
            "var_type":var_type[0] if len(var_type)>0 else "",
            "gene_id_hgnc": gene_id_hgnc[0] if len(gene_id_hgnc)>0 else "",
            "gene_symbol": gene_symbol[0] if len(gene_symbol)>0 else "",
            "zygosity": zygosity[0] if len(zygosity)>0 else "",
            "clinvar": clinvar[0] if len(clinvar)>0 else "",
            "cell_line_uuid":cell_line["cell_line_uuid"]
            }
            
    if len(clinvar)>0:
      row = get_clinvar_data(row, clinvar)
      
    cell_line_gene_links.append(row)
    time.sleep(1) 
    
# %% (Archive) Get Strain Data 
#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Run_________________________
# Adding in Strain data
"""
for index, cell_line in cell_line_df[cell_line_df["mgi_id"].notnull()].iterrows():

  # Add organism link?
  if not any(d["cell_line_uuid"] == cell_line["cell_line_uuid"] for d in cell_line_organism_links):
    
    row = {"cell_line_uuid" : cell_line["cell_line_uuid"],
           "organism_id_ncbi": "1758"}
           
    cell_line_organism_links.append(row)
  
  blockPrint()
  service = Service("https://www.mousemine.org/mousemine/service")
  query=service.new_query("Strain")
  query.select("*")
  query.add_constraint("primaryIdentifier","=",cell_line["mgi_id"])
  for row in query.rows():
    cell_line_df.loc[index]["cell_line_name"] = row["name"] if row["name"]!="None" else ""
    cell_line_df.loc[index]["category"] = row["attributeString"] if row["attributeString"]!="None" else ""
    cell_line_df.loc[index]["cell_line_symbol"] = row["symbol"] if row["symbol"]!="None" else ""
    cell_line_df.loc[index]["description"] = row["description"] if row["description"]!="None" else ""
  enablePrint()
"""

# %% Run get_map_link_df
#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Run_________________________
cell_line_organism_df, organism_df = get_map_link_df(pd.DataFrame(cell_line_organism_links), organism_df, 
                                                     "organism_id_ncbi", "organism_id_ncbi", 
                                                     table1_uuid = "cell_line_uuid", table2_uuid = "organism_uuid",
                                                     table1_colstobring=["organism_name"])
                                                     
cell_line_disease_df, disease_df = get_map_link_df(pd.DataFrame(cell_line_disease_links), disease_df, 
                                                   "disease_id_ncit", "disease_id_ncit",
                                                   table1_uuid = "cell_line_uuid", table2_uuid = "disease_uuid",
                                                   table1_colstobring=["disease_name"])
                                                   
cell_line_tissue_df, tissue_df = get_map_link_df(pd.DataFrame(cell_line_tissue_links), tissue_df, 
                                                "tissue_id_uberon", "tissue_id_uberon",
                                                table1_uuid = "cell_line_uuid", table2_uuid = "tissue_uuid",
                                                table1_colstobring=["tissue_name"])
                                                
cell_line_gene_df, gene_df = get_map_link_df(pd.DataFrame(cell_line_gene_links), gene_df, 
                                             "gene_id_hgnc", "gene_id_hgnc", 
                                             table1_uuid = "cell_line_uuid", table2_uuid = "gene_uuid",
                                             table1_colstobring=["molecular_consequence","variant_type","gene_symbol","var_type","zygosity","clinvar","gene_symbol","gene_id_hgnc",
                                                                 "germline_classification","oncogenicity_classification","clinical_impact_classification",
                                                                 "functional_consequence_so","functional_consequence_so_name","functional_consequence_vario","functional_consequence_vario_name"])



# %% Get Treatment data
#----------------------------------------------------------------------------------------------------------------------------------------
# Adding in Treatment data
for index, treatment in treatment_df[treatment_df["treatment_id_pubchem"].notnull()].iterrows():

  pubchem_id = str(treatment["treatment_id_pubchem"])
  url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pccompound&id=" +  pubchem_id  + "&retmode=json"
  response  = requests.get(url)
  
  #if "result" in response.json().keys():
  #      print(response.json()["result"][pubchem_id])
    
  time.sleep(1) 
    
  
  
  
  
  
  
# %% Modify and Print Tables to CSV
#----------------------------------------------------------------------------------------------------------------------------------------
##_________________________Remove Linker columns from DataFrames________________________
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
          #disease_gene_df,
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
              #"disease_gene_df",
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

#cell_line_gene_df = cell_line_gene_df[[col for col in cell_line_gene_df.columns if col not in cell_line_gene_cols]]
cell_line_df = cell_line_df[[col for col in cell_line_df.columns if col not in cell_line_cols]]
gene_df = gene_df[[col for col in gene_df.columns if col not in gene_cols]]

for i in range(0,len(tables)):
  tables[i] = tables[i][[col for col in tables[i].columns if col != "search_term"]]
  path = "./tables/" + tables_str[i] + ".csv"
  tables[i].to_csv(path, index=False)
  print(tables_str[i], ": ", str(tables[i].shape))
  
  
  
# %% Print DBML Script File
#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Print DBML for diagram_________________________
with open("./scripts/DBML_script.txt", "w") as text_file:
    
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
    
  



# %% Print Merge Script File
#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Merge all togehther_________________________
with open("./scripts/merge_script.txt", "w") as text_file:
   
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
        
