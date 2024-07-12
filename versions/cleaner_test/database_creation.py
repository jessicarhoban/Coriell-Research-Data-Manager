
import duckdb
import pandas as pd
import requests 
import numpy as np
import warnings
warnings.filterwarnings('ignore')



con = duckdb.connect("browser_cleaner.db")

import re






#df = pd.DataFrame(columns = ["map_ex"])
#con.execute("CREATE table maps as select * from df")
#con.sql("select * from maps")
#con.execute("ALTER TABLE maps ADD COLUMN map_ex3 MAP(VARCHAR, VARCHAR)")
#con.execute("insert into maps (map_ex2) values (MAP {'cc5299a5-93ed-4616-85ee-bd7a6bdfc644': 'two'})")







#url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=knockout&rows=500000"
#response  = requests.get(url)
#knockout_opts =  set([t.split("Method=")[1].split(";")[0].strip() for t in response.text.split("\n")  if "Method" in t])

#url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=knockout&rows=500000"
#response2  = requests.get(url2)
#knockout_opts2 =  set([t.split("Method=")[1].split(";")[0].strip() for t in response2.text.split("\n") if "Method" in t])

#knockout_opts.update(knockout_opts2)
#knockout_df = pd.DataFrame(knockout_opts, columns=["knockout_name"])




#%% ______________________POPULATE VALUES TABLES___________________________





#-----------------------
# %% CELL OPTIONS -- 223
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=cell&rows=400000"
response  = requests.get(url)
cell_opts =  set([t.strip() for t in response.text.split("\n") if "Cell type:" in t]) #.split(":")[1].split(";")[0]

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=cell&rows=400000"
response2  = requests.get(url2)
cell_opts2 =  set([t.strip() for t in response2.text.split("\n") if "Cell type:" in t])

cell_opts.update(cell_opts2)
cell_df = pd.DataFrame(cell_opts, columns=["cell_text"])
cell_df["cell_name"] = cell_df["cell_text"].apply(lambda x: x.split(":")[1].split(";")[0])
cell_df["cell_id_CL"] = cell_df["cell_text"].apply(lambda x: x.split("CL=")[-1].replace(".","") if "CL" in x else "")
cell_df = cell_df[["cell_name", "cell_id_CL"]]

#-----------------------
# %% TISSUE OPTIONS -- 882
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=site&rows=500000"
response  = requests.get(url)
tissue_opts =  set([t.strip() for t in response.text.split("\n") if "Derived from site:" in t])

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=site&rows=500000"
response2  = requests.get(url2)
tissue_opts2 =  set([t.strip() for t in response2.text.split("\n") if "Derived from site:" in t])

tissue_opts.update(tissue_opts2)
tissue_df = pd.DataFrame(tissue_opts, columns=["tissue_text"])
tissue_df["tissue_name"] = tissue_df["tissue_text"].apply(lambda x: x.split(";")[1].replace(".",""))
tissue_df["tissue_id_UBERON"] = tissue_df["tissue_text"].apply(lambda x: x.split("UBERON=")[-1].replace(".","") 
                                                           if "UBERON" in x else "")
tissue_df = tissue_df[["tissue_name", "tissue_id_UBERON"]]


#-----------------------
# %% DISEASE OPTIONS -- 3055
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=di&rows=500000"
response  = requests.get(url)
disease_opts =  set([t.strip() for t in response.text.split("\n") if t.startswith("DI")])

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=di&rows=500000"
response2  = requests.get(url2)
disease_opts2 =  set([t.strip() for t in response2.text.split("\n") if t.startswith("DI")])

disease_opts.update(disease_opts2)

disease_df = pd.DataFrame(disease_opts, columns=["disease_text"])
disease_df["disease_name"] = disease_df["disease_text"].apply(lambda x: x.split(";")[-1].replace(".",""))
disease_df["disease_id_NCIt"] = disease_df["disease_text"].apply(lambda x: x.split("NCIt;")[1].split(";")[0].strip() 
                                                           if "NCIt" in x else "")
disease_df["disease_id_ORDO"] = disease_df["disease_text"].apply(lambda x: x.split("ORDO;")[1].split(";")[0].strip() 
                                                           if "ORDO" in x else "")
disease_df = disease_df[["disease_name", "disease_id_NCIt", "disease_id_ORDO"]]

disease_df = disease_df.replace(r'^\s*$', np.nan, regex=True)
disease_df["disease_id_NCIt"] = disease_df.groupby(["disease_name"])["disease_id_NCIt"].ffill()
disease_df["disease_id_NCIt"] = disease_df.groupby(["disease_name"])["disease_id_NCIt"].bfill()
disease_df["disease_id_ORDO"] = disease_df.groupby(["disease_name"])["disease_id_ORDO"].ffill()
disease_df["disease_id_ORDO"] = disease_df.groupby(["disease_name"])["disease_id_ORDO"].bfill()
disease_df = disease_df.drop_duplicates()


#-----------------------




# %% GENE OPTIONS -- 1271
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=sequence-variation&rows=500000"
response  = requests.get(url)
gene_opts =  set([t.strip() for t in response.text.split("\n") if t.startswith("CC") and "Gene fusion" not in t])

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=sequence-variation&rows=500000"
response2  = requests.get(url2)
gene_opts2 =  set([t.strip() for t in response2.text.split("\n") if t.startswith("CC") and "Gene fusion" not in t])

import re 
gene_opts.update(gene_opts2)
gene_df = pd.DataFrame(gene_opts, columns=["gene_text"])
gene_df["gene_name"] = gene_df["gene_text"].apply(lambda x: x.split(";")[3].strip())
gene_df["gene_id_HGNC"] = gene_df["gene_text"].apply(lambda x: re.findall("HGNC;\s*(.*?)[;\s]", x)[0]
                                                     if re.search("HGNC;\s*(.*?)[;\s]", x) else "")

gene_df["gene_symbol_HGNC"] = gene_df["gene_text"].apply(lambda x: re.findall("HGNC;(?:[^;]*;)\s*(.*?)[;\s]", x)[0]
                                                         if re.search("HGNC;(?:[^;]*;)\s*(.*?)[;\s]", x) else "")

gene_df["gene_id_MGI"] = gene_df["gene_text"].apply(lambda x: re.findall("MGI;\s*(.*?)[;\s]", x)[0]
                                                    if re.search("MGI;\s*(.*?)[;\s]", x) else "")

gene_df["gene_symbol_MGI"] = gene_df["gene_text"].apply(lambda x: re.findall("MGI;(?:[^;]*;)\s*(.*?)[;\s]", x)[0]
                                                    if re.search("MGI;(?:[^;]*;)\s*(.*?)[;\s]", x) else "")

gene_df["gene_id_VGNC"] = gene_df["gene_text"].apply(lambda x: re.findall("VGNC;\s*(.*?)[;\s]", x)[0]
                                                     if re.search("VGNC;\s*(.*?)[;\s]", x) else "")

gene_df["gene_symbol_VGNC"] = gene_df["gene_text"].apply(lambda x: re.findall("VGNC;(?:[^;]*;)\s*(.*?)[;\s]", x)[0]
                                                     if re.search("VGNC;(?:[^;]*;)\s*(.*?)[;\s]", x) else "")
gene_df = gene_df.drop(["gene_text"], axis=1)
gene_df["gene_id_NCBI"] = ""
gene_df["gene_symbol_NCBI"] = ""
gene_df = gene_df.drop_duplicates().reset_index()
gene_df = gene_df.loc[gene_df["gene_id_HGNC"] != "Group_1972"]

for index, gene in gene_df.iterrows():
    if len(gene["gene_id_HGNC"])>0:
        url = "https://rest.genenames.org/fetch/hgnc_id/" +  str(gene["gene_id_HGNC"])
        response  = requests.get(url)
        gene_id_ncbi = re.findall('.*<str name="entrez_id">(.*?)</str>.*', response.text)
        gene_name = re.findall('.*<str name="name">(.*?)</str>.*', response.text)
        gene_symbol = re.findall('.*<str name="symbol">(.*?)</str>.*', response.text)
        gene_df.loc[index, "gene_name"] = gene_name[0] if len(gene_name)>0 else ""
        gene_df.loc[index, "gene_id_NCBI"] = gene_id_ncbi[0] if len(gene_id_ncbi)>0 else ""
        gene_df.loc[index, "gene_symbol_NCBI"] = gene_symbol[0] if len(gene_symbol)>0 else ""
    
    #print(index)

gene_df = gene_df.drop(["index"], axis=1)




# %% BREED OPTIONS -- 507 (TEXT?)
url = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=breed&rows=500000"
response  = requests.get(url)
breed_opts =  set([t.split("Breed/subspecies:")[1].strip()[:-1] for t in response.text.split("\n") if "Breed/subspecies" in t])

breed_df = pd.DataFrame(breed_opts, columns=["breed_name"])

import intermine 



from intermine.webservice import Service

service = Service("https://www.mousemine.org/mousemine/service")
query=service.new_query("Strain")


query.select("*")
query.add_constraint("primaryIdentifier","=","2160625")
for row in query.rows():
    print(row)




# %% CELL LINE OPTIONS -- ??

# 
query = "%28dr%3AATCC%2A%20OR%20dr%3ACoriell%2A%29%20AND%20ox%3Asapiens"

fields = "%2C".join(["id,ac,cell,site,di,ca,sx,ag,breed,cc,dr"])
url = "https://api.cellosaurus.org/search/cell-line?q="+query + "&format=txt&fields="+fields+"&rows=500000"
response  = requests.get(url)

cell_line_df = pd.DataFrame()

for index, line in enumerate(response.text.split(" 45090\n")[1].split("//")):
    print(index)
    
    #____ Cell Line specific fields ____
    cell_line_ac = re.findall("\nAC\s*(.*?)\n", line)
    cell_line_name = re.findall("\nID\s*(.*?)\n", line)
    coriell_id = re.findall("\nDR\s*Coriell;\s(.*?)\n",line)
    atcc_id = re.findall("\nDR\s*ATCC;\s(.*?)\n",line)  
    age = re.findall("\nAG\s*(.*?)\n", line)
    sex = re.findall("\nSX\s*(.*?)\n", line)
    population = re.findall("\nCC\s*Population:\s*(.*?)[;\.]", line)
    cell_line_category = re.findall("\nCA\s*(.*?)\n", line)  
    transformant_id_ncbi = re.findall("\nCC\s*Transformant: NCBI_TaxID;\s*(.*?);", line)
    transformant_name_ncbi = re.findall("\nCC\s*Transformant: NCBI_TaxID;.*;\s*(.*?)\n",line)
    breed = re.findall("\nCC\s*Breed/subspecies:\s*(.*?)[;\.]", line)
    cell = re.findall("\nCC\s*Cell type:\s*(.*?)[;\.]", line)
    tissue = re.findall("\n.*?UBERON=\s*(.*?)[;\.]", line)
    disease = re.findall("\nDI\s*(.*?)\n", line)
    seqvar = re.findall("\nCC\s*Sequence variation:(.*?)\n", line)
    knockout = re.findall("\nCC\s*Knockout cell: Method=(.*?)\n", line)

    cell_line_df.loc[index,"cell_line_ac"] = cell_line_ac[0] if len(cell_line_ac)>0 else ""
    cell_line_df.loc[index,"cell_line_name"] = cell_line_name[0] if len(cell_line_name)>0 else ""
    cell_line_df.loc[index,"coriell_id"] = coriell_id[0] if len(coriell_id)>0 else ""
    cell_line_df.loc[index,"atcc_id"] = atcc_id[0] if len(atcc_id)>0 else ""
    cell_line_df.loc[index,"age"] = age[0] if len(age)>0 else ""
    cell_line_df.loc[index,"sex"] = sex[0] if len(sex)>0 else ""
    cell_line_df.loc[index,"population"] = population[0] if len(population)>0 else ""
    cell_line_df.loc[index,"cell_line_category"] = cell_line_category[0] if len(cell_line_category)>0 else ""
    cell_line_df.loc[index,"transformant_id_ncbi"] = transformant_id_ncbi[0] if len(transformant_id_ncbi)>0 else ""
    cell_line_df.loc[index,"transformant_name_ncbi"] = transformant_name_ncbi[0] if len(transformant_name_ncbi)>0 else ""
    cell_line_df.loc[index,"breed"] = breed[0] if len(breed)>0 else ""
    cell_line_df.loc[index,"cell"] = cell[0] if len(cell)>0 else ""
    cell_line_df.loc[index,"tissue"] = tissue[0] if len(tissue)>0 else ""
    cell_line_df.loc[index,"disease"] = ",".join(disease) if len(disease)>0 else ""
    cell_line_df.loc[index,"seqvar"] = ",".join(seqvar) if len(seqvar)>0 else ""
    cell_line_df.loc[index,"knockout"] = ",".join(knockout) if len(knockout)>0 else ""



   



cell_line_df["sex"].value_counts()





cellline_opts =  set([t.strip().replace("AC   ","") for t in response.text.split("\n") if t.startswith("AC")])







query1 = "ca%3ACancer%2A%20AND%20ox%3Asapiens" 
url1 = "https://api.cellosaurus.org/search/cell-line?q=" + query1 + "&format=json&fields="+fields+"&rows=500000"
response1  = requests.get(url1)
cellline_opts1 =  set([t.strip().replace("AC   ","") for t in response1.text.split("\n") if t.startswith("AC")])












#query = "%28dr%3AATCC%2A%20OR%20dr%3ACoriell%2A%20OR%20dr%3Acancercelllines%2A%29%20AND%20ox%3Asapiens"
url = "https://api.cellosaurus.org/search/cell-line?q=" + query + "&format=txt&fields=ac%2Cdr&rows=500000"


#url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=ac%2Cdr&rows=500000"
response1  = requests.get(url)
cellline_opts1 =  set([t.strip().replace("AC   ","")  for t in response1.text.split("\n") if t.startswith("AC")])








query2 = "%28dr%3AATCC%2A%20OR%20dr%3ACoriell%2A%20OR%20dr%3Acancercelllines%2A%20OR%20ca%3Acancer%20cell%20line%29%20AND%20ox%3Amusculus"

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=ac%2Cdr&rows=500000"
response2  = requests.get(url2)
cellline_opts2 =  set([t.strip().replace("AC   ","") for t in response2.text.split("\n") if t.startswith("AC")])

cellline_opts.update(cellline_opts2)
cell_line_df = pd.DataFrame(cellline_opts, columns=["cellline_ac"])




for index,cellline in cell_line_df.iterrows():#.loc[cell_line_df["cellline_ac"].isin(filtered)]
    

    print(index)

    fields = "%2C".join(["id,ac,cell,site,di,ca,sx,ag,breed,cc,dr"])
    url = "https://api.cellosaurus.org/cell-line/" + cellline["cellline_ac"] + "?format=txt&fields=" + fields
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

    breed = re.findall("\nCC\s*Breed/subspecies:\s*(.*?)[;\.]", response.text)
    cell = re.findall("\nCC\s*Cell type:\s*(.*?)[;\.]", response.text)
    tissue = re.findall("\n.*?UBERON=\s*(.*?)[;\.]", response.text)

    disease = re.findall("\nDI\s*(.*?)\n", response.text)
    seqvar = re.findall("\nCC\s*Sequence variation:(.*?)\n", response.text)
    knockout = re.findall("\nCC\s*Knockout cell: Method=(.*?)\n", response.text)

    cell_line_df.loc[index,"cell_line_name"] = cell_line_name[0] if len(cell_line_name)>0 else ""
    cell_line_df.loc[index,"coriell_id"] = coriell_id[0] if len(coriell_id)>0 else ""
    cell_line_df.loc[index,"atcc_id"] = atcc_id[0] if len(atcc_id)>0 else ""
    cell_line_df.loc[index,"age"] = age[0] if len(age)>0 else ""
    cell_line_df.loc[index,"sex"] = sex[0] if len(sex)>0 else ""
    cell_line_df.loc[index,"population"] = population[0] if len(population)>0 else ""
    cell_line_df.loc[index,"cell_line_category"] = cell_line_category[0] if len(cell_line_category)>0 else ""
    cell_line_df.loc[index,"transformant_id_ncbi"] = transformant_id_ncbi[0] if len(transformant_id_ncbi)>0 else ""
    cell_line_df.loc[index,"transformant_name_ncbi"] = transformant_name_ncbi[0] if len(transformant_name_ncbi)>0 else ""

    cell_line_df.loc[index,"breed"] = breed[0] if len(breed)>0 else ""
    cell_line_df.loc[index,"cell"] = cell[0] if len(cell)>0 else ""
    cell_line_df.loc[index,"tissue"] = tissue[0] if len(tissue)>0 else ""

    cell_line_df.loc[index,"disease"] = ",".join(disease) if len(disease)>0 else ""
    cell_line_df.loc[index,"seqvar"] = ",".join(seqvar) if len(seqvar)>0 else ""
    cell_line_df.loc[index,"knockout"] = ",".join(knockout) if len(knockout)>0 else ""



   
    
  


  


# %%
#______________________CREATE VALUES TABLES___________________________

#  %% INITIALIZE TABLES
tables = [
           {"table_name":    "cell",
           "cols":          {"cell_name":"TEXT", "cell_id_CL":"TEXT"},
           "primary_key":   "cell__uuid", 
           "values":        "cell_df"},
           
           {"table_name":    "tissue",
           "cols":          {"tissue_name":"TEXT", "tissue_id_UBERON":"TEXT"},
           "primary_key":   "tissue__uuid", 
           "values":        "tissue_df"},

           {"table_name":    "disease",
           "cols":          {"disease_name":"TEXT", "disease_id_NCIt":"TEXT", "disease_id_ORDO":"TEXT"},
           "primary_key":   "disease__uuid", 
           "values":        "disease_df"},

           {"table_name":    "gene",
           "cols":          {"gene_name":"TEXT", 
                             "gene_id_HGNC":"TEXT", "gene_symbol_HGNC":"TEXT", 
                             "gene_id_MGI":"TEXT", "gene_symbol_MGI":"TEXT", 
                             "gene_id_VGNC":"TEXT", "gene_symbol_VGNC":"TEXT",
                             "gene_id_NCBI":"TEXT", "gene_symbol_NCBI":"TEXT"}, 
           "primary_key":   "gene__uuid", 
           "values":        "gene_df"},

           {"table_name":    "breed",
           "cols":          {"breed_name":"TEXT"},
           "primary_key":   "breed_uuid", 
           "values":        "breed_df"}
           ]


#  %% EXECUTE TABLES
for table in tables:
    commands1 = ["DROP TABLE IF EXISTS ", table["table_name"]]
    commands1 = "".join(commands1)
    con.execute(commands1)

    commands = ["CREATE TABLE ", table["table_name"], " ( ", 
                table["primary_key"], " UUID NOT NULL DEFAULT uuid(), ",
                " PRIMARY KEY (", table["primary_key"], "))"]
    commands = "".join(commands)
    con.execute(commands)

    for col_name, col_type in table["cols"].items():
        col_command = ["ALTER TABLE ", table["table_name"], " add column ", col_name, " ", col_type]
        col_command = "".join(col_command)
        con.execute(col_command)
    
    insert_commands = ["INSERT INTO ", table["table_name"], "(" , ",".join([k for k in table["cols"].keys()])  ,
                       ") SELECT * FROM ", table["values"]]
    insert_commands = "".join(insert_commands)
    con.execute(insert_commands)
    
    

# %% SHOW TABLES
con.sql("Select * from cell")
con.sql("Select * from tissue")
con.sql("Select * from disease")
con.sql("Select * from gene")
con.sql("Select * from breed")


#-------------------------------------------
con.execute("drop table sample")
con.execute("CREATE TABLE sample (sample_uuid UUID NOT NULL DEFAULT uuid(), PRIMARY KEY (sample_uuid))")

con.execute("ALTER TABLE sample ADD COLUMN title TEXT") # sample_name
con.execute("ALTER TABLE sample ADD COLUMN folder_uuid UUID")
con.execute("ALTER TABLE sample ADD COLUMN number_files TEXT")

sample_df = pd.read_csv("./database/database_creation/sample.csv")

con.execute( """INSERT INTO sample (title, 
                                    number_files,  
                                    sample_uuid, 
                                    folder_uuid)
                SELECT * FROM sample_df """)

# Free text
con.execute("ALTER TABLE sample ADD COLUMN library_name TEXT") # sample_id
con.execute("ALTER TABLE sample ADD COLUMN description TEXT")

# Options
con.execute("ALTER TABLE sample ADD COLUMN organism TEXT")
con.execute("ALTER TABLE sample ADD COLUMN sex TEXT")
con.execute("ALTER TABLE sample ADD COLUMN molecule TEXT")
con.execute("ALTER TABLE sample ADD COLUMN singlepaired TEXT")
con.execute("ALTER TABLE sample ADD COLUMN instrument_model TEXT")

con.execute("ALTER TABLE sample ADD COLUMN cell_uuid UUID")
con.execute("ALTER TABLE sample ADD COLUMN tissue_uuid UUID")



#con.execute("ALTER TABLE sample ADD COLUMN cellline_category UUID")
#con.execute("ALTER TABLE sample ADD COLUMN breed_uuid UUID")
#"ALTER TABLE sample ADD COLUMN gene_uuids LIST"
#"ALTER TABLE sample ADD COLUMN knockout_uuid UUID"
#"ALTER TABLE sample ADD COLUMN disease_uuids LIST"



#------------------------------------------- Create from files ------------------------------------------
con.execute("drop table folder")
con.execute("CREATE TABLE folder (folder_uuid UUID NOT NULL DEFAULT uuid(), PRIMARY KEY (folder_uuid))")

con.execute("ALTER TABLE folder ADD COLUMN date DATE")
con.execute("ALTER TABLE folder ADD COLUMN folderpath TEXT")
con.execute("ALTER TABLE folder ADD COLUMN foldername TEXT")
con.execute("ALTER TABLE folder ADD COLUMN jj_description TEXT")
con.execute("ALTER TABLE folder ADD COLUMN jj_type TEXT")
con.execute("ALTER TABLE folder ADD COLUMN server TEXT")
con.execute("ALTER TABLE folder ADD COLUMN is_root INT")

folder_df = pd.read_csv("./database/database_creation/folder.csv")
folder_df["date"] = pd.to_datetime(folder_df["date"])

con.execute( """INSERT INTO folder (date, 
                                    folderpath,  
                                    foldername, 
                                    jj_description, 
                                    jj_type, 
                                    server, 
                                    folder_uuid,
                                    is_root) 
                SELECT * FROM folder_df """)

con.execute("ALTER TABLE folder ADD COLUMN title TEXT")
con.execute("ALTER TABLE folder ADD COLUMN summary TEXT")
con.execute("ALTER TABLE folder ADD COLUMN experimental_design TEXT")
con.execute("ALTER TABLE folder ADD COLUMN growth_protocol TEXT")
con.execute("ALTER TABLE folder ADD COLUMN treatment_protocol TEXT")
con.execute("ALTER TABLE folder ADD COLUMN extract_protocol TEXT")
con.execute("ALTER TABLE folder ADD COLUMN library_construction_protocol TEXT")
con.execute("ALTER TABLE folder ADD COLUMN data_processing_step TEXT")
con.execute("ALTER TABLE folder ADD COLUMN processed_files_format TEXT")

con.execute("ALTER TABLE folder ADD COLUMN library_strategy TEXT") # UUID?
con.execute("ALTER TABLE folder ADD COLUMN genome_build TEXT") # UUID?




#-------------------------------------------
con.execute("drop table researcher")
con.execute("CREATE TABLE researcher (researcher_uuid UUID NOT NULL DEFAULT uuid(), PRIMARY KEY (researcher_uuid))")
con.execute("ALTER TABLE researcher ADD COLUMN researcher_name TEXT")
con.execute("ALTER TABLE researcher ADD COLUMN geo_name TEXT")
con.execute("ALTER TABLE researcher ADD COLUMN institute TEXT")
con.execute("ALTER TABLE researcher ADD COLUMN lab TEXT")

researcher_df = pd.read_csv("./database/database_creation/researcher.csv")
con.execute( """INSERT INTO researcher (researcher_name, geo_name, institute, lab, researcher_uuid) 
                SELECT * FROM researcher_df """)


#-------------------------------------------
con.execute("drop table parameter")
con.execute("CREATE TABLE parameter (parameter_uuid UUID NOT NULL DEFAULT uuid(), PRIMARY KEY (parameter_uuid))")
con.execute("ALTER TABLE parameter ADD COLUMN parameter_name TEXT")
con.execute("ALTER TABLE parameter ADD COLUMN parameter_type TEXT")

parameter_df = pd.read_csv("./database/database_creation/parameters.csv")
con.execute( """INSERT INTO parameter (parameter_name, parameter_type, parameter_uuid) 
                SELECT * FROM parameter_df """)

#-------------------------------------------
con.execute("drop table user")
con.execute("CREATE TABLE user (user_uuid UUID NOT NULL DEFAULT uuid(), PRIMARY KEY (user_uuid))")
con.execute("ALTER TABLE user ADD COLUMN user_username TEXT")
con.execute("ALTER TABLE user ADD COLUMN user_name TEXT")
con.execute("ALTER TABLE user ADD COLUMN user_role TEXT")
con.execute("ALTER TABLE user ADD COLUMN researcher_uuid TEXT")

user_df = pd.read_csv("./database/database_creation/user.csv")
con.execute( """INSERT INTO user (user_username, user_name, user_role, researcher_uuid) 
                SELECT * FROM user_df """)



#-------------------------------------------
con.execute("drop table researcher_folder")
researcher_folder_df = pd.read_csv("./database/database_creation/researcher_folder.csv")
con.execute("CREATE TABLE researcher_folder (researcher_uuid UUID, folder_uuid UUID)")
con.execute("INSERT INTO researcher_folder (researcher_uuid, folder_uuid) select * from researcher_folder_df ")

#-------------------------------------------
con.execute("drop table parameter_folder")
#parameter_folder_df = pd.read_csv("./database/database_creation/researcher_folder.csv")
con.execute("CREATE TABLE parameter_folder (parameter_uuid UUID, folder_uuid UUID)")
#con.execute("INSERT INTO researcher_folder (researcher_uuid, folder_uuid) select * from researcher_folder_df ")




#-------------------------------------------
con.execute("drop table cell_line")
con.execute("CREATE TABLE cell_line (cell_line_uuid UUID NOT NULL DEFAULT uuid(), PRIMARY KEY (cell_line_uuid))")

con.execute("ALTER TABLE cell_line ADD COLUMN  cell_line_ac  TEXT")
con.execute("ALTER TABLE cell_line ADD COLUMN  cell_line_name  TEXT")
con.execute("ALTER TABLE cell_line ADD COLUMN  coriell_id  TEXT")
con.execute("ALTER TABLE cell_line ADD COLUMN  atcc_id  TEXT")
con.execute("ALTER TABLE cell_line ADD COLUMN  age  TEXT")
con.execute("ALTER TABLE cell_line ADD COLUMN  sex  TEXT")
con.execute("ALTER TABLE cell_line ADD COLUMN  population  TEXT")
con.execute("ALTER TABLE cell_line ADD COLUMN  cell_line_category  TEXT")
con.execute("ALTER TABLE cell_line ADD COLUMN  transformant_id_ncbi  TEXT")
con.execute("ALTER TABLE cell_line ADD COLUMN  transformant_name_ncbi  TEXT")
con.execute("ALTER TABLE cell_line ADD COLUMN  breed  TEXT")
con.execute("ALTER TABLE cell_line ADD COLUMN  cell  TEXT")
con.execute("ALTER TABLE cell_line ADD COLUMN  tissue  TEXT")
con.execute("ALTER TABLE cell_line ADD COLUMN  disease  TEXT")
con.execute("ALTER TABLE cell_line ADD COLUMN  seqvar  TEXT")
con.execute("ALTER TABLE cell_line ADD COLUMN  knockout  TEXT")

cellline_df = pd.read_csv("./database/cell_line_upload.csv")

con.execute( """INSERT INTO cell_line (cell_line_ac ,
                                    cell_line_name ,
                                    coriell_id ,
                                    atcc_id ,
                                    age ,
                                    sex ,
                                    population ,
                                    cell_line_category ,
                                    transformant_id_ncbi ,
                                    transformant_name_ncbi ,
                                    breed ,
                                    cell ,
                                    tissue ,
                                    disease ,
                                    seqvar ,
                                    knockout)
                SELECT * FROM cellline_df """)




con.execute("ALTER TABLE cell RENAME cell__uuid TO cell_uuid;")
con.execute("ALTER TABLE tissue RENAME tissue__uuid TO tissue_uuid;")



#-------------------------------------------
con.execute("ALTER TABLE biomaterial_folder ADD COLUMN cell_line_name  TEXT")


# biomaterial_folder --- add options for samples to folder
# biomaterial_sample --- connect options to samples


# delete biomaterialgroup_biomaterial and biomaterialgroup_folder?
biomaterialgroup_sample	

biomaterialgroup_uuid



con.execute("ALTER TABLE biomaterialgroup_sample RENAME biomaterialgroup_name to biomaterial_name ")


#-------------------------------------------
con.execute("drop table gene_folder")
#gene_folder_df = pd.read_csv("./database/database_creation/researcher_folder.csv")
con.execute("CREATE TABLE gene_folder (gene_uuid UUID, folder_uuid UUID)")



con.execute("ALTER TABLE gene RENAME gene__uuid to gene_uuid")



con.execute("ALTER TABLE disease RENAME disease__uuid to disease_uuid")

con.execute("drop table disease_folder")
con.execute("CREATE TABLE disease_folder (disease_uuid UUID, folder_uuid UUID)")


con.execute("drop table cell_line_folder")
con.execute("CREATE TABLE cell_line_folder (cell_line_uuid UUID, folder_uuid UUID)")
con.execute("CREATE TABLE cell_line_sample (cell_line_uuid UUID, sample_uuid UUID)")


con.execute("drop table disease_sample")
con.execute("CREATE TABLE disease_sample (disease_uuid UUID, sample_uuid UUID)")

con.execute("drop table gene_sample")
con.execute("CREATE TABLE gene_sample (gene_uuid UUID, sample_uuid UUID)")



con.execute("CREATE TABLE cell_line_sample (cell_line_uuid UUID, sample_uuid UUID)")

con.execute("ALTER TABLE disease_folder ADD COLUMN  disease_nickname TEXT")
con.execute("ALTER TABLE disease_folder ADD COLUMN  disease_name TEXT")
con.execute("ALTER TABLE disease_folder ADD COLUMN  stage TEXT")

con.execute("ALTER TABLE gene_folder ADD COLUMN  gene_nickname TEXT")
con.execute("ALTER TABLE gene_folder ADD COLUMN  gene_name TEXT")
con.execute("ALTER TABLE gene_folder ADD COLUMN  knockout TEXT")

con.execute("ALTER TABLE biomaterial_folder ADD COLUMN  biomaterial_type TEXT")
con.execute("ALTER TABLE biomaterial_folder ADD COLUMN  age TEXT")

con.execute("ALTER TABLE biomaterial_folder ADD COLUMN  breed TEXT")

con.execute("ALTER TABLE biomaterial_folder ADD COLUMN  strain TEXT")


con.execute("ALTER TABLE breed RENAME breed__uuid to breed_uuid")



import duckdb
import pandas as pd
import requests 
import numpy as np
import warnings
warnings.filterwarnings('ignore')



con = duckdb.connect("browser_cleaner.db")

import re



strain_df = pd.read_csv('./database/database_creation/strain.csv', dtype=object)

strain_df = strain_df.rename(columns={'Strain.name':'strain_name',
                                'Strain.crossReferences.identifier':'JAX_id',
                                'Strain.crossReferences.source.name':'strain_source',
                                'Strain.attributeString':'strain_description'})

con.execute("drop table strain")
con.execute("CREATE TABLE strain (strain_uuid UUID NOT NULL DEFAULT uuid(), PRIMARY KEY (strain_uuid))")
con.execute("ALTER TABLE strain ADD COLUMN  strain_name TEXT")
con.execute("ALTER TABLE strain ADD COLUMN  JAX_id TEXT")
con.execute("ALTER TABLE strain ADD COLUMN  strain_source TEXT")
con.execute("ALTER TABLE strain ADD COLUMN  strain_description TEXT")

con.execute("INSERT INTO strain (strain_name, JAX_id, strain_source, strain_description) select * from strain_df")
