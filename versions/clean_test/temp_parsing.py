
import duckdb
con = duckdb.connect("../database/browser_test.db")
import pandas as pd
import uuid

df = pd.read_csv("./files_samples_folders2.csv",dtype=str)
df = df[[col for col in df.columns if "#" not in col]]
df = df.drop(columns = ["row_match", "level_0"])
df = df.rename(columns={k:k.replace(" ","_").lower()+"_ssheet" for k in df.columns 
                        if k not in ["Sample Number","Lane Number","Read", 
                                     "Folder Name", "Filepath", "Filename", "Match Status", "Sample Name"]})

df = df.rename(columns={k:k.replace(" ","_").lower() for k in df.columns 
                        if k in ["Folder Name", "Filepath", "Filename",  "Match Status", "Sample Name"]})

df = df.rename(columns={k:k.replace(" ","_").lower()+"_fqname" for k in df.columns 
                        if k in ["Sample Number","Lane Number","Read"]})
df = df.drop_duplicates()



# Folders (198)
folders = con.sql("select distinct foldername, folder_uuid from folder").df()
print(len(df))
# Merge in folder_uuid
df = df.merge(folders, left_on = "folder_name", right_on="foldername", how="outer")
print(len(df))
# Sample df (7570)
df['sample_numfiles'] = df.groupby(['folder_name','sample_name'])['filepath'].transform('size').fillna(0).astype('Int64')



sample_cols = ['folder_name', 'sample_name','sample_numfiles']
sample = df[sample_cols].loc[df["sample_name"].notnull()].drop_duplicates()
sample["sample_uuid"] = sample.apply(lambda _: uuid.uuid4(), axis=1) 

# Merge in sample_uuid
df = df.merge(sample, on = ["folder_name", "sample_name"], how='left', indicator=True)
print(len(df))
sample = sample.drop(columns=["folder_name"])

# Create sample-folder links
sample_folder = df.loc[df["_merge"]=="both"][['folder_uuid', 'sample_uuid']].drop_duplicates()

# Files (27597)
df["file_uuid"] = df.apply(lambda _: uuid.uuid4(), axis=1) 
file_columns = ['filepath', "filename", 'match_status', 'file_uuid',
                'sample_number_fqname', 'lane_number_fqname', 'read_fqname',
                'sample_id_ssheet', 'sample_name_ssheet', 'description_ssheet',
                'index_plate_well_ssheet', 'index_ssheet', 'i7_index_id_ssheet',
                'index2_ssheet', 'i5_index_id_ssheet', 'sample_project_ssheet',
                'folder_uuid','sample_uuid']
file = df.loc[df["sample_name"].notnull()][file_columns].drop_duplicates()
file_sample_folder = file[['file_uuid', 'sample_uuid', 'folder_uuid']]
file = file[[col for col in file.columns if col not in ['folder_uuid','sample_uuid']]]


file["filepath"] = file["filepath"].str.replace("../../../../","")

sample.to_csv("./upload/sample.csv",index=False)
sample_folder.to_csv("./upload/sample_folder.csv",index=False)
file.to_csv("./upload/file.csv",index=False)
file_sample_folder.to_csv("./upload/file_sample_folder.csv",index=False)


con.execute("create table sample as select * from sample")
con.execute("create table sample_folder as select * from sample_folder")
con.execute("create table file as select * from file")
con.execute("create table file_sample_folder as select * from file_sample_folder")










df = df.rename(columns = {"Sample Number":"Sample Number (FQ)", 
                          "Lane Number":"Lane Number (FQ)", 
                          "Read":"Read Number (FQ)", 
                          "sample_id":"Sample ID (Sample Sheet)",
                          "sample_name":"Sample Name (Sample Sheet)",
                          "description":"Description (Sample Sheet)",
                          "index_plate_well":"Index Plate Well (Sample Sheet)",
                          "index":"Index 1 (Sample Sheet)",
                          "i7_index_id": "i7 Index (Sample Sheet)",
                          "index2":"Index 2 (Sample Sheet)",
                          "i5_index_id":"i5 Index (Sample Sheet)",
                          "sample_project":"Sample Project (Sample Sheet)"})
