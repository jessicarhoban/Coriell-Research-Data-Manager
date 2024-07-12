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

def parse_samplesheet(path, delimiter=",", header="infer"):
    df = pd.read_csv(path, dtype=str, delimiter=delimiter, header=header)
    df['index1'] = df.index
    df = df.reset_index(drop=True)
    if "[Header]" in df.columns:
        df = df[int((df["[Header]"] == "[Data]").idxmax())+1:]
        #df = df.drop(columns=df.columns[0])
        new_header = df.iloc[0]
        df = df[1:]
        df.columns = new_header
        #if df.iloc[0] == df.iloc[1]:
        #    df = df[1:]
        return df
    else:
        if len(df)>0:
            return df
        else:
            return pd.DataFrame()
        

def get_flist():
    flist = []
    count_samplesheets = 0
    count_fastqfolder = 0
    count_fastqfiles = 0
    count_none_exceptfiles = 0
    count_md5xt =0
    count_none = 0
    count_novogene =0
    folders = [f for f in glob.glob("../../../../mnt/data/research_data/*") if os.path.isdir(f) == True]

    for _, folder in enumerate(folders):
        
        frow = {"foldername":folder.split("/")[-1]}
        folderpaths1 = [f for f in glob.glob(folder+"/*") if os.path.isdir(f) == True]
        foldernames1 = [f.split("/")[-1] for f in folderpaths1]
        
        #-----------SEARCH FOR 00_fastq folder-----------
        fqfolder = []
        searches = ["/00_fastq/*", "/fastq/*", "/data/fastq/*", "/raw_data/*"]
        for search in searches:
            fqfolder.extend([f for f in glob.glob(folder+search) if os.path.isdir(f) == True])#, recursive=True

        if len(fqfolder)>0:
            fqfolder = [f.split("/")[-1]  for f in fqfolder if "fast" not in f.split("/")[-1]]
            fqfolder = [f.split("_L001")[0] for f in fqfolder]
            
            if len(fqfolder)>0:
                frow["fq_folders"]=fqfolder
                print(fqfolder)
                count_fastqfolder += 1

        #if len(fqfolder)>0:
        #    listt = [f.split("/")[-1] for f in glob.glob(folder+"/00_fastq/*") if os.path.isdir(f) == True]
        #    if len(listt)>0:
        #        frow["fq_folders"]=listt
        #        count_fastqfolder += 1

        #-----------SEARCH FOR novogene folder-----------
        if len([f for f in foldernames1 if "novogene.com" in f])>0:
            
            listt = [f for f in glob.glob(folder+"/**/*novogene.com/**/*aw*ata/*", recursive=True) if os.path.isdir(f) == True]
            if len(listt)>0:
                frow["novogene_folders"]=[f.split("/")[-1] for f in listt]
                full_df  = pd.DataFrame()
                count_novogene +=1 
                for f1 in listt:
                    # MD5.txt
                    md5txt = [f for f in glob.glob(f1+"/MD5.txt")]
                    if len(md5txt)>0:
                        df = parse_samplesheet(md5txt[0], delimiter="\s+", header=None)
                        df["folder"] = f1.split("/")[-1]
                        full_df =pd.concat([full_df,df],ignore_index=True)
                
                if len(full_df)>0:
                    frow["md5txt"] = full_df
                    count_md5xt +=1


        #-----------SEARCH FOR ALL FASTQ FILES-----------
        fq_searches = ["/**/*.fastq.gz","/**/*.fq.gz"]
        fq_files = [] 
        for search in fq_searches:
            fq_files.extend([f for f in glob.glob(folder+search, recursive=True)])

        if len(fq_files)>0:
            frow["fq_files"] = [f for f in fq_files]
            frow["fq_filenames"] = [f.split("/")[-1].replace(".fastq.gz","").replace(".fq.gz","") for f in fq_files]
            count_fastqfiles +=1

        #-----------SEARCH FOR SAMPLE SHEETS-----------
        searches = ["/**/*ssheet*.csv", "/**/*samplesheet*.csv","/**/*SampleSheet*.csv","/**/*sample_metadata*.csv",
                    "/**/*sample_sheet*.csv",
                    "/**/*sample_code*.txt","/**/*samples_reads*.txt", "/**/*_description.txt",
                    "raw_fastq_stats.txt",
                    "sn_rnaseq_mbrain_20220411.csv", "rrbs_129svev_aging_bac_20220511.csv"]
        samplesheets = []
        for search in searches:
            samplesheets.extend([f for f in glob.glob(folder+search, recursive=True)])

        if len(samplesheets)>0:
            frow["samplesheets"] = {}
            for sheet in samplesheets:
                try:
                    if "txt" in sheet:
                        df = parse_samplesheet(sheet, delimiter="\t")
                    else:
                        df = parse_samplesheet(sheet)
                    frow["samplesheets"][sheet.split("/")[-1]] = df
                    count_samplesheets +=1
                except:
                    print()
            
        #-----------ADD-----------
        if not any([k for k in frow.keys() if k in ["samplesheets", "fq_folders", "novogene_folders", "md5txt"]]):
            if not any([k for k in frow.keys() if k in frow["fq_files"]]):#["fq_files"]]):
                #print(count_none,frow["foldername"])
                #for f in foldernames1:
                #    print("\t",f)
                count_none+=1
            else:
                count_none_exceptfiles+=1
                #print(frow["foldername"])
                #for f in foldernames1:
                #    print("\t",f)

        flist.append(frow)


    print("None:", count_none)
    print("None Except FQFiles",count_none_exceptfiles)
    print("FQFiles",count_fastqfiles)
    print("FQFolders",count_fastqfolder)
    print("Sample Sheets",count_samplesheets)
    print("Novogene and MD5",count_novogene,count_md5xt)
    return flist


def send_to_file(flist):

    for f in flist:
        path = "./results/flist4/"+f["foldername"]
        if any([k for k in f.keys() if k in ["samplesheets", "fq_folders", "fq_files"]]):
            if not os.path.exists(path):
                os.makedirs(path)
            if "samplesheets" in f:
                for name, df in f["samplesheets"].items():
                    fullname = path+"/"+name.replace(".txt",".csv")
                    if df is not None:
                        df.to_csv(fullname, index=False)
            if "fq_folders" in f:
                fullname = path+"/fq_folders.csv"
                df00fold = pd.Series(f["fq_folders"])
                df00fold.to_csv(fullname, index=False)
            if "fq_files" in f:
                fullname = path+"/fq_files.csv"
                df00fil = pd.Series(f["fq_files"])
                df00fil.to_csv(fullname, index=False)
            if "novogene_folders" in f:
                fullname = path+"/novogene_folders.csv"
                df00fold = pd.Series(f["novogene_folders"])
                df00fold.to_csv(fullname, index=False)

            if "md5txt" in f:
                fullname = path+"/novogene_md5_txts.csv"
                if f["md5txt"] is not None:
                    f["md5txt"].to_csv(fullname, index=False)


flist = get_flist()
send_to_file(flist)






#-------------
flist2 = flist
# Find unmatched
#df[["Folder Name", "Unmatched FQ Files"]].drop_duplicates()["Unmatched FQ Files"].value_counts()
# 113 have 0 unmatched -- Good 
#df.loc[df["Unmatched FQ Files"].isin([2,8,82,16])]



unmatched = ['2023-02-17_bisamp_mpb_x15', # OK, all unmatched are in ssheet (besides undetermined)
           '2023-03-16_crispr_clones_jian', # WEIRD, zhaorui_clones-384141030
            '2023-06-27_cut_run_zhaorui',  # OK - all unmatched are in ssheet
            '2023-05-18_crispr3_zhaorui', # OK, all unmatched are in ssheet (besides undetermined)
            '2021-08-11_dnmt3a_mplex_blood_saliva', # Weird, have to check against ssheet
            '2022-05-07_arsa_merge_mplex12'] # A little weird, _val_1/2 after fastq

nofq = ['2022-02-24_25_bis_amplicon_cori_op1',
            '2019-07-17_rrbs24B_redownload',
            '2022-09-22_cpmc2_merged_bams',
            '2023-03-13_bisamp_x15_cuh123_combined_summaries',
            '2023-05-30_novogene_rnaseq']

weird_generally = ['2022-09-30_sc_atac_seq_human_brain_miseq_nano', # Only undetermined FQ,
                    '2023-04-19_sn_atacseq_human_brain_omics',# Only undetermined FQ,
                   '2023-01-21_sc_atac_mouse_intestine']# Fix samplesheet
               
multiple_ssheets = {'2021-10-27_bis_amplicon_cooper2x48':'SampleSheet2021-10-27.csv',
                    '2022-02-25_bis_amplicon_cori_op1':'corrected_bisamp_op1_20220223_samplesheet.csv',#all the same
                    '2021-12-02_bis_amplicon_cooper2_96_r1':'bisamp_cooper2r1_20211202_samplesheet.csv', #same
                    '2023-03-03_bisamp_x15_cuh2_corrected_barcodes':'bisamp_x15_cuh2_20230303_r1_samplesheet.csv', #same
                    '2023-04-18_sn_rnaseq_human_brain_omics':'sn_rnaseq_human_brain_20230418_samplesheet.csv',
                    '2023-02-23_crispr_zhaorui_atavistik':'SampleSheet_crispr.csv',# both
                    '2022-03-17_bis_amplicon_cori_op1r1':'corrected_bisamp_op1r1_20220317_samplesheet.csv'}



df = pd.DataFrame()

for f in flist2:
    if "fq_files" in f:
        if len(f["fq_files"])>0:


            sample_rows = []
            matched_samples = set()

            #---------------CHECK FOR FASTQ NAMING MATCHES---------------
            for file in f["fq_files"]:
                
                filepath = file
                file = file.split("/")[-1].replace(".fastq.gz","").replace(".fq.gz","")
                matches = re.findall("(.*)_(.*?)_(.*?)_(.*?)_001", file)
                sample = {"Filepath": filepath,
                          "Filename": file}

                if len(matches)>0:
                    sample["Match Status"] = "Matched"
                    sample["Sample Name"] = matches[0][0]
                    sample["Sample Number"] = matches[0][1]
                    sample["Lane Number"] = matches[0][2]
                    sample["Read"] = matches[0][3]
                    matched_samples.add(matches[0][0])
                    sample_rows.append(sample)

            # Re runthrough for duplicates
            for file in f["fq_files"]:
                filepath = file
                file = file.split("/")[-1].replace(".fastq.gz","").replace(".fq.gz","")
                matches = re.findall("(.*)_(.*?)_(.*?)_(.*?)_001", file)
                sample = {"Filepath": filepath,
                          "Filename": file}

                if len(matches)==0:
                    if any([s for s in matched_samples if s in file]):
                        sample["Match Status"] = "Duplicate"
                        name = [s for s in matched_samples if s in file]
                        if len(name)==1:
                            sample["Sample Name"] = name[0]
                        else:
                            name = sorted(name,key=len, reverse=True)
                            if len(name[0])==len(name[1]):
                                print(name)
                            else:
                                sample["Sample Name"] = name[0]
                    else:
                        sample["Match Status"] = "Unmatched"

                    sample_rows.append(sample)

            #
            sample_df = pd.DataFrame(sample_rows)
            sample_df["Folder Name"] = f["foldername"]

            #---------------CHECK FOR SAMPLESHEETS---------------
            if "samplesheets" in f:
                samplesheets = f["samplesheets"]
                if (len(samplesheets) >0) & (f["foldername"] not in weird_generally):

                    # Deduplicate:
                    if len(samplesheets) >1:
                        if f["foldername"] in multiple_ssheets:
                            samplesheets = {k:v for k,v in samplesheets.items() 
                                            if k == multiple_ssheets[f["foldername"]]}
                   
                    # Only 1 samplesheet per file now
                    samplesheet_df = pd.DataFrame()
                    
                    for name,df1 in samplesheets.items():
                        allowed_cols = ['sample_id','index','sample_project','i7_index_id','index2','i5_index_id',
                                            'index_plate_well','sample_name','description']

                        df1.columns = [str(x).lower() for x in df1.columns]
                        df1 = df1[[x for x in df1.columns if x in allowed_cols]]
                        samplesheet_df = pd.concat([df1,samplesheet_df],ignore_index=True)

                    samplesheet_df = samplesheet_df.drop_duplicates()

                    #---------------ACTUALLY MERGE IN---------------
                    if "sample_id" in samplesheet_df.columns:
                        for index,sample_row in sample_df.iterrows():
                            #
                            if sample_row["Match Status"] == "Matched":
                                samplesheet_rows = samplesheet_df.loc[samplesheet_df["sample_id"] == sample_row["Sample Name"]].reset_index()
                                if len(samplesheet_rows)==1:
                                    for col in samplesheet_rows.columns:
                                        sample_df.loc[index,col] = samplesheet_rows.loc[0][col]
                                    
                                    sample_df.loc[index,"Sample Name"] = samplesheet_rows.loc[0]["sample_id"]

                            # 
                            elif sample_row["Match Status"] == "Unmatched":
                                samplesheet_df["row_match"] = samplesheet_df.apply(lambda row: 
                                                                                   str(row["sample_id"]).lower() in str(sample_row["Filename"]).lower(),axis=1)
                                samplesheet_rows = samplesheet_df.loc[samplesheet_df["row_match"] == True].reset_index()
                                if len(samplesheet_rows)==1:
                                    for col in samplesheet_rows.columns:
                                        sample_df.loc[index,col] = samplesheet_rows.loc[0][col]
                                    
                                    sample_df.loc[index,"Match Status"] = "Matched (Sample Sheet)"
                                    sample_df.loc[index,"Sample Name"] = samplesheet_rows.loc[0]["sample_id"]

            #---------------CHECK FOR FOLDERNAMES---------------
            #"fq_folders", "novogene_folders", "md5txt"
            if "fq_folders" in f:
                fq_folders = f["fq_folders"]
                if len(fq_folders)>0:
                    for index,sample_row in sample_df.iterrows():
                        if sample_row["Match Status"] == "Unmatched":
                            fq_folder = [f for f in fq_folders if f.lower() in str(sample_row["Filename"]).lower()]
                            if len(fq_folder)>1:
                                fq_folder = [sorted(fq_folder,key=len, reverse=True)[0]]
                                sample_df.loc[index,"Sample Name"] = fq_folder[0]
                                sample_df.loc[index,"Match Status"] = "Matched (FQFolder)"
                            if len(fq_folder)==1:
                                sample_df.loc[index,"Sample Name"] = fq_folder[0]
                                sample_df.loc[index,"Match Status"] = "Matched (FQFolder)"

            #---------------CHECK FOR NOVOGENE---------------
            if "novogene_folders" in f:
                novogene_folders = list(set(f["novogene_folders"]))

                if len(novogene_folders)>0:

                    for index,sample_row in sample_df.iterrows():

                        if sample_row["Match Status"] == "Unmatched":

                            novogene_folder = [f for f in novogene_folders if f.lower() in str(sample_row["Filename"]).lower()]

                            if (len(novogene_folder)>1) & (len(novogene_folder)<=4):

                                novogene_folder = sorted(novogene_folder,key=len, reverse=True)

                                if len(novogene_folder[0]) == len(novogene_folder[1]):
                                    novogene_folder = [f for f in novogene_folder if f == sample_row["Filename"].split("_")[0]]
                                    sample_df.loc[index,"Sample Name"] = novogene_folder[0]
                                    sample_df.loc[index,"Match Status"] = "Matched (NovogeneFolder)"
                                else:
                                    novogene_folder = [sorted(novogene_folder,key=len, reverse=True)[0]]
                                    sample_df.loc[index,"Sample Name"] = novogene_folder[0]
                                    sample_df.loc[index,"Match Status"] = "Matched (NovogeneFolder)"
                                
                            elif len(novogene_folder)==1:
                                sample_df.loc[index,"Sample Name"] = novogene_folder[0]
                                sample_df.loc[index,"Match Status"] = "Matched (NovogeneFolder)"

            #---------------CUSTOM NAME CLEANING--------------
            for index,sample_row in sample_df.iterrows():
                if sample_row["Match Status"] == "Unmatched":
                    subs = ["[_-](?:[CGAT].*)-(?:[CGAT].*)","_R[12]_00[12]","_R[12]",
                            "_val_[12]","_unpaired_[12]","_qual_trimmed",
                            "_CKDL230012418-1A_H5MCKDSX7_L4_[12]","_fastq-r[12]"]
                    newname = sample_row["Filename"]
                    for sub in subs:
                        newname = re.sub(sub,"",newname)

                    clean_folders = ["2023-05-18_crispr3_zhaorui",
                                     "2023-03-16_crispr_clones_jian",
                                     "2019-10-22_rrbs_mouse_aging_rrbs41-42_himani_peace_genewiz",
                                     "2019-12-16_rnaseq_himani_aging_mouse_rnaseq_bgi",
                                     "2020-08-13_jian_RNAseq_LC-Lumina", 
                                     "2023-05-19_rrbs_mouse_himani",
                                     "2021-08-26_novaseq_YB5"] # Weird but ok
                                    
                    
                    if (newname == sample_row["Filename"].split("_")[0]) | (sample_row["Folder Name"] in clean_folders): # | (newname == sample_row["Filename"].split("-")[0]
                        sample_df.loc[index,"Sample Name"] = newname
                        sample_df.loc[index,"Match Status"] = "Matched (Name Sub)"

                    else:
                        if sample_row["Folder Name"] in ["2022-05-19_ying_wang_rnaseq",
                                                         "2021-08-26_novaseq_rrbs_mouse_peace_himani"]:
                            sample_df.loc[index,"Sample Name"] = newname.replace(newname.split("_")[-1],"")[:-1]
                            sample_df.loc[index,"Match Status"] = "Matched (Name Sub)"

                        elif sample_row["Folder Name"] == "2021-08-11_dnmt3a_mplex_blood_saliva":
                            if "_1" in newname:
                                newname = "mt1"+newname.replace("_1","")
                                sample_df.loc[index,"Sample Name"] = newname
                                sample_df.loc[index,"Match Status"] = "Matched (Name Sub)"
                            elif "_2" in newname:
                                newname = "mt2"+newname.replace("_2","")
                                sample_df.loc[index,"Sample Name"] = newname
                                sample_df.loc[index,"Match Status"] = "Matched (Name Sub)"

                        elif sample_row["Folder Name"] == "2023-05-18_novogene_novaseq_lane_undetermined":
                            newname = newname.split("-")[0]
                            sample_df.loc[index,"Sample Name"] = newname
                            sample_df.loc[index,"Match Status"] = "Matched (Name Sub)"
                            
                        elif sample_row["Folder Name"] in ["retrieved_2019-09-06_gm12878_meth",
                                                         "2022-06-01-rrbs_mouse_aging_microbiome_peace"]:
                            newname = newname.split("_")[0]
                            sample_df.loc[index,"Sample Name"] = newname
                            sample_df.loc[index,"Match Status"] = "Matched (Name Sub)"
                            #print(sample_row["Folder Name"], newname)

                        else:
                            print(sample_row["Filename"])
                            print("\t",newname)
                            print()

            sample_df["# Matched Files"] = len(sample_df[sample_df["Match Status"]=="Matched"])
            sample_df["# Matched (Name Sub) Files"] = len(sample_df[sample_df["Match Status"]=="Matched (Name Sub)"])
            sample_df["# Matched (NovogeneFolder) Files"] = len(sample_df[sample_df["Match Status"]=="Matched (NovogeneFolder)"])
            sample_df["# Matched (FQFolder) Files"] = len(sample_df[sample_df["Match Status"]=="Matched (FQFolder)"])
            sample_df["# Matched (Sample Sheet) Files"] = len(sample_df[sample_df["Match Status"]=="Matched (Sample Sheet)"])
            sample_df["# Unmatched Files"] = len(sample_df[sample_df["Match Status"]=="Unmatched"])
            sample_df["# Duplicate Files"] = len(sample_df[sample_df["Match Status"]=="Duplicate"])

            df = pd.concat([df,sample_df], ignore_index=True)
        

                    

#------------------------------------------------------------------















"""

            matched_files = [f for f in f["fq_files"] if len(re.findall("(.*)_(.*?)_(.*?)_(.*?)_001", f))>0]
            unmatched_files = [f for f in f["fq_files"] if len(re.findall("(.*)_(.*?)_(.*?)_(.*?)_001", f))==0]
            matches = [re.findall("(.*)_(.*?)_(.*?)_(.*?)_001", f) for f in f["fq_files"]]
            matches = [f for f in matches if len(f)>0]

            matched_samples = set()

            sample_rows = []
            if len(matches)>0:
                for match in matches:
                    sample = {"Sample Name":match[0][0],
                            "Sample Number":match[0][1],
                            "Lane Number":match[0][2],
                            "Read":match[0][3],
                            "Folder Name":f["foldername"],
                            "Unmatched FQ Files": len(unmatched_files)}
                    #df_links.append(sample)
                    sample_rows.append(sample)
                    matched_samples.add(match[0][0])

            if len(unmatched_files)>0:
                duplicate_files = [f for f in unmatched_files if any([s for s in matched_samples if s in f])]
                unmatched_files = [f for f in unmatched_files if f not in duplicate_files]

            sample_df = pd.DataFrame(sample_rows)
            sample_df["Total FQ Files"] = len(f["fq_files"])
            sample_df["Matched FQ Files"] = len(matched_files)
            sample_df["Unmatched FQ Files"] = len(unmatched_files)
            sample_df["Duplicate FQ Files"] = len(duplicate_files)
            sample_df["Source"] = "FastQ File Name"
            
            df = pd.concat([df,sample_df], ignore_index=True)




            # ------- Try to read in data ------
            # ["samplesheets", "fq_folders", "novogene_folders", "md5txt"]
            if "samplesheets" in f:
                if len(f["samplesheets"]) >0:
                    samplesheets = f["samplesheets"]
                    if len(samplesheets) > 1:
                        if f["foldername"] in multiple_ssheets:
                            samplesheets = {k:v for k,v in samplesheets.items() 
                                            if k == multiple_ssheets[f["foldername"]] }
                    
                    if len(samplesheets) > 1:
                        print(f["foldername"])

                    else:

                        for name, df1 in samplesheets.items():
                            #all_ssheet_cols.extend(list(df1.columns))
                            allowed_cols = ['sample_id','index','sample_project','i7_index_id','index2','i5_index_id',
                                            'index_plate_well','sample_name','description']

                            df1.columns = [str(x).lower() for x in df1.columns]
                            df1 = df1[[x for x in df1.columns if x in allowed_cols]]

                            if "sample_id" in df1.columns:
                                sample_df = sample_df.merge(df1, left_on="Sample Name", 
                                                            right_on="sample_id", how="left")
                                
            df = pd.concat([df,sample_df], ignore_index=True)
            








pd.Series([str(f).lower() for f in all_ssheet_cols]).value_counts()






                      if "sample_id" in df1.columns:
                            mergetest1 = sample_df.merge(df1, left_on="Sample Name", 
                                                        right_on="sample_id", how="outer", indicator=True)
                            merge1both = len(mergetest1[mergetest1["_merge"] == "both"])
                            merge1right = mergetest1[mergetest1["_merge"] == "right_only"]
                            if len(merge1right)>0:
                                #print(f["foldername"])
                                #print(merge1right["sample_id"].unique())
                                weird_sheets.add(f["foldername"])
                                








# --------  71 done, 128 left --------
flist2 = [f for f in folders if f.split("/")[-1] not in [f["foldername"] for f in flist]]
flist3 = []

i=1
for f2 in flist2:
    folderpaths1 = [f for f in glob.glob(f2+"/*") if os.path.isdir(f) == True]
    frow = {"foldername":f2.split("/")[-1]}

    for f1 in folderpaths1:
        samplesheets = []
        searches = [] #"/**/*metadata*.*sv", "/**/*sample*ids*.txt", 
        for search in searches:
            samplesheets.extend([f for f in glob.glob(f1+search, recursive=True)])
        if len(samplesheets)>0:
            frow["samplesheets"] = {}
            for sheet in samplesheets:
                print(f1, sheet)
                try:
                    df = parse_samplesheet(sheet)
                    frow["samplesheets"][sheet.split("/")[-1]] = df
                except:
                    print("Didn't work")
        
    if not any([k for k in frow.keys() if k in ["samplesheets"]]):
        print(i,frow["foldername"])
        for f in foldernames1:
            print("\t",f)
        i+=1

    else:
        flist3.append(frow)
    
for f in flist3:
    path = "./results/flist/"+f["foldername"]
    print(path)
    if not os.path.exists(path):
        os.makedirs(path)
        if "samplesheets" in f:
            for name,df in f["samplesheets"].items():
                fullname = path+"/"+name
                if df is not None:
                    df.to_csv(fullname, index=False)






            #print(f2.split("/")[-1])
            #print("\t"+f1.split("/")[-1])
            #for s in samplesheets:
            #    print("\t\t"+s.split("/")[-1])


# novogene.com -> *metadata-information*



    #else:
    #    for _, folder1 in enumerate(folderpaths1):
    #        samplesheets1 = [f for f in glob.glob(folder1+"/*samplesheet*.csv")]
    ##        samplesheets1.extend([f for f in glob.glob(folder1+"/*sample_code*.txt")])
     #       if len(samplesheets1)>0:
     #           frow["samplesheets1"] = {}
      #          for sheet in samplesheets1:
      #              df = parse_samplesheet(sheet)
      #              frow["samplesheets1"][sheet.split("/")[-1]] = df

        #print("\t" + ",".join(samplesheets))
        frow["samplesheets"] = samplesheets
    else:
        folderpaths1 = [f for f in glob.glob(folder+"/*") if os.path.isdir(f) == True]
        print()
        print(folder.split("/")[-1])
        

# Initialize list to hold DataFrame rows
row_list = []
folder1_names = []
folder2_names = []
folder3_names = []


# Grab all folder names
folders = [f for f in glob.glob("../../../../mnt/data/research_data/*") if os.path.isdir(f) == True]
# Iterate over folder names and capture index
for _, folder in enumerate(folders):

    folderpaths1 = [f for f in glob.glob(folder+"/*") if os.path.isdir(f) == True]
    foldernames1 = [f.split("/")[-1] for f in folderpaths1]
    

    
    
    
    
    
    novogene_fold = [f for f in foldernames1 if ".novogene.com" in f]

    if "00_fastq" in foldernames1:
        folder1_names.append("00_fastq")
        print(folder.split("/")[-1])
        print("\t00_fastq")

    elif len(novogene_fold)>0:
        folder1_names.append(novogene_fold[0])
        print(folder.split("/")[-1])
        print("\t"+novogene_fold[0])

    else:
        print(folder.split("/")[-1])
    
        for idx, folder1 in enumerate(folderpaths1):
            folder1_names.append(folder1)
            folderpaths2 = [f for f in glob.glob(folder1+"/*") if os.path.isdir(f) == True]
            foldernames2 = [f.split("/")[-1] for f in folderpaths2]
            print("\t"+folder1.split("/")[-1])
    
    """