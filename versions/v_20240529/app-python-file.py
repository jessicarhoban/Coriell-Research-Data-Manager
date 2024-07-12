import os
import glob
import re
#os.getcwd()

#195 folders
#-------------------------------------------------------------------------

#____Experiment type parsing____
experiment_types = [
             {"search_term":"RNA-seq", "name":"RNA-seq"},
             {"search_term":"low pass WGS", "name": "Low Pass Whole Genome Sequencing (WGS)"}, #JJ
             {"search_term":"eukmrnaseq", "name": "Eukaryotic mRNA-Seq"},
             {"search_term":"lncrnaseq", "name": "Long Noncoding RNA-Seq (LncRNA)"},
             {"search_term":"lncrna", "name": "Long Noncoding RNA-Seq (LncRNA)"},
             {"search_term":"mrna", "name": "mRNA"},
             {"search_term":"crispr", "name": "CRISPR"},
             {"search_term":"scrnaseq", "name": "Single Cell RNA-Seq (scRNA-seq)"},
             {"search_term":"scrna", "name": "Single Cell RNA-Seq (scRNA-seq)"},
             {"search_term":"snatacseq", "name": "Single Nucleus ATAC-Seq (snATAC-seq)"},
             {"search_term":"sn_atacseq", "name": "Single Nucleus ATAC-Seq (snATAC-seq)"},
             {"search_term":"scrnaseq", "name": "Single Cell RNA-Seq (scRNA-seq)"},
             {"search_term":"sc_rnaseq", "name": "Single Cell RNA-Seq (scRNA-seq)"},
             {"search_term":"scrna-seq", "name": "Single Cell RNA-Seq (scRNA-seq)"},
             {"search_term":"sn_rnaseq", "name": "Single Nucleus RNA-Seq (snRNA-seq)"},
             {"search_term":"snrnaseq", "name": "Single Nucleus RNA-Seq (snRNA-seq)"},
             {"search_term":"sc_atacseq", "name": "Single Cell ATAC-Seq (scATAC-seq)"},
             {"search_term":"sc_atac", "name": "Single Cell ATAC-Seq (scATAC-seq)"},
             {"search_term":"sc_atac-seq", "name": "Single Cell ATAC-Seq (scATAC-seq)"},
             {"search_term":"scatac-seq", "name": "Single Cell ATAC-Seq (scATAC-seq)"},
             {"search_term":"rrbs", "name": "Reduced-Representation Bisulfite Sequencing (RRBS-seq)"},
             {"search_term":"dream", "name": "DREAM"},
             {"search_term":"16s", "name": "16S rRNA"},
             {"search_term":"miseq_micro", "name": "MiSeq Micro"},#***********Keep here?
             {"search_term":"miseq_nano", "name": "MiSeq Nano"}, #***********Keep here?
             {"search_term":"miseq", "name": "MiSeq"}, #***********Keep here?
             {"search_term":"novoseq", "name": "NovoSeq"}, #***********Keep here?
             {"search_term":"ampliseq", "name": "AmpliSeq"},
             {"search_term":"cutrun", "name": "CUT&RUN"},
             {"search_term":"cut_run", "name": "CUT&RUN"},
             {"search_term":"cut&run", "name": "CUT&RUN"},
             {"search_term":"bisulfite_amplicon", "name": "Bisulfite Amplicon (BSAS)"},
             {"search_term":"bisulfite amplicon", "name": "Bisulfite Amplicon (BSAS)"},
             {"search_term":"bisamp", "name": "Bisulfite Amplicon (BSAS)"},
             {"search_term":"bis_amplicon", "name": "Bisulfite Amplicon (BSAS)"},
             {"search_term":"hwgs", "name": "Human Whole Genome Sequencing (hWGS)"}, #***********
             {"search_term":"wgs", "name": "Whole Genome Sequencing (WGS)"}, #***********
             {"search_term":"rnaseq", "name":"RNA-seq"},
             {"search_term":"grna", "name":"Guide RNA (gRNA)"},
             {"search_term":"chip", "name":"Chromatin Immunoprecipitation Sequencing (ChIP-Seq)"},
             {"search_term":"atacseq", "name": "ATAC-seq"},
             {"search_term":"atac-seq", "name": "ATAC-seq"},
             {"search_term":"atac_seq", "name": "ATAC-seq"}]
             
#____Experiment company parsing____
company_types = [{"search_term":"gencove", "name":"Gencove"},
                 {"search_term":"novogene", "name": "Novogene"},
                 {"search_term":"10x", "name":"10x"},
                 {"search_term":"bgi", "name":"BGI"},
                 {"search_term":"genewiz", "name": "GENEWIZ"},
                 {"search_term":"atavistik", "name":"Atavistik Bio"},
                 {"search_term":"qiagen", "name":"Qiagen"},
                 {"search_term":"zymo", "name":"Zymo Research"},
                 {"search_term":"gain therapeutics", "name":"Gain Therapeutics"}]

#____Organism parsing____
organism_types = [{"search_term":"human", "name":"Human"},
                  {"search_term":"hek", "name":"Human"},
                  {"search_term":"goals", "name":"Human"},
                  {"search_term":"optin", "name":"Human"},
                  {"search_term":"gf mice", "name": "Mouse (Germ-Free)"},
                  {"search_term":"mouse", "name": "Mouse"},
                  {"search_term":"hwgs", "name":"Human"}]
             
#____Tissue parsing____
tissue_types = [
                {"search_term":"cord blood", "name":"Cord Blood"},
                {"search_term":"blood", "name":"Blood"},
                {"search_term":"skeletal muscle", "name":"Skeletal Muscle"},
                {"search_term":"adjacent normal tissue", "name":"Adjacent Normal Tissue"},
                {"search_term":"heart", "name": "Heart"},
                {"search_term":"kidney", "name": "Kidney"},
                {"search_term":"saliva", "name":"Saliva"},
                {"search_term":"spleen", "name": "Spleen"},
                {"search_term":"colon", "name": "Colon"},
                {"search_term":"tumors", "name": "Tumor"},
                {"search_term":"tumor", "name": "Tumor"},
                {"search_term":"crc", "name": "Colon"}, 
                {"search_term":"crc", "name": "Rectum"}, 
                {"search_term":"organoid", "name": "Organoid"},
                {"search_term":"brainoids", "name": "Brain Organoid"},
                {"search_term":"brain", "name": "Brain"},
                {"search_term":"esophagus", "name": "Esophagus"},
                {"search_term":"intestine", "name": "Intestine"},
                {"search_term":"ffpe", "name": "Formalin-Fixed or Paraffin-Embedded Tissue (FFPE)"}, # Treatment type?
                {"search_term":"aorta", "name": "Aorta"}]


#____Cell parsing____
cell_types =   [{"search_term": "bloodsep",       "name": "Blood Separated Subpopulations"},
                {"search_term": "cd8 lympocytes", "name": "Lymphocytes (CD8)"}, 
                {"search_term": "esc",            "name": "Embryonic Stem Cells (ESCs)"},
                {"search_term": "endo",           "name": "Endothelial Cells"},
                {"search_term": "hek",            "name": "Human Embryonic Kidney (HEK)"}, #Cell line or tissue? # Add to kidney?
                {"search_term": "hsc",            "name": "Hematopoietic Stem Cells (HSCs)"},#Cell line or tissue?
                {"search_term": "ipsc",           "name": "Induced Pluripotent Stem Cells (iPSCs)"}, #Cell line or tissue?
                {"search_term": "lcl",            "name": "Lymphoblastoid Cell Lines (LCLs)"}#Cell line or tissue?
                ]

             
   
#____Disease parsing____
# Add overarching categories?
disease_types = [{"search_term":"cancer",   "name": "Cancer", "category": "Cancer"},
                {"search_term":"aging",    "name": "Aging", "category":""},
                {"search_term":"young",    "name": "Aging", "category":""},
                {"search_term":"cml",    "name": "Chronic Myeloid Leukemia (CML)", "category":"Cancer"},
                {"search_term":"aml",    "name": "Acute Myeloid Leukemia (CML)", "category":"Cancer"},
                {"search_term":"crc",      "name": "Colorectal Cancer", "category": "Cancer"},
                {"search_term":"leukemia", "name": "Leukemia", "category": "Cancer"},
                {"search_term":"parkinson","name": "Parkinson's Disease", "category": "Neurodegenerative"}]

#____Lab parsing____
#
lab_types = [{"search_term":"engel",      "name":"The Nora Engel Lab"},
            {"search_term":"jj",          "name":"The Issa & Jelenik Lab"},
            {"search_term":"jian_huang_lab",  "name":"The Jian Huang Lab"},
            {"search_term":"jian",        "name":"The Jian Huang Lab"},
            {"search_term":"chen",        "name":"The Luke Chen Lab"},
            {"search_term":"shumei",      "name":"The Shumei Song Lab"},
            {"search_term":"scheinfeldt", "name":"The Scheinfeldt Lab"}]
            
#____Employee parsing____
# 
employee_types = [{"search_term": "woonbok",  "name": "Woonbok Chung",      "affiliation": "Coriell",  "lab": "The Issa & Jelenik Lab"},
                  {"search_term": "jj",       "name": "Jaroslav Jelinek",   "affiliation": "Coriell",  "lab": "The Issa & Jelenik Lab"},
                  {"search_term": "shoghag",  "name": "Shoghag Panjarian",  "affiliation": "Coriell",  "lab": "The Issa & Jelenik Lab"},
                  {"search_term": "himani",   "name": "Himani Vaidya",      "affiliation": "Coriell",  "lab": "The Issa & Jelenik Lab"},
                  {"search_term": "peace",    "name": "Peace Park",         "affiliation": "Coriell",  "lab": "The Issa & Jelenik Lab"},
                  {"search_term": "matt",     "name": "Matthew Walt",       "affiliation": "Coriell",  "lab": "The Issa & Jelenik Lab"}, #Double Check
                  {"search_term": "jian",     "name": "Jian Huang",         "affiliation": "Coriell",  "lab": "The Jian Huang Lab"},
                  {"search_term": "zhaorui",  "name": "Zhaorui Lian",       "affiliation": "Coriell",  "lab": "The Jian Huang Lab"},
                  {"search_term": "zhang",    "name": "Yi Zhang",       .   "affiliation": "",  "lab": ""}, #Double Check
                  {"search_term": "engel",    "name": "Nora Engel",         "affiliation": "Coriell",  "lab": "The Nora Engel Lab"},
                  {"search_term": "diego",    "name": "Diego Morales",      "affiliation": "Coriell",  "lab": ""},
                  {"search_term": "wang",     "name": "Peng Wang",          "affiliation": "Coriell",  "lab": ""}, #Double Check
                  {"search_term": "laura",    "name": "Laura Scheinfeldt",  "affiliation": "Coriell","lab": "The Scheinfeldt Lab"},
                  {"search_term": "platoff",  "name": "Rebecca Platoff",    "affiliation": "Cooper University Hospital (CUH)","lab": ""},
                  {"search_term": "bela",     "name": "Bela Patel",         "affiliation": "Fels Institute for Cancer Research and Molecular Biology","lab": ""},
                  {"search_term": "whelan",   "name": "Kelly Whelan",       "affiliation": "Fels Cancer Institute for Personalized Medicine","lab": ""},
                  {"search_term": "italo",    "name": "Italo Tempera",      "affiliation": "Fels Institute for Cancer Research and Molecular Biology","lab": ""},
                  {"search_term": "tempera",  "name": "Italo Tempera",      "affiliation": "Fels Institute for Cancer Research and Molecular Biology","lab": ""},
                  {"search_term": "hyeseon",  "name": "HyeSeon",            "affiliation": "","lab": ""},
                  {"search_term": "sierra",   "name": "Sierra",             "affiliation": "","lab": ""},
                  {"search_term": "gigi",     "name": "Gigi",               "affiliation": "","lab": ""},
                  {"search_term": "ying",     "name": "Ying","affiliation": "","lab": ""},
                  {"search_term": "olivia",   "name": "Olivia","affiliation": "", "lab": ""},
                  {"search_term": "zach",     "name": "Zach","affiliation": "","lab": ""},
                  {"search_term": "wes",      "name": "Wes","affiliation": "","lab": ""}]
                  
                   
            
#____Partner parsing____
#
partner_types = [{"search_term": "cooper", "name": "Cooper University Hospital (CUH)"},
                 {"search_term": "cuh", "name": "Cooper University Hospital (CUH)"},
                 {"search_term": "gsk", "name": "GSK"},
                 {"search_term": "cori", "name": "Camden Opioid Research Initiative (CORI)"},
                 {"search_term": "ninds", "name": "National Institute of Neurological Disorders and Stroke (NINDS)"},
                 {"search_term": "cpmc", "name": "Coriell Personalized Medicine Collaborative"}, 
                 {"search_term": "fels", "name": "Fels Institute for Cancer Research and Molecular Biology"}
                 ]  
            
#____Cell line parsing____
cellline_types = [
                 {"search_term": "miapaca2",  "name":"MIA PaCa-2",                 "organism": "Human", "accession_id": "Cellosaurus:CVCL_0428"},
                 {"search_term": "ht29",      "name":"HT-29",                      "organism": "Human", "accession_id":"Cellosaurus:CVCL_0320"}, #HT-29 human colorectal carcinoma cell line ?
                 {"search_term": "bon1",      "name":"BON-1",                      "organism": "Human", "accession_id":"Cellosaurus:CVCL_3985"},
                 {"search_term": "yb5",       "name":"YB5 (der. from SW48)",       "organism": "Human", "accession_id":"Cellosaurus:CVCL_1724"}, #???
                 {"search_term": "apcmin",    "name":"C57BL/6J-ApcMin/J (ApcMin)", "organism": "Mouse", "accession_id":"IMSR_JAX:002020"},
                 {"search_term": "gu114",     "name":"GU114",                      "organism": "Human", },
                 {"search_term": "gm12878",   "name":"GM12878",                    "organism": "Human", "accession_id":"Cellosaurus:CVCL_7526"},
                 {"search_term": "b6",        "name":"C57BL/6J (B6)",              "organism": "Mouse", "accession_id":"IMSR_JAX:000664"},
                 {"search_term": "yccel",     "name":"YCCEL1",                     "organism": "Human", "accession_id":"Cellosaurus:CVCL_9647"}, # SNU YCCEL??
                 {"search_term": "nalm13",    "name":"NALM13",                     "organism": "Human", "accession_id":"Cellosaurus:CVCL_L061"},
                 {"search_term": "nalm14",    "name":"NALM14",                     "organism": "Human", "accession_id":"Cellosaurus:CVCL_QX00"},
                 {"search_term": "gm00538",   "name":"GM00538",                    "organism": "Human", "accession_id":"Cellosaurus:CVCL_9W75"},
                 {"search_term": "mv411",     "name":"MV-4-11",             "organism": "Human", "accession_id":"Cellosaurus:CVCL_0064"},
                 {"search_term": "na12878",   "name":"NA12878", "organism": "Human", "accession_id":"Cellosaurus:CVCL_7526"},
                 {"search_term": "mcf7",      "name":"MFC-7", "organism": "Human", , "accession_id":"Cellosaurus:CVCL_0031"},
                 {"search_term": "129svev",   "name":"129/SvEv", "organism": "Mouse", "accession_id":"Cellosaurus:CVCL_C0LC"}
                 ]    
                 
                 
#____Gene/Protein parsing____
#
geneprot_types = [{"search_term": "arsa",   "accession_id": "410",   "name": "ARSA (Arylsulfatase A)", "organism":"Human"},
                  {"search_term": "spry3",  "accession_id": "10251", "name": "Sprouty RTK Signaling Antagonist 3 (SPRY3)", "organism":"Human"},
                  {"search_term": "spry4",  "accession_id": "81848", "name": "Sprouty RTK Signaling Antagonist 4 (SPRY4)", "organism":"Human"},
                  {"search_term": "dbet",   "accession_id": "100419743", "name": "D4Z4 Binding Element Transcript (DBET)", "organism":"Human"},
                  {"search_term": "asxl1",  "accession_id": "171023", "name": "ASXL Transcriptional Regulator 1 (ASXL1)", "organism":"Human"},
                  {"search_term": "znf251", "accession_id": "90987", "name": "Zinc Finger Protein 251 (ZNF251)"},
                  {"search_term": "tet2",   "accession_id": "54790", "name": "Tet Methylcytosine Dioxygenase 2 (TET2)"}, 
                  {"search_term": "dnmt3a", "accession_id": "1788", "name": "DNA Methyltransferase 3 Alpha (DNMT3A)"},
                  {"search_term": "dlat",   "accession_id": "1737", "name": "Dihydrolipoamide S-acetyltransferase (DLAT)"},
                  {"search_term": "elov13", "accession_id": "83401", "name": "ELOVL Fatty Acid Elongase 3 (ELOVL3)"},
                  {"search_term": "p53",    "accession_id": "7157", "name": "Tumor Protein (P53)"},
                  {"search_term": "yod1",   "accession_id": "55432", "name": "YOD1 Deubiquitinase (YOD1)"},
                  {"search_term": "pdha1",  "accession_id": "5160", "name": "Pyruvate Dehydrogenase E1 Subunit Alpha 1 (PDHA1)"},
                  {"search_term": "pdhb",   "accession_id": "5162", "name": "Pyruvate Dehydrogenase E1 Subunit Beta (PDHB)"},
                  {"search_term": "cdk5r1", "accession_id": "8851", "name": "Cyclin-Dependent Kinase 5 Regulatory Subunit 1 (CDK5R1)"}, 
                  {"search_term": "cdk9",   "accession_id": "1025", "name": "Cyclin-Dependent Kinase 9 (CDK9)"},
                  
                  {"search_term": "k9me3",  "accession_id": "", "name": "Histone 3 (K9me3) Bivalent Methylation Factor"},
                  {"search_term": "ebna",   "accession_id": "", "name": "EBV Nuclear Antigen (EBNA)"},
                  {"search_term": "id3",    "accession_id": "3399", "name": "Inhibitor Of DNA Binding 3 (ID3)", "organism":"Mouse"},#MOUSE
                  {"search_term": "mmp",    "accession_id": "", "name": "Matrix metalloproteinases (MMPs)", "organism":"Mouse"}, #MOUSE
                  {"search_term": " evi",   "accession_id": "", "name": "Ecotropic Viral Integration (EVI)", "organism":"Mouse"}, #MOUSE
                  {"search_term": " tet",   "accession_id": "", "name": "Ten Eleven Translocation (TET)", "organism":"Mouse"} #MOUSE
                  ]  

#____Sample parsing____
#   
sample_types =    [{"search_term": " ko ", "name": "Knock-out (KO)"},
                   {"search_term": " kos ", "name": "Knock-out (KO)"},
                   {"search_term": " wt ", "name": "Wild-type (WT)"},
                   {"search_term": " wts ", "name": "Wild-type (WT)"},
                   {"search_term": "control", "name": "Control"},
                   {"search_term": "gf mice", "name": "Germ-Free"}]  
                   

#____Treatment parsing____
# 
treatment_types = [{"search_term": "dmso", "name": "Dimethyl Sulfoxide (DMSO)"},
                   {"search_term": "dac", "name": "Decitabine (DAC)"},
                   {"search_term": "thz1", "name": "(THZ1)"},
                   {"search_term": "bsj", "name": "(BSJ)"},
                   {"search_term": "bsj4", "name": "(BSJ4)"},
                   {"search_term": "protac", "name": "Proteolysis Targeting Chimera (PROTAC)"},
                   {"search_term": "olaparib", "name": "Olaparib"},
                   {"search_term": "gilterinib", "name": "Gilterinib"},
                   {"search_term": "5aza", "name": "5-Azacytidine (5-aza)"},
                   {"search_term": "azd", "name": "AZD-7648 DNA-PK inhibitor (AZD)"},
                   {"search_term": "bdnf",   "name": "Brain-Derived Neurotrophic Factor (BDNF)"}, # Double check
                   {"search_term": "c10orf125", "name": "C10orf125 Polyclonal Antibody"} # Double check
                   ] 
                   
#____Study parsing____
#   
study_types = [{"search_term": "optin", "name": "Optimizing Pain Treatment In New Jersey (OPTIN) Study",  "disease": "Opioid Use Disorder", "disease_category":"Addiction"},
                 {"search_term": "goals", "name": "Genomics of Opioid Addiction Longitudinal Study (GOALS) Study"}]  

            
#-------------------------------------------------------------------------    
# Initialize dictionary
dictt = []

# Read in JJ's annotations
df1 = pd.read_csv("./data/test-app/research_data_annotation_2023-02-27.tsv", sep="\t")

# Grab all folder names
flist = [f for f in glob.glob("../../mnt/data/research_data/*") if os.path.isdir(f) == True]

# Iterate over folder names and capture index
for idx, f in enumerate(flist):
  
  # Grab only folder name
  fname = f.split("/")[-1]
  
  # Find where JJ annotation matches
  jj_type = df1.loc[df1["sequencing run"] == fname]["type"]
  jj_description = df1.loc[df1["sequencing run"] == fname]["description"]
  
  jj_type = "" if len(jj_type) == 0 else jj_type.values[0]
  jj_description = "" if len(jj_description) == 0 else jj_description.values[0]
  
  #____Initialize Dict Structure____
  dictf = {"date": "",
                "folderpath":f,
                "foldername":fname,
                "reduced_foldername": fname.lower(),
                "reduced_jjdescription": jj_description.lower(),
                "jj_description": jj_description,
                "jj_type": jj_type,
                "cbix1/2": "CBIX"}   
  
  #____Date parsing____
  fdate = re.findall("(\d\d\d\d-\d\d-\d\d)", f)
  
  if len(fdate) == 0:
    dictf["date"] = "Null"
  else:
    dictf["date"] =  fdate[0]
    dictf["reduced_foldername"] = dictf["reduced_foldername"].replace(fdate[0], "")

              
  #____Other parsing____
  other_types = [[partner_types, "partner"],
                 [study_types, "study"],
                 [experiment_types, "experiment"],
                 [organism_types, "organism"],
                 [tissue_types, "tissue"],
                 [cell_types, "cell_type"],
                 [sample_types, "sample"],
                 [disease_types, "disease"],
                 [cellline_types, "cell_line"],
                 [geneprot_types, "gene/protein"],
                 [treatment_types, "treatment"],
                 [lab_types, "lab"],
                 [employee_types, "employee"],
                 [company_types, "company"]
                 ]
  
  search_terms = set()
  dictf["disease_category"] = set()
  dictf["disease_category_jj"] = set()
  
  for other_type in other_types:
    temp_fname = fname.lower()
    temp_fnamejj = (dictf["jj_description"] + " " + dictf["jj_type"]).lower()
    
    typedict = other_type[0]
    dictkey = other_type[1]
    
    dictf[dictkey] = set()
    #dictf[dictkey+"_jj"] = set()
    
    for typee in typedict:
      
      # Create temporary foldername so there's only one match per category
      if typee["search_term"] in temp_fname:
        search_terms.add(typee["search_term"])
        #temp_fname = temp_fname.replace(typee["search_term"], "")
        
        dictf[dictkey].add(typee["name"])
        
        if dictkey == "employee":
          if typee["lab"] != "":
            dictf["lab"].add(typee["lab"])
          if typee["affiliation"] not in ["Coriell", ""]:
            dictf["partner"].add(typee["affiliation"])
        
        elif dictkey == "cell_line":
          
          for key in ["organism", "disease", "disease_category", "cell_type", "tissue"]:
            if key in typee:
              dictf[key].add(typee[key])
              
        elif dictkey == "disease":
          dictf["disease_category"].add(typee["category"])
    
      #---------------------------------
      # Do the same for JJ description
      if typee["search_term"] in temp_fnamejj:
        
        search_terms.add(typee["search_term"])
        #temp_fnamejj = temp_fnamejj.replace(typee["search_term"], "")
        
        #dictf[dictkey+"_jj"].add(typee["name"])
        dictf[dictkey].add(typee["name"])
        
        if dictkey == "employee":
          if typee["lab"] != "":
            dictf["lab"].add(typee["lab"])
          if typee["affiliation"] not in ["Coriell", ""]:
            dictf["partner"].add(typee["affiliation"])
            
        elif dictkey == "cell_line":
          for key in ["organism", "disease", "disease_category", "cell_type", "tissue"]:
            if key in typee:
              dictf[key].add(typee[key])
                  
        elif dictkey == "disease":
          dictf["disease_category"].add(typee["category"])
    
    
  # FIX
  for search_term in search_terms:
    dictf["reduced_foldername"] = dictf["reduced_foldername"].replace(search_term, "")
    dictf["reduced_jjdescription"] = dictf["reduced_jjdescription"].replace(search_term, "")

  
  #____File parsing____
  file_counts = {}
  file_types = [".zip",".json",".bam",".tsv",".csv",".gz", ".txt"]
  for file_type in file_types:
    fcount = sum(file.endswith(file_type) for root, dirs, files in os.walk(f) for file in files)
    file_counts[file_type + "_counts"] = fcount
  
  dictf.update(file_counts)
  
  #____Make readable____
  for key,value in dictf.items():
    if isinstance(value,set) == True:
      if len(value) == 0:
        dictf[key] = ""
      elif len(value) == 1:
        dictf[key] = list(dictf[key])[0]
      else:
        dictf[key] = ','.join(dictf[key])
        
  dictt.append(dictf)
  
#------------------------
df2 = pd.DataFrame(dictt)

# Fill null type values with JJs
df2["experiment"] = df2["experiment"].fillna(df2["jj_type"])
df2.to_csv("./data/test-app/df_merged.csv", index=False)



