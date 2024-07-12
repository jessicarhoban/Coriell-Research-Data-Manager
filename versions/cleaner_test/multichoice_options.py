
#%% ______________________PARAMETERS (UNDER 50 VALS)___________________________

# STUDY
# *title                            DONE
# *summary (abstract)               DONE
# *experimental design              DONE
# contributor

# SAMPLES
# *library name (i.e., sample id)   DONE
# *title (i.e., sample name)        DONE
# *organism                         DONE
# **tissue
# **cell line
# **cell type
# genotype
# treatment 
# time 
# strain 
# genetic modification 
# developmental stage 
# age
# sex                               DONE
# disease state
# tumor stage
# ChIP antibody
# *molecule                         DONE
# *single or paired end             DONE
# *instrument model                 DONE
# description                       DONE
# *processed data file
# *raw file

# growth protocol                   DONE
# treatment protocol                DONE
# *extract protocol                 DONE
# *library construction protocol    DONE
# *library strategy (experiment)    DONE              

# *data processing step             
# *genome build/assembly            DONE
# *processed data files format and content

import uuid
import pandas as pd


# GEO -- SAMPLES
organism_options = ["Homo sapiens", "Mus musculus"]

# From GEO metadata template
molecule_options = ['total RNA','polyA RNA','cytoplasmic RNA','nuclear RNA','genomic DNA','protein', 'other']

singlepaired_options = ["single", "paired-end"]

instrumentmodel_options = ['Illumina HiSeq 1000', 'Illumina HiSeq 1500', 'Illumina HiSeq 2000', 
                           'Illumina HiSeq 2500', 'Illumina HiSeq 3000', 'Illumina HiSeq 4000', 
                           'HiSeq X Five', 'HiSeq X Ten', 'Illumina HiScanSQ', 'Illumina iSeq 100', 
                           'Illumina MiSeq', 'Illumina MiniSeq', 'Illumina NextSeq 500', 'NextSeq 550', 
                           'NextSeq 1000', 'NextSeq 2000', 'Illumina NovaSeq 6000', 'Illumina NovaSeq X', 
                           'Illumina NovaSeq X Plus', 'AB 5500 Genetic Analyzer', 'AB 5500xl Genetic Analyzer', 
                           'AB 5500xl-W Genetic Analysis System', 'BGISEQ-500', 'DNBSEQ-G400', 'DNBSEQ-G400 FAST', 
                           'DNBSEQ-G50', 'DNBSEQ-T7', 'MGISEQ-2000RS', 'GridION', 'MinION', 'PromethION', 
                           'Ion GeneStudio S5', 'Ion GeneStudio S5 plus', 'Ion GeneStudio S5 prime', 
                           'Ion Torrent Genexus', 'Ion Torrent PGM', 'Ion Torrent Proton', 'Ion Torrent S5', 
                           'Ion Torrent S5 XL', 'PacBio RS', 'PacBio RS II', 'Sequel', 'Sequel II', 
                           'Sequel IIe', 'Complete Genomics', 'Element AVITI', 'FASTASeq 300', 'GenoCare 1600', 
                           'GenoLab M', 'GS111', 'Helicos HeliScope', 'Onso', 'Revio', 
                           'Sentosa SQ301', 'Tapestri', 'UG 100']

# From cellosaurus
sex_options = ['Sex unspecified', 'Sex ambiguous', 'Male', 'Mixed sex', 'Female']

# From human cell atlas
biomaterial_options = ["Cell line", "Cell suspension", "Organoid", "Specimen from organism"]

# From cellosaurus
knockout_options = ['KO mouse', 'Recombinant Adeno-Associated Virus', 'Not specified', 'Homologous recombination'
                    'EBV-based vector siRNA knockdown', 'Spontaneous mutation', 'TALEN', 'Floxing/Cre recombination',
                    'Gene-targeted KO mouse', 'Promoterless gene targeting', 'ZFN', 'CRISPR/Cas9n', 
                    'CRISPR/Cas9', 'Gene trap', 'BAC homologous recombination', 'Gamma radiation', 
                    'siRNA knockdown', 'Knockout-first conditional', 'shRNA knockdown', 'Cre/loxP']

# From cellosaurus
cellline_category_options = ['Hybridoma', 'Conditionally immortalized cell line', 'Cancer cell line', 
                            'Hybrid cell line', 'Transformed cell line', 'Spontaneously immortalized cell line', 
                            'Stromal cell line', 'Finite cell line', 'Embryonic stem cell', 
                            'Induced pluripotent stem cell', 'Telomerase immortalized cell line', 
                            'Somatic stem cell', 'Factor-dependent cell line', 'Undefined cell line type']

# From UCSC
genome_build_options = ["hs1 (T2T Consortium CHM13v2.0)", "hg38 (Genome Reference Consortium GRCh38)",
                        "hg19 (Genome Reference Consortium GRCh37)","hg18 (NCBI Build 36.1)","hg17 (NCBI Build 35)",
                        "hg16 (NCBI Build 34)","hg15 (NCBI Build 33)","hg13 (NCBI Build 31)","hg12 (NCBI Build 30)",
                        "hg11 (NCBI Build 29)","hg10 (NCBI Build 28)","hg8 (UCSC-assembled)""hg7 (UCSC-assembled)",
                        "hg6 (UCSC-assembled)","hg5 (UCSC-assembled)","hg4 (UCSC-assembled)","hg3 (UCSC-assembled)",
                        "hg2 (UCSC-assembled)","hg1 (UCSC-assembled)","mm39 (Genome Reference Consortium Mouse Build 39)",
                        "mm10 (Genome Reference Consortium GRCm38)","mm9 (NCBI Build 37)","mm8 (NCBI Build 36)",
                        "mm7 (NCBI Build 35)","mm6 (NCBI Build 34)","mm5 (NCBI Build 33)","mm4 (NCBI Build 32)",
                        "mm3 (NCBI Build 30)","mm2 (MGSCv3)","mm1 (MGSCv2)"]

library_strategy_options = ['RNA-Seq', 'scRNA-seq', 'miRNA-Seq', 'ncRNA-Seq', 'ChIP-Seq', 
                            'ATAC-seq', 'Bisulfite-Seq', 'Bisulfite-Seq (reduced representation)', 
                            'Spatial Transcriptomics', 'CITE-seq', 'CUT&Run', 'CUT&Tag', 'MNase-Seq', 
                            'Hi-C', 'MBD-Seq', 'MRE-Seq', 'MeDIP-Seq', 'DNase-Hypersensitivity', 'Tn-Seq', 
                            'FAIRE-seq', 'SELEX', 'RIP-Seq', 'ChIA-PET', 'BRU-Seq', 'CRISPR Screen', 
                            'Capture-C', 'ChEC-seq', 'ChIRP-seq', 'DamID-Seq', 'EM-seq', 'GRO-Seq', 
                            'HiChIP', 'MeRIP-Seq', 'PRO-Seq', 'RNA methylation', 'Ribo-Seq', 'TCR-Seq', 
                            'BCR-Seq', 'iCLIP', 'smallRNA-Seq', 'snRNA-Seq', 'ssRNA-Seq', 'RNA-Seq (CAGE)', 
                            'RNA-Seq (RACE)', '16S rRNA-seq', '4C-Seq', 
                            "OTHER: WGS", "OTHER: snATAC-Seq", "OTHER: scATAC-Seq", "OTHER: snRNA-Seq", "OTHER: DREAM"] # Jess added

#---------------------------------------------------------------------------------------
lab_options = ['The Issa & Jelenik Lab', 'The Jian Huang Lab', 'The Scheinfeldt Lab',
               'The Nora Engel Lab', 'The Luke Chen Lab', 'The Shumei Song Lab']

institute_options = ['GSK',
                    'National Institute of Neurological Disorders and Stroke (NINDS)',
                    'The Wistar Institute', 'Camden Opioid Research Initiative (CORI)',
                    'Coriell Institute for Medical Research',
                    'Harvard Stem Cell Institue', 'Cooper University Hospital (CUH)',
                    'Fels Institute for Cancer Research and Molecular Biology',
                    'Memorial Sloan Kettering Cancer Center (MSK)',
                    'Coriell Personalized Medicine Collaborative']

study_options =[ 'Optimizing Pain Treatment In New Jersey (OPTIN) Study',
                  'Genomics of Opioid Addiction Longitudinal Study (GOALS) Study']



iters = {"organism":organism_options, 
         "molecule":molecule_options, 
         "single_paired":singlepaired_options,
         "instrument_model":instrumentmodel_options,
         "sex":sex_options,
         "biomaterial":biomaterial_options,
         "knockout":knockout_options,
         "cellline_category":cellline_category_options,
         "genome_build":genome_build_options,
         "library_strategy":library_strategy_options,
         "lab":lab_options,
         "institute":institute_options,
         "study":study_options}

iter_df = pd.DataFrame()

for type, values in iters.items():
    add_df = pd.DataFrame({"parameter_name":values, 
                           "parameter_type":type, 
                           "parameter_uuid":[uuid.uuid4() for i in range(0,len(values))]})
    iter_df = pd.concat([iter_df,add_df], ignore_index=True)


#%% ______________________GETTING PARAMETERS (ARCHIVE)___________________________
# %% MOLECULE OPTIONS -- 7
#molecule_options = ['total RNA','polyA RNA','cytoplasmic RNA',
#                    'nuclear RNA','genomic DNA','protein', 'other']
#molecule_df = pd.DataFrame(molecule_options, columns=["molecule_name"])

# %% CATEGORY OPTIONS -- 14
#url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=ca&rows=500000"
#response  = requests.get(url)
#category_opts =  set([t.strip() for t in response.text.split("\n") if t.startswith("CA")])

#url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=ca&rows=500000"
#response2  = requests.get(url2)
#category_opts2 =  set([t.strip() for t in response2.text.split("\n") if t.startswith("CA")])

#category_opts.update(category_opts2)
#category_df = pd.DataFrame(category_opts, columns=["category_text"])
#category_df["category_name"] = category_df["category_text"].apply(lambda x: x.replace("CA ",""))
#category_df = category_df[["category_name"]]

# %% SEX OPTIONS -- 5
#url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=sx&rows=500000"
#response  = requests.get(url)
#sex_opts =  set([t.strip() for t in response.text.split("\n") if t.startswith("SX")])

#url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=sx&rows=500000"
#response2  = requests.get(url2)
#sex_opts2 =  set([t.strip() for t in response2.text.split("\n") if t.startswith("SX")])

#sex_opts.update(sex_opts2)
#sex_df = pd.DataFrame(sex_opts, columns=["sex_text"])
#sex_df["sex_name"] = sex_df["sex_text"].apply(lambda x: x.replace("SX ",""))
#sex_df = sex_df[["sex_name"]]
# %% KNOCKOUT OPTIONS -- 20