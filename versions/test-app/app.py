# %%
import duckdb
import hvplot.pandas
import numpy as np
import pandas as pd
import panel as pn
con = duckdb.connect("browser_test.db")

# Primary
biomaterial = con.sql("SELECT * from biomaterial").df()
cell = con.sql("SELECT * from cell").df()
cell_line = con.sql("SELECT * from cell_line").df()
disease = con.sql("SELECT * from disease").df()
experiment = con.sql("SELECT * from experiment").df()
folder = con.sql("SELECT * from folder").df()
gene = con.sql("SELECT * from gene").df()
institute = con.sql("SELECT * from institute").df()
lab = con.sql("SELECT * from lab").df()
organism = con.sql("SELECT * from organism").df()
researcher = con.sql("SELECT * from researcher").df()
seqcompany = con.sql("SELECT * from seqcompany").df()
study = con.sql("SELECT * from study").df()
tissue = con.sql("SELECT * from tissue").df()
treatment = con.sql("SELECT * from treatment").df()

# Links
cell_folder = con.sql("SELECT * from cell_folder").df()
cell_line_folder = con.sql("SELECT * from cell_line_folder").df()
experiment_folder = con.sql("SELECT * from experiment_folder").df()
researcher_folder = con.sql("SELECT * from researcher_folder").df()
seqcompany_folder = con.sql("SELECT * from seqcompany_folder").df()
study_folder = con.sql("SELECT * from study_folder").df()
treatment_folder = con.sql("SELECT * from treatment_folder").df()

# Views
v_folder_biomaterial = con.sql("SELECT * from v_folder_biomaterial").df()
v_folder_gene = con.sql("SELECT * from v_folder_gene").df()
v_folder_organism = con.sql("SELECT * from v_folder_organism").df()
v_folder_tissue = con.sql("SELECT * from v_folder_tissue").df()
v_folder_institute = con.sql("SELECT * from v_folder_institute").df()
v_folder_lab = con.sql("SELECT * from v_folder_lab").df()
v_folder_disease = con.sql("SELECT * from v_folder_disease").df()




folder = folder.rename(columns = {"cbix1/2":"Server", "foldername":"Folder Name","jj_description": "Description"})
folder = folder.rename(columns = {col:col.replace("_"," ").title() for col in folder.columns if "uuid" not in col})
biomaterial = biomaterial.rename(columns = {col:col.replace("_"," ").title() for col in biomaterial.columns if "uuid" not in col})
cell = cell.rename(columns ={col:col.replace("_"," ").title() for col in cell.columns if "uuid" not in col})
cell_line = cell_line.rename(columns ={col:col.replace("_"," ").title() for col in cell_line.columns if "uuid" not in col})
disease = disease.rename(columns ={col:col.replace("_"," ").title() for col in disease.columns if "uuid" not in col})
experiment = experiment.rename(columns ={col:col.replace("_"," ").title() for col in experiment.columns if "uuid" not in col})
folder = folder.rename(columns ={col:col.replace("_"," ").title() for col in folder.columns if "uuid" not in col})
gene = gene.rename(columns ={col:col.replace("_"," ").title() for col in gene.columns if "uuid" not in col})
institute = institute.rename(columns ={col:col.replace("_"," ").title() for col in institute.columns if "uuid" not in col})
lab = lab.rename(columns ={col:col.replace("_"," ").title() for col in lab.columns if "uuid" not in col})
organism = organism.rename(columns ={col:col.replace("_"," ").title() for col in organism.columns if "uuid" not in col})
researcher = researcher.rename(columns ={col:col.replace("_"," ").title() for col in researcher.columns if "uuid" not in col})
seqcompany = seqcompany.rename(columns ={col:col.replace("_"," ").title() for col in seqcompany.columns if "uuid" not in col})
study = study.rename(columns ={col:col.replace("_"," ").title() for col in study.columns if "uuid" not in col})
tissue = tissue.rename(columns ={col:col.replace("_"," ").title() for col in tissue.columns if "uuid" not in col})
treatment = treatment.rename(columns ={col:col.replace("_"," ").title() for col in treatment.columns if "uuid" not in col})

v_folder_gene = v_folder_gene.rename(columns ={col:col.replace("_"," ").title() for col in v_folder_gene.columns if "uuid" not in col})
treatment_folder = treatment_folder.rename(columns ={col:col.replace("_"," ").title() for col in treatment_folder.columns if "uuid" not in col})
v_folder_disease = v_folder_disease.rename(columns ={col:col.replace("_"," ").title() for col in v_folder_disease.columns if "uuid" not in col})
experiment_folder = experiment_folder.rename(columns ={col:col.replace("_"," ").title() for col in experiment_folder.columns if "uuid" not in col})

folder["Date"] = pd.to_datetime(folder["Date"]).dt.date

# %%
pn.extension(design="material", sizing_mode="stretch_width")
pn.extension('tabulator')


# %%
template = pn.template.MaterialTemplate(
        site="Coriell",
        title="Research Data Browser"
        )

#-----------------------------------------------------------------
# %% Define field_options

ignore_cols = ["Latin Name", "Experiment Type", "Experiment Name", "Cell Line Category", "Transformant Name Ncbi", "Disease Symbol", 
			   "Gene Symbol", "Clinvar","Oncogenicity Classiciation", "Clinical Impact Classification", "Functional Consequence So", 
			   "Functional Consequence So Name", "Functional Consequence Vario", "Functional Consequence Vario Name"]

def get_field_option(field_name, field_uuid, main_df, view_df):
	field_view = view_df.merge(main_df, on=field_uuid, how="left")
	field_columns = [{"name":col, "dtype":"str"} for col in field_view.columns 
				  	if not any(x in col for x in ["earch ", "uuid", "Id", "source", "Source","reation"])
					and col not in ignore_cols]
	field_option = {
					"field_name":field_name,
					"field_uuid":field_uuid,
					"field_df": main_df,
					"field_view": field_view,
					"row":pn.Column(field_name),
					"field_columns": field_columns
					}
	return field_option

tables = [["Organism", "organism_uuid", organism, v_folder_organism],
		  ["Experiment", "experiment_uuid", experiment, experiment_folder],
		  ["Treatment", "treatment_uuid", treatment, treatment_folder],
		  ["Lab", "lab_uuid", lab, v_folder_lab],
		  ["Researcher", "researcher_uuid", researcher, researcher_folder],
		  ["Cell Line", "cell_line_uuid", cell_line, cell_line_folder],
		  ["Tissue", "tissue_uuid", tissue, v_folder_tissue],
		  ["Cell", "cell_uuid", cell, cell_folder],
		  ["Disease", "disease_uuid", disease, v_folder_disease],
		  ["Gene", "gene_uuid", gene, v_folder_gene],
		  ["Biomaterial", "biomaterial_uuid", biomaterial, v_folder_biomaterial],
		  ["Institute", "institute_uuid", institute, v_folder_institute],
		  ["Sequencing Company", "seqcompany_uuid", seqcompany, seqcompany_folder],
		  ["Study", "study_uuid", study, study_folder]
		  ]

field_options = []
for table in tables:
	field_option = get_field_option(table[0], table[1], table[2], table[3])
	field_options.append(field_option)


#-----------------------------------------------------------------
# For each of these fields, create MultiChoice widgets and a Row to store them
for index1, field_option in enumerate(field_options):

	for index2, column in enumerate(field_option["field_columns"]):

		options = [f for f in field_option["field_view"][column["name"]].unique() if len(str(f))>0]
		mc = pn.widgets.MultiChoice(options=options, name=column["name"])
		field_options[index1]["field_columns"][index2]["mc"] = mc 
		field_options[index1]["row"].append(mc)



# %%
#-----------Test out df-----------
for index, field_option in enumerate(field_options):
	folder = folder.merge(field_option["field_view"][[col for col in field_option["field_view"] if col !="source" and "reation" not in col]], on = "folder_uuid", how="left").fillna('')
	
groupby_cols = ["Date", "Folder Name", "Description", "Server"]
nongroupby_cols = [col for col in folder.columns if col not in groupby_cols]

def groupby_custom(x):
	set_x = set(x)
	if len(set_x)==0:
		return ""
	elif len(set_x) == 1:
		return list(set_x)[0]
	else:
		return ', '.join(list(set_x))

folder = folder.drop_duplicates()
folder2 = folder.groupby(groupby_cols, as_index=False).transform(lambda x: groupby_custom(x))
folder = pd.concat([folder[groupby_cols],folder2], axis=1).drop_duplicates().reset_index(drop=True)
folder.index += 1

df_pane = pn.widgets.Tabulator(folder, pagination = 'local', 
							   page_size=10, sorters= [{'field': 'index', 'dir': 'desc'}],
							   selectable=1, disabled=True)
							   #layout='fit_data_stretch'
		  						#show_index=False
								   
#hidden_cols = [c for c in folder.columns if c not in allowed_cols_1]
#df_pane.hidden_columns = hidden_cols

df_pane.hidden_columns = nongroupby_cols

template.main.append(df_pane)


#-----------------------------------------------------------------
# Create an overall MultiChoice widget to choose the fields themselves
field_mc = pn.widgets.MultiChoice(options=[f["field_name"] for f in field_options], name='Fields')
template.sidebar.append(field_mc)

#-----------------------------------------------------------------
# For each of the chosen fields, go back and find its 
# 		- Row of MultiChoice widgets --> Add to sidebar
# 		- Each MultiChoice widget --> Add as filters to tabler
def get_field_rows(mc_val): 
	rows = pn.Column()
	if len(mc_val)>0:
		for val in mc_val: 
			field_option = [d for d in field_options if d["field_name"] == val][0]
			rows.append(field_option["row"])
	
	return rows

rows = pn.panel(pn.bind(get_field_rows, field_mc))
template.sidebar.append(rows)

#-----------------------------------------------------------------
def contains_filter(df, filter_value, column):
	if len(filter_value) > 0:
		df = df[df.apply(lambda x: any(val in x[column].split(", ") for val in filter_value), axis = 1)].reset_index(drop=True)
		df.index +=1
	return df

def get_current_options(mc_val):
	options = []
	if len(mc_val)>0:
		for val in mc_val: 
			field_option = [d for d in field_options if d["field_name"] == val][0]
			options.append(field_option)
	return options

def apply_current_filters(options):
	for option in options:
		for column in option["field_columns"]:
			new_filter = pn.bind(contains_filter, filter_value = column["mc"], column = column["name"])
			df_pane.add_filter(new_filter)
	return ""

options = pn.bind(get_current_options, field_mc)
filters = pn.bind(apply_current_filters, options)
template.main.append(filters) # For some reason it won't work without calling it to the main template

#-----------------------------------------------------------------
def get_old_options(mc_val):
	options = [option for option in field_options if option["field_name"] not in mc_val]
	return options

def remove_old_filters(old_options):
	for option in old_options:
		for column in option["field_columns"]:
			column["mc"].value = []
	return ""

old_options = pn.bind(get_old_options, field_mc)
old_filters = pn.bind(remove_old_filters, old_options)
template.main.append(old_filters) # For some reason it won't work without calling it to the main template


#-----------------------------------------------------------------
def unhide_current_columns(options, hidden_cols):
	non_hidden = []
	for option in options:
		cols = [col["name"] for col in option["field_columns"]] #if not any(x in col["name"] for x in ["earch ", "uuid", "Id"])]
		non_hidden.extend(cols)

	new_hidden = [col for col in hidden_cols if col not in non_hidden]
	return new_hidden


unhidden_cols =  pn.bind(unhide_current_columns, options, df_pane.hidden_columns)
df_pane.hidden_columns = unhidden_cols

template.servable()






# %%

#-------------------------------TO FIX ---------------------------------
#--------------------------
def get_length(df):
	return str(df.shape[0])#pn.Column(str(df.index.max))

length = pn.bind(get_length, df_pane.current_view)
#template.main.append(pn.Column(length))




#------------------------------ FIXING DATA ---------------------------------
# Check uuids 
for opt in field_options:
	df_uuids =  set(opt["field_df"][opt["field_uuid"]].unique())
	view_uuids = set(opt["field_view"][opt["field_uuid"]].unique())
	diff1 = df_uuids.difference(view_uuids)
	diff2 = view_uuids.difference(df_uuids)
	print(opt["field_name"], len(diff1), len(diff2))

