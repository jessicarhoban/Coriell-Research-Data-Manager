# %%
import duckdb
#import hvplot.pandas
import numpy as np
import pandas as pd
import requests
import time
import panel as pn
con = duckdb.connect("browser_test.db")

pn.extension(design="material", sizing_mode="stretch_width")
pn.extension('tabulator')
pn.extension('mathjax')

# %%
template = pn.template.MaterialTemplate(
        site="Coriell",
        title="Submission Form (Research Data Browser)",
        sidebar=pn.Column()
        )


#---------------------------------------------------------OVERALL DEFINITIONS---------------------------------------------------------
def get_clean_duckdb_options(table_name, field_name, field_uuid, filter=""):
    sql_command = "SELECT distinct " + field_name + "," + field_uuid + " FROM " + table_name + filter
    sql_result = con.sql(sql_command).df()
    dict_result = {str(result[field_name]):result[field_uuid] for _,result in sql_result.iterrows()}
    return dict_result

def get_clean_duckdb_values(table_view, table_name, field_name, field_uuid, folder_mc1):
    sql_command = "SELECT distinct " + field_name + "," + field_uuid + " FROM " + table_name
    sql_result = con.sql(sql_command).df()

    for val in folder_mc1:
        sql_command2 = "SELECT distinct " + field_uuid  + ",folder_uuid " + " FROM " + table_view 
        sql_result2 = con.sql(sql_command2).df()
        sql_result2 = sql_result2.loc[sql_result2["folder_uuid"] == str(val)]
        filtered_result = sql_result.merge(sql_result2, on=field_uuid)
        if table_name == "gene":
            result = [str(result["gene_id_ncbi"]) for _,result in filtered_result.iterrows()]
        else:
            result = [result[field_uuid] for _,result in filtered_result.iterrows()]
        return result


def update_database(value_list, table_view_editable, table_view, table_uuid, folder_mc1, 
                    is_searchable=False, searchable_field="", table_name = ""):
    # Simple example, where the uuid is already in there (i.e. lab)
    commands = []
    for val in folder_mc1: 
        
        if is_searchable == False:
            get_command =  ["SELECT distinct " + table_uuid  + ",folder_uuid " + " FROM " + table_view]
            get_command = "".join(get_command)
            get_result = con.sql(get_command).df()
            get_result = get_result.loc[get_result["folder_uuid"] == str(val)]
            
            current_ids = set(get_result[table_uuid].unique())
            add_ids = set(value_list).difference(current_ids)
            delete_ids = current_ids.difference(set(value_list))

        elif is_searchable == True:

            #--------------------------------------------------------
            # Get all current options based on the "searchable_field" {searchable_field:table_uuid}
            options = get_clean_duckdb_options(table_name, searchable_field, table_uuid)
            # Find where a new searchable field was chosen
            new_ids = [v for v in value_list if v not in options.keys()]
            
            for new_id in new_ids:
                insert_command = ["INSERT INTO ", table_name, " (", searchable_field, ",", table_uuid, ") values ",
                                  "(", str(new_id), ", uuid() )"]
                insert_command = "".join(insert_command)
                con.execute(insert_command)

            #--------------------------------------------------------
            get_command =  ["SELECT distinct " + table_uuid  + ",folder_uuid " + " FROM " + table_view]
            get_command = "".join(get_command)
            get_result = con.sql(get_command).df()
            get_result = get_result.loc[get_result["folder_uuid"] == str(val)]

            current_ids = set(get_result[table_uuid].unique())
            options = get_clean_duckdb_options(table_name, searchable_field, table_uuid)
            value_uuid_list = [options[v] for v in value_list]

            add_ids = set(value_uuid_list).difference(current_ids)
            delete_ids = current_ids.difference(set(value_uuid_list))


        for add_id in add_ids:
            insert_command = ["INSERT INTO ",  table_view_editable ," (", table_uuid, ", folder_uuid)",
                                " values ('", str(add_id) , "','", str(val) , "')"]
            insert_command = "".join(insert_command)
            commands.append(insert_command)
            # 
            if is_searchable == True:
                con.execute(insert_command)
            

        for delete_id in delete_ids:
            delete_command = ["DELETE FROM ",  table_view_editable , " WHERE ", table_uuid, " = '", delete_id,
                              "' AND folder_uuid = '", val, "'"]
            delete_command = "".join(delete_command)
            commands.append(delete_command)
            if is_searchable == True:
                con.execute(delete_command)

    return commands



def get_tabs(tables, header, folder_mc1):
    tab = pn.Column()
    tab.append(header)
    for _,table in enumerate(tables):
        table_row = pn.Column(pn.pane.Markdown("### "+table["table_title"]))
        for col_name,col_title in table["columns"].items():
            options = get_clean_duckdb_options(table["table_name"], col_name, table["table_uuid"])
            values = get_clean_duckdb_values(table["table_view"], table["table_name"], col_name, table["table_uuid"],folder_mc1)
            mc = pn.widgets.MultiChoice(name=col_title, options=options, value=values)
            commands = pn.bind(update_database, mc, table["table_view_editable"],table["table_view"], table["table_uuid"], folder_mc1, is_searchable=False)
            pn.bind(save_changes, save_button, commands, watch=True)
            table["mc"] = mc
            table_row.append(pn.Row(pn.Spacer(width=30),mc))
            
        tab.append(table_row)
    
    return tab


def save_changes(event, commands):
    if not event:
        return
    for command in commands:
        con.execute(command)




#---------------------------------------------------------COLLABORATORS TAB---------------------------------------------------------
collaborators_tables = [
    {"table_view_editable":"lab_folder", "table_view": "v_folder_lab", "table_title":"Lab", "table_name":"lab", "table_uuid": "lab_uuid", "columns":{"lab_name":"Lab Name"}},
    {"table_view_editable":"researcher_folder","table_view": "researcher_folder", "table_title":"Researcher", "table_name":"researcher", "table_uuid": "researcher_uuid", "columns":{"researcher_name":"Researcher Name"}},
    {"table_view_editable":"institute_folder",  "table_view": "v_folder_institute", "table_title":"Institute", "table_name":"institute", "table_uuid": "institute_uuid", "columns":{"institute_name":"Institute Name"}},
    {"table_view_editable":"study_folder", "table_view": "study_folder", "table_title":"Study", "table_name":"study", "table_uuid": "study_uuid", "columns":{"study_name":"Study Name"}}
]

collaborators_header = pn.pane.Markdown("""Test Title""")

#---------------------------------------------------------STUDY DESIGN TAB---------------------------------------------------------
studydesign_tables = [
    {"table_view_editable":"experiment_folder", "table_view": "experiment_folder", "table_title":"Experiment", "table_name":"experiment", "table_uuid": "experiment_uuid", "columns":{"experiment_name":"Experiment Name"}},
    {"table_view_editable":"treatment_folder", "table_view": "treatment_folder", "table_title":"Treatment", "table_name":"treatment", "table_uuid": "treatment_uuid", "columns":{"treatment_name":"Treatment Name"}},
    {"table_view_editable":"seqcompany_folder", "table_view": "seqcompany_folder", "table_title":"Sequencing Company", "table_name":"seqcompany", "table_uuid": "seqcompany_uuid", "columns":{"seqcompany_name":"Sequencing Company Name"}}
]

studydesign_header = pn.pane.Markdown("""Test Title""")

#----------------------------------------------------------BIOMATERIALS TAB---------------------------------------------------------
biomaterial_tables = [
    {"table_view_editable":"biomaterial_folder", "table_view": "v_folder_biomaterial", "table_title":"Biomaterial", "table_name":"biomaterial", "table_uuid": "biomaterial_uuid", "columns":{"biomaterial_name":"Biomaterial Name","biomaterial_type":"Biomaterial Type","biomaterial_subtype":"Biomaterial Subtype"}},
    {"table_view_editable":"organism_folder", "table_view": "v_folder_organism", "table_title":"Organism", "table_name":"organism", "table_uuid": "organism_uuid", "columns":{"organism_name":"Organism Name"}},
    {"table_view_editable":"cell_line_folder", "table_view": "cell_line_folder", "table_title":"Cell Line", "table_name":"cell_line", "table_uuid": "cell_line_uuid", "columns":{"cell_line_name":"Cell Line Name"}},
    {"table_view_editable":"cell_folder", "table_view": "cell_folder", "table_title":"Cell", "table_name":"cell", "table_uuid": "cell_uuid", "columns":{"cell_name":"Cell Name"}},
    {"table_view_editable":"tissue_folder", "table_view": "v_folder_tissue", "table_title":"Tissue", "table_name":"tissue", "table_uuid": "tissue_uuid", "columns":{"tissue_name":"Tissue Name"}},
    {"table_view_editable":"disease_folder", "table_view": "v_folder_disease", "table_title":"Disease", "table_name":"disease", "table_uuid": "disease_uuid", "columns":{"disease_name":"Disease Name"}}
]

biomaterial_header = pn.pane.Markdown("""Test Title""")

#---------------------------------------------------------GENE TAB---------------------------------------------------------
# Definitions
def get_clean_genes(options1, is_value):
    
    if is_value == 0:
        clean_options = {}

    elif is_value == 1:
        clean_options = []

    for n in options1:
        name = "("+n["name"] + ") " + n["description"].title() + " [" + n["organism"] +"]" 
        uuid = str(n["uid"])
        
        if is_value == 0:
            clean_options[name] = uuid

        else:
            if n["is_value"] == 1:
                clean_options.append(uuid)

    return clean_options

#---------------------------------------------------------------
def get_ncbi_gene_ids(search):
    search_text =  '"' + search + '"[Gene/Protein Name]'
    search_params = {"db":"gene", "retmax":"30", "retmode":"json", "term":search_text}
    url_base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?&"
    for param,value in search_params.items():
        url_base += "&"
        url_base += param
        url_base += "="
        url_base += value
    
    response = requests.get(url_base)
    if "esearchresult" in response.json().keys():
        idfiltered = [i for i in response.json()["esearchresult"]["idlist"]]
        idlist = ",".join(idfiltered)
        return idlist
    
    else:
        return ""
                
#---------------------------------------------------------------
def get_ncbi_gene_info(idlist, organisms, ret_max='10', is_value=0, filter_uids=[]):
    
    # Initialize empty values
    all_genes = []

    url3 = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=gene&retmode=json&retmax=' + ret_max + '&id=' + idlist
    response3  = requests.get(url3)

    if "result" in response3.json().keys():
        uids1 = [i for i in response3.json()["result"]["uids"]]

        if len(filter_uids)>0:
            uids1 = [u for u in uids1 if u not in filter_uids]

        genes = [{"name":response3.json()["result"][j]["name"] ,
                    "description":response3.json()["result"][j]["description"],
                    "organism":response3.json()["result"][j]["organism"]["scientificname"],
                    "uid":response3.json()["result"][j]["uid"],
                    "is_value":is_value,
                    "index":i}
                    for i,j in enumerate(uids1)
                    if response3.json()["result"][j]["organism"]["scientificname"] in organisms]
            
        for gene in genes:
            all_genes.append(gene) 
    
    return all_genes

#---------------------------------------------------------------
def get_genes(gene_text, gene_checkbox, folder_mc1):
   
    # Filter for organism checkbox
    organisms = []
    for val in gene_checkbox:
        if val == "Human":
            organisms.append("Homo sapiens")
        elif val == "Mouse":
            organisms.append("Mus musculus")

    if len(gene_checkbox) == 0:
        organisms = ["Homo sapiens", "Mus musculus"]
        #gene_checkbox.value.append("Human")
    
    #--------------------------------
    # Append ones already there 
    values = get_clean_duckdb_values("v_folder_gene", "gene", "gene_id_ncbi", "gene_uuid", folder_mc1)
    
    all_genes = []
    filter_genes = []

    if len(values)>0:
        idlist =  ",".join(values)
        current_genes = get_ncbi_gene_info(idlist, organisms=["Homo sapiens", "Mus musculus"], is_value=1)
        filter_genes.extend([c["uid"] for c in current_genes])
        all_genes.extend(current_genes)
        time.sleep(0.5) 
    
    # If they searched for something
    if len(gene_text)>0:
        distinct_searches = gene_text.split(",")
        for search in distinct_searches:
            idlist = get_ncbi_gene_ids(search)
            if len(idlist)>0:
                new_genes = get_ncbi_gene_info(idlist, organisms, filter_uids = filter_genes)
                all_genes.extend(new_genes)
                time.sleep(0.5) 
        #all_genes = sorted(all_genes, key=lambda x: x['index'])
    
    else:
        options = list(get_clean_duckdb_options("gene", "gene_id_ncbi", "gene_uuid").keys())
        if len(options)>0:
            options = [o for o in options if o not in values]
            idlist =  ",".join(options)
            current_options = get_ncbi_gene_info(idlist, organisms=["Homo sapiens", "Mus musculus"], ret_max=str(len(options)))
            all_genes.extend(current_options)
            time.sleep(0.5) 
        
    #all_genes = sorted(all_genes, key=lambda x: x['name'])

    return all_genes

#-----------------------
def get_gene_tab(folder_mc1):

    gene_tab = pn.Column()
    gene_header = pn.pane.Markdown("""Note: Please save after every distinct gene search.""")
    gene_tab.append(gene_header)
    gene_row = pn.Column(pn.pane.Markdown("### Gene"))
    gene_checkbox = pn.widgets.CheckBoxGroup( options=["Human", "Mouse"], value=["Human", "Mouse"], inline=True, align=('center', 'center'))             
    gene_text = pn.widgets.TextInput(placeholder="Search for gene name here:",name='Gene Search')
    gene_name_mc = pn.widgets.MultiChoice(options={},name="Gene Name Options", option_limit=200)
    gene_row.append(pn.Row(pn.Spacer(width=30),gene_name_mc))
    gene_row.append(pn.Row(pn.Spacer(height=35)))
    gene_row.append(pn.Row(pn.Spacer(width=30), 
                        pn.pane.Markdown("#### If the gene you need is not in the options above, use this field to search NCBI for it.",
                                         width=500,align=('center', 'center'))))
    gene_row.append(pn.Row(pn.Spacer(width=30), 
                        pn.pane.Markdown("<i>Which organism to search genes for?</i>",width=200,align=('center', 'center')), 
                        pn.Spacer(width=5), 
                        gene_checkbox))
    gene_row.append(pn.Row(pn.Spacer(width=30),gene_text))

    gene_name_options = pn.bind(get_genes, gene_text, gene_checkbox, folder_mc1)
    clean_gene_name_options = pn.bind(get_clean_genes, gene_name_options, is_value=0)
    gene_name_mc.options = clean_gene_name_options

    clean_gene_name_values = pn.bind(get_clean_genes, gene_name_options, is_value=1)
    gene_name_mc.value = clean_gene_name_values
    
    commands = pn.bind(update_database, gene_name_mc, "gene_folder","v_folder_gene", "gene_uuid", folder_mc1, 
                       is_searchable=True, searchable_field="gene_id_ncbi", table_name = "gene")
    pn.bind(save_changes, save_button, commands, watch=True)

    gene_tab.append(gene_row)
    return gene_tab
    

#---------------------------------------------------------SAVE BUTTON---------------------------------------------------------
save_button = pn.widgets.Button(name='Save', button_type='primary', visible=False)
template.main.append(pn.Spacer(height=35))
template.main.append(save_button)
template.main.append(pn.Spacer(height=35))

#---------------------------------------------------------USERNAME SIDEBAR---------------------------------------------------------
username_input = pn.widgets.TextInput(name="Username", placeholder="Enter your username here...")
template.sidebar.append(username_input)

#---------------------------------------------------------FOLDERS SIDEBAR---------------------------------------------------------
folder_tab = pn.Column()
folder_header = pn.pane.Markdown("""Test Title""")
folder_row = pn.Column(pn.pane.Markdown("### Folder"))

folder_mc = pn.widgets.MultiChoice(name="Folder Name", max_items=1, option_limit=200, visible=False)
folder_row.append(pn.Row(pn.Spacer(width=30),folder_mc))
folder_tab.append(folder_header)
folder_tab.append(folder_row)

def update_with_username(username_input):            

    users = con.sql("SELECT * FROM user").df()

    if len(username_input)>0:

        if username_input in users["user_username"].unique():
            
            user_row = users.loc[users["user_username"] == username_input].to_dict('records')[0]
            folder_mc.visible = True

            if user_row["user_role"] == "super_user":
                folder_options = get_clean_duckdb_options("folder", "foldername", "folder_uuid", " where is_root = 1")
                folder_mc.options = folder_options
            else:
                folder_commands = ["SELECT f.foldername, f.folder_uuid",
                                   " FROM user_researcher ur",
                                   " join researcher_folder rf on ur.researcher_uuid = rf.researcher_uuid  ",
                                   " join folder f on f.folder_uuid = rf.folder_uuid ",
                                   " WHERE ur.user_uuid = '", user_row["user_uuid"],
                                   "' and f.is_root = 1"]
                folder_command = "".join(folder_commands)
                folder_options = con.sql(folder_command).df()
                folder_options = {str(result[f"foldername"]):result["folder_uuid"] for _,result in folder_options.iterrows()}
                folder_mc.options = folder_options
        else:
            folder_mc.visible = False
        
        return folder_tab

sidebar = pn.bind(update_with_username,username_input)
template.sidebar.append(sidebar)

#-----------------------------------------------------APPEND ALL TABS-------------------------------------------------------
def update_with_folderselection(folder_mc):            
    
    tabs = pn.Tabs()

    if len(folder_mc) > 0:
        collaborators_tab = get_tabs(collaborators_tables, collaborators_header, folder_mc)
        tabs.append(("Collaborators",collaborators_tab))
        studydesign_tab = get_tabs(studydesign_tables, studydesign_header, folder_mc)
        tabs.append(("Study Design", studydesign_tab))
        biomaterial_tab = get_tabs(biomaterial_tables, biomaterial_header, folder_mc)
        tabs.append(("Biomaterial", biomaterial_tab))
        gene_tab = get_gene_tab(folder_mc)
        tabs.append(("Gene", gene_tab))

        save_button.visible = True
        return tabs

    else:
        return "No folder selected"

      
main = pn.bind(update_with_folderselection, folder_mc)
template.main.append(main)




#-----------------------------------------------------UPDATE DATABASE WITH VALUES-------------------------------------------------------

template.servable()

# %%
