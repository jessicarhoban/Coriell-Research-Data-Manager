# %% INITIAL SETUP

# Import required libraries
import pandas as pd
import requests
import time

# Import panel library and extensions
import panel as pn
pn.extension(design="material", sizing_mode="stretch_width")
pn.extension('tabulator')
pn.extension('tabulator-tables')
pn.extension('mathjax')

# Import and establish database connection
import duckdb
con = duckdb.connect("./database/browser_test.db")

# Import custom functions
import functions.sql_functions as sqlfunc
import functions.api_functions as apifunc
import functions.searchtab_functions as searchfunc


def append_samples(mc, mc_options, folder_mc1):
    
    row_width = 210 + (len(mc)*170)
    samples_row = pn.Row(pn.Spacer(width=210), width=row_width)

    if len(mc)>0:
        for mc in mc:
            mc_dict = [k for k,v in mc_options.items() if v==mc][0]
            
            mc_samples = {}
            for val in folder_mc1:
                command = "".join(["select distinct s.sample_uuid, s.sample_name from sample s join sample_folder sf on sf.sample_uuid = s.sample_uuid ",
                                    "where sf.folder_uuid = '", str(val) ,"'"])
                sample_df = con.sql(command).df()
                for _, row in sample_df.iterrows():
                    mc_samples[row["sample_name"]] = row["sample_uuid"]
            
            title = pn.pane.Markdown(mc_dict, width = 150)
            mc_checkbox = pn.widgets.Checkbox(name = "All samples", width = 150)
            mc_samples = pn.widgets.MultiChoice(options = mc_samples, width = 150)

            samples_row.append(pn.Column(title,mc_checkbox,mc_samples))
            #samples_row.append()
            #samples_row.append(mc_checkbox)

    return samples_row

#---------------------------------------------------------CREATE TABS---------------------------------------------------------
def get_nonsearchable_tab(tab_layout, tab_name, header, folder_mc1):
    tab = pn.Column(header)
    tables = tab_layout.loc[tab_layout["tab_name"]==tab_name]
    for _,table in tables.iterrows():
        table_row = pn.Column(pn.pane.Markdown("### "+table["table_title"]))
        key_list = [k for k in table["columns"]["key"]]
        value_list = [k for k in table["columns"]["value"]]
        columns = dict(zip(key_list,value_list))
        for col_name,col_title in columns.items():
            
            options = sqlfunc.get_clean_options(table["table_name"], col_name, table["table_uuid"])
            values = sqlfunc.get_clean_values(table["table_view"], table["table_name"], col_name, table["table_uuid"],folder_mc1)
            mc = pn.widgets.MultiChoice(name=col_title, options=options, value=values)
            update_commands = pn.bind(sqlfunc.get_update_commands, mc, table["table_view_editable"],table["table_view"], table["table_uuid"], folder_mc1, is_searchable=False)
            pn.bind(sqlfunc.execute_update_commands, save_button, update_commands, watch=True)
            table["mc"] = mc
            
            samples_row = pn.bind(append_samples,mc, mc.options, folder_mc1)
            table_row.append(pn.Row(pn.Spacer(width=30),mc))
            table_row.append(samples_row)
            
        tab.append(table_row)
    
    return tab


#--------------------------------------------------------
def get_searchable_tab(folder_mc1, header, title, searchwebsite, 
                       get_options_function, get_clean_options_function, 
                       checkbox_options="None", checkbox_text = "None"):

    if checkbox_options != "None":
        checkbox = pn.widgets.CheckBoxGroup(options=checkbox_options, value=checkbox_options, inline=True, align=('center', 'center'))  
        checkbox_row = pn.Row(pn.Spacer(width=30),
                           pn.pane.Markdown(checkbox_text,width=200,align=('center', 'center')),
                           pn.Spacer(width=5),
                           checkbox)
    else:
        checkbox = "None"
        checkbox_row = pn.Row()

    textinput = pn.widgets.TextInput(placeholder="Search for " + title.lower() + " name here:",name=title+' Search')
    mc = pn.widgets.MultiChoice(options = {} ,name = title + " Options", option_limit = 200)

    options = pn.bind(get_options_function, textinput, checkbox, folder_mc1)
    clean_options = pn.bind(get_clean_options_function, options, is_value=0)
    mc.options = clean_options

    clean_values = pn.bind(get_clean_options_function, options, is_value=1)
    mc.value = clean_values
    
    update_commands = pn.bind(sqlfunc.get_update_commands, mc, "gene_folder","v_folder_gene", "gene_uuid", folder_mc1, 
                       is_searchable=True, searchable_field="gene_id_ncbi", table_name = "gene")
    pn.bind(sqlfunc.execute_update_commands, save_button, update_commands, watch=True)

    tab = pn.Column(header,
                    pn.pane.Markdown("### "+ title),
                    pn.Row(pn.Spacer(width=30),mc),
                    pn.Row(pn.Spacer(height=35)),
                    pn.Row(pn.Spacer(width=30),
                           pn.pane.Markdown("#### If the "+ title.lower() +
                                            " you need is not in the options above, use this field to search " + 
                                            searchwebsite + " for it.",
                                            width=500,align=('center', 'center'))),
                    checkbox_row,
                    pn.Row(pn.Spacer(width=30),textinput))
    return tab


#---------------------------------------------------------UPDATE TABS---------------------------------------------------------
def update_with_username(username_input):            

    folder_tab = pn.Column(pn.pane.Markdown("### Folder"),
                           pn.Row(pn.Spacer(width=30),folder_mc)
                       )
    
    users = con.sql("SELECT * FROM user").df()

    if len(username_input)>0:

        if username_input in users["user_username"].unique():
            
            user_row = users.loc[users["user_username"] == username_input].to_dict('records')[0]
            folder_mc.visible = True

            if user_row["user_role"] == "super_user":
                folder_options = sqlfunc.get_clean_options("folder", "foldername", "folder_uuid", " where is_root = 1")
                folder_mc.options = folder_options
            else:
                folder_options = sqlfunc.get_user_folders(user_row["user_uuid"])
                folder_mc.options = folder_options
        else:
            folder_mc.visible = False
        
        return folder_tab

#--------------------------------------------------------
def update_with_folderselection(folder_mc):            
        
    tabs = pn.Tabs()

    if len(folder_mc) > 0:
        biomaterial_header = pn.pane.Markdown("""Test Title""")
        collaborators_header = pn.pane.Markdown("""Test Title""")
        studydesign_header = pn.pane.Markdown("""Test Title""")
        gene_header = pn.pane.Markdown("""Note: Gene selections are auto-saved.""")
        tab_layout = con.sql("SELECT * FROM tab_layout").df()

        collaborators_tab = get_nonsearchable_tab(tab_layout, "Collaborators", collaborators_header, folder_mc)
        tabs.append(("Collaborators",collaborators_tab))
        studydesign_tab = get_nonsearchable_tab(tab_layout, "Study Design", studydesign_header, folder_mc)
        tabs.append(("Study Design", studydesign_tab))
        biomaterial_tab = get_nonsearchable_tab(tab_layout, "Biomaterial", biomaterial_header, folder_mc)
        tabs.append(("Biomaterial", biomaterial_tab))
        gene_tab = get_searchable_tab(folder_mc, gene_header, "Gene", searchwebsite="NCBI", 
                                      checkbox_options=["Human", "Mouse"],checkbox_text="<i>Which organism to search genes for?</i>",
                                      get_options_function = searchfunc.get_genes, get_clean_options_function=searchfunc.get_clean_genes)
        tabs.append(("Gene", gene_tab))

        save_button.visible = True
        return tabs

    else:
        return "No folder selected"


#--------------------------------------------------------

def get_sample_tab(folder_mc):

    if len(folder_mc)>0:

        ch1 = pn.widgets.Checkbox(name="SOM", width = 210)
        ch1mc = pn.widgets.MultiChoice(options=[1,2], width = 200, visible=False)

        sample_tab = pn.Column(
                        pn.Row(
                            pn.Spacer(width = 200),
                            ch1,
                            pn.widgets.Checkbox(name="SOM", width = 210)),
                        pn.Row(
                            pn.Spacer(width = 200),
                            pn.Row(ch1mc, height=20)),
                        pn.Row(pn.Spacer(height=50))
        )
        
        def get_mc(ch1):
            if ch1==True:
                ch1mc.visible = True
            else:
                ch1mc.visible = False

        temp = pn.bind(get_mc, ch1)
        template.main.append(temp)

        for val in folder_mc:
            command = "".join(["select distinct s.sample_uuid, s.sample_name from sample s join sample_folder sf on sf.sample_uuid = s.sample_uuid ",
                                            "join folder f on f.folder_uuid = sf.folder_uuid ",
                                            "join file_sample_folder fsf on fsf.sample_uuid = s.sample_uuid ",
                                            "join file fi on fi.file_uuid = fsf.file_uuid ", 
                                            "where f.folder_uuid = '", str(val) ,"'"])
            
            sample_folder = con.sql(command).df()
            
            for _,sample in sample_folder.iterrows():
                
                row = pn.Row(pn.pane.Markdown(sample["sample_name"], width = 200),
                            pn.widgets.MultiChoice(options=[1,2], width = 200),
                            pn.widgets.MultiChoice(options=[1,2], width = 200))
                
                sample_tab.append(row)

        return sample_tab




#---------------------------------------------------------POPULATE TEMPLATE---------------------------------------------------------
# %% CREATE PANEL COMPONENTS
save_button = pn.widgets.Button(name='Save', button_type='primary', visible=False)
username_input = pn.widgets.TextInput(name="Username", placeholder="Enter your username here...")
folder_mc = pn.widgets.MultiChoice(name="Folder Name", max_items=1, option_limit=200, visible=False, placeholder="Please select a folder to edit...")

folder_tab = pn.bind(update_with_username, username_input)
main_tab = pn.bind(update_with_folderselection, folder_mc)
sample_tab = pn.bind(get_sample_tab, folder_mc)

template = pn.template.MaterialTemplate(
        site="Coriell",
        title="Submission Form (Research Data Browser)",
        sidebar=pn.Column(username_input,
                          folder_tab),
        main = pn.Column(pn.Spacer(height=35),
                         save_button,
                         pn.Spacer(height=35),
                         main_tab))
                         #sample_tab))







template.servable()


