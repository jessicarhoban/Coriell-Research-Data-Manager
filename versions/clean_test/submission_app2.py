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
import functions.panel_functions as panelfunc
import parameters.multichoice_options as multioptions


#---------------------------------------------------------UPDATE TABS---------------------------------------------------------
def update_folder_by_username(username_input):            

    folder_tab = pn.Column(pn.pane.Markdown("### Folder"),
                           pn.Row(pn.Spacer(width=30),folder_mc)
                       )
    
    users = con.sql("SELECT * FROM user").df()

    if len(username_input)>0:

        if username_input in users["user_username"].unique():
            
            user_row = users.loc[users["user_username"] == username_input].to_dict('records')[0]
            folder_mc.visible = True

            if user_row["user_role"] == "super_user":
                folder_options = sqlfunc.get_options("folder", "foldername", "folder_uuid", " where is_root = 1")
                folder_mc.options = {v:k for k,v in sorted(folder_options.items(), key=lambda item: item[1], reverse=True)} #MC and Tabulator have opposite inputs, so have to flip dictionary here
            else:
                folder_options = sqlfunc.get_user_folders(user_row["user_uuid"])
                folder_mc.options = {k:v for k,v in sorted(folder_options.items(), reverse=True)} 
        else:
            folder_mc.visible = False
        
        return folder_tab

#--------------------------------------------------------
def update_tabs_with_folder(folder_mc):            
        
    tabs = pn.Tabs()

    if len(folder_mc) > 0:
        #tab_layout = con.sql("SELECT * FROM tab_layout").df()
        tab_layout = pd.read_csv("./tab_layout_v1.csv")

        overview_header = pn.pane.Markdown("""Test Title""")
        overview_tab = panelfunc.get_nonsearchable_folder_tab(tab_layout.loc[tab_layout["tab_name"]=="Overview"], "Overview", overview_header, folder_mc, save_button)
        tabs.append(("Overview", overview_tab))

        collaborators_header = pn.pane.Markdown("""Test Title""")
        collaborators_tab = panelfunc.get_nonsearchable_folder_tab(tab_layout.loc[tab_layout["tab_name"]=="Collaborators"], 
                                                                   "Collaborators", 
                                                                   collaborators_header, 
                                                                   folder_mc, 
                                                                   save_button)
        tabs.append(("Collaborators",collaborators_tab))

        protocol_header = pn.pane.Markdown("""Test Title""")
        protocol_tab = panelfunc.get_nonsearchable_folder_tab(tab_layout.loc[tab_layout["tab_name"]=="Protocol"], "Protocol", 
                                                              protocol_header, folder_mc, save_button)
        tabs.append(("Protocol", protocol_tab))

        #studydesign_header = pn.pane.Markdown("""Test Title""")
        #studydesign_tab = panelfunc.get_nonsearchable_folder_tab(tab_layout, "Study Design", studydesign_header, folder_mc, save_button)
        #tabs.append(("Study Design", studydesign_tab))

        biomaterial_header = pn.pane.Markdown("""Test Title""")
        biomaterial_tab = panelfunc.get_nonsearchable_sample_tab(tab_layout.loc[tab_layout["tab_name"]=="Sample (Biomaterial)"],  biomaterial_header, folder_mc)#, by_sample=True)
        tabs.append(("Biomaterial", biomaterial_tab))
        
        save_button.visible = True
        return tabs

    else:
        return "No folder selected"


#---------------------------------------------------------POPULATE TEMPLATE---------------------------------------------------------


# %% CREATE PANEL COMPONENTS
save_button = pn.widgets.Button(name='Save', button_type='primary', visible=False)
username_input = pn.widgets.TextInput(name="Username", placeholder="Enter your username here...")
folder_mc = pn.widgets.MultiChoice(name="Folder Name", max_items=1, 
                                   option_limit=200, visible=False, 
                                   placeholder="Please select a folder to edit...")

folder_tab = pn.bind(update_folder_by_username, username_input)
main_tab = pn.bind(update_tabs_with_folder, folder_mc)

template = pn.template.MaterialTemplate(
        site="Coriell",
        title="Submission Form (Research Data Browser)",
        sidebar=pn.Column(username_input,
                          folder_tab),
        main = pn.Column(pn.Spacer(height=35),
                         save_button,
                         pn.Spacer(height=35),
                         main_tab))


template.servable()