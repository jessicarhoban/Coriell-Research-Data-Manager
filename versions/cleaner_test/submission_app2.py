# %% INITIAL SETUP

#  To-do
#         - Add update_commands for: 
#               - treatment_folder table, 
#               - treatmentgroup_folder table, 
#               - widget_row
#           - Required
#           - Delete name all the way messes it up in tables


# Import required libraries
import pandas as pd
import requests
import uuid 
import time
from functools import partial
import warnings
warnings.filterwarnings('ignore')

# Import panel library and extensions
import panel as pn
from bokeh.models.widgets.tables import NumberFormatter, BooleanFormatter
pn.extension(design="material", sizing_mode="stretch_width")
pn.extension('tabulator')
#pn.extension('tabulator-tables')
pn.extension('mathjax')

# Import and establish database connection
import duckdb
con = duckdb.connect("./browser_cleaner.db")

# Import custom functions
import functions.sql_functions as sqlfunc
import functions.api_functions as apifunc
import functions.searchtab_functions as searchfunc
import functions.panel_functions as panelfunc
import parameters.multichoice_options as multioptions
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


#---------------------------------------------------------UPDATE TABS---------------------------------------------------------
def update_folder_by_username(username_input):            

    # Initialize tab
    folder_tab = pn.Column(pn.pane.Markdown("### Folder"),
                           pn.Row(pn.Spacer(width=30),folder_mc)
                       )
    
    # Get all users
    users = con.sql("SELECT * FROM user").df()

    # Check if username has been entered
    if len(username_input)>0:

        #-------------------------If valid username-------------------------
        if username_input in users["user_username"].unique():
            
            user_row = users.loc[users["user_username"] == username_input].to_dict('records')[0]
            folder_mc.visible = True

            # If they're a super user, get all folders
            if user_row["user_role"] == "super_user":
                filter = " where is_root = 1"

            # Otherwise, find which folders they have access to 
            else:
                filter = [""" where folder.folder_uuid in 
                                (SELECT distinct f.folder_uuid 
                                FROM researcher_folder rf
                                    join folder f on f.folder_uuid = rf.folder_uuid
                                WHERE rf.researcher_uuid = '""", user_row["researcher_uuid"], 
                                "') and folder.is_root = 1"]
                
                filter = "".join(filter)

            # Use these filters to find accessible folders
            folder_options = sqlfunc.get_options(table_name = "folder", 
                                                 field_name = "foldername", 
                                                 field_uuid = "folder_uuid", 
                                                 filter = filter)
            
            folder_mc.options = folder_options 

        #-------------------------If invalid username-------------------------
        else:
            folder_mc.visible = False
        
        return folder_tab

#--------------------------------------------------------
def update_tabs_with_folder(folder_mc):            
        
    tabs = pn.Tabs()

    if len(folder_mc) > 0:

        for folder in folder_mc:

            #tab_layout = con.sql("SELECT * FROM tab_layout").df()
            tab_layout = pd.read_csv("./tab_layout_v3.csv")

            #-------------------------Overview tab-------------------------
            title_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Title'].reset_index().iloc[0], 
                                                 save_button = save_button, 
                                                 folder = folder)

            summary_abstract_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Summary (Abstract)'].reset_index().iloc[0], 
                                                            save_button = save_button, 
                                                            folder = folder)

            experimental_design_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Experimental Design'].reset_index().iloc[0], 
                                                               save_button = save_button, 
                                                               folder = folder)

            overview_accordion1 = pn.Accordion(("Optional", 
                                                pn.Column(summary_abstract_widget, 
                                                          experimental_design_widget)))
             
            overview_tab = pn.Column(pn.pane.Markdown("""# Overview"""),
                                     pn.Row(pn.Spacer(height=20)),
                                     title_widget,pn.Row(pn.Spacer(height=20)),
                                     overview_accordion1,
                                     pn.Row(pn.Spacer(height=20)))


            tabs.append(("Overview", overview_tab))

            #-------------------------Collaborators tab-------------------------
            collaborator_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Collaborator'].reset_index().iloc[0], 
                            save_button = save_button, 
                            folder = folder)
            
            lab_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Lab'].reset_index().iloc[0], 
                            save_button = save_button, 
                            folder = folder)

            institute_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Collaborating Institute'].reset_index().iloc[0], 
                            save_button = save_button, 
                            folder = folder)
            
            study_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Study'].reset_index().iloc[0], 
                            save_button = save_button, 
                            folder = folder)


            collaborators_accordion1 = pn.Accordion(("Optional", 
                                                pn.Column(pn.Spacer(height=20),
                                                          institute_widget, 
                                                          study_widget)))

            collaborators_tab = pn.Column(pn.pane.Markdown("""# Collaborators"""),
                                     pn.Row(pn.Spacer(height=20)),
                                     pn.Row(pn.Spacer(width=30),collaborator_widget),pn.Row(pn.Spacer(height=20)),
                                     pn.Row(pn.Spacer(width=30),lab_widget),pn.Row(pn.Spacer(height=20)),
                                     collaborators_accordion1,
                                     pn.Row(pn.Spacer(height=20)))

            tabs.append(("Collaborators", collaborators_tab))

            #-------------------------Protocol tab-------------------------

            library_construction_protocol_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Library Construction Protocol'].reset_index().iloc[0], 
                            save_button = save_button, 
                            folder = folder)

            instrumentmodel_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Instrument Model'].reset_index().iloc[0], 
                                                    save_button = save_button, 
                                                    folder = folder)

            singlepaired_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Single or Paired End'].reset_index().iloc[0], 
                                                    save_button = save_button, 
                                                    folder = folder)

            library_strategy_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Library Strategy'].reset_index().iloc[0], 
                            save_button = save_button, 
                            folder = folder)

            data_processing_step_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Data Processing Step'].reset_index().iloc[0], 
                            save_button = save_button, 
                            folder = folder)

            genome_build_assembly_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Genome Build / Assembly'].reset_index().iloc[0], 
                            save_button = save_button, 
                            folder = folder)

            processed_data_files_format_and_content_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Processed Data Files Format and Content'].reset_index().iloc[0], 
                            save_button = save_button, 
                            folder = folder)
            
            molecule_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Molecule'].reset_index().iloc[0], 
                                                    save_button = save_button, 
                                                    folder = folder)

            protocol_box1 = pn.WidgetBox("Step 1:", 
                                         library_strategy_widget,
                                         instrumentmodel_widget,
                                         molecule_widget)
            
            protocol_box2 = pn.WidgetBox("Step 2:", 
                                         genome_build_assembly_widget)
            protocol_accordion1 = pn.Accordion(("Optional:",
                                                pn.Column(library_construction_protocol_widget, 
                                                data_processing_step_widget,
                                                processed_data_files_format_and_content_widget)))
            
            protocol_tab = pn.Column(pn.pane.Markdown("""# Protocol"""),
                                     pn.Row(pn.Spacer(height=20)),
                                     protocol_box1,
                                     protocol_box2,
                                     protocol_accordion1)

            tabs.append(("Protocol", protocol_tab))


            #-------------------------Treatment tab-------------------------
            has_treatment_switch = pn.widgets.Switch(name="Treatment Switch",
                                                     width = 30)

            treatmentgroup_sample_widget_template = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Treatment Samples'].reset_index().iloc[0], 
                                                                save_button = save_button, 
                                                                folder = folder,
                                                                visible = False)
            
            treatment_widget_row = pn.Row()
            treatment_widget_row.options = treatmentgroup_sample_widget_template.options

            treatment_protocol_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Treatment Protocol'].reset_index().iloc[0], 
                            save_button = save_button, 
                            folder = folder)
            
            treatmentgroup_table, add_treatmentgroup_button, delete_treatmentgroup_button = panelfunc.get_table(sql_table = "treatmentgroup_folder", 
                                                                                                                        sql_columns = ["treatmentgroup_name", "treatmentgroup_uuid", "folder_uuid"], 
                                                                                                                        sql_linksto = "folder_uuid",
                                                                                                                        folder = folder,
                                                                                                                        formatters = {},
                                                                                                                        editors = {},
                                                                                                                        name = "Treatment Group",
                                                                                                                        titles = {'treatmentgroup_name': "Group Name"},
                                                                                                                        hidden_columns = ["treatmentgroup_uuid","folder_uuid"],
                                                                                                                        widget_row = treatment_widget_row, 
                                                                                                                        widget_template = treatmentgroup_sample_widget_template, 
                                                                                                                        addrow_uuid = "treatmentgroup_uuid", 
                                                                                                                        addrow_newname = "treatmentgroup_name",
                                                                                                                        effect_type="add_widget"
                                                                                                                        )
            
            treatment_table, add_treatment_button, delete_treatment_button = panelfunc.get_table(sql_table = "treatment_folder", 
                                                                                                     sql_columns = ["treatment_nickname","treatment_name", 
                                                                                                                    "dosage", "dosage_units", "timing", "timing_units",
                                                                                                                    "treatment_uuid","folder_uuid"], 
                                                                                                        sql_linksto = "folder_uuid",
                                                                                                        formatters = {},
                                                                                                        folder = folder,
                                                                                                        editors = { "dosage":{'name':"dosage", 'type': 'number'},
                                                                                                                    "dosage_units":{'name':"dosage_units", 'type': 'list', 'values':["mg","nM"]},
                                                                                                                    "timing":{'name':"timing", 'type': 'number'},
                                                                                                                    "timing_units":{'name':"timing_units", 'type': 'list', 'values':["hrs","mins","days"]}},
                                                                                                        titles = {  "treatment_nickname": "Treatment Nickname",
                                                                                                                    "treatment_name": "Treatment Name",
                                                                                                                    "dosage": "Dosage",
                                                                                                                    "dosage_units": "Dosage Units",
                                                                                                                    "timing": "Timing",
                                                                                                                    "timing_units": "Timing Units"},
                                                                                                        name = "Treatment",
                                                                                                        addrow_uuid = "treatment_uuid", 
                                                                                                        addrow_newname = "treatment_nickname",
                                                                                                        affected_table = treatmentgroup_table, 
                                                                                                        effect_type="add_column",
                                                                                                        hidden_columns = ["treatment_uuid","folder_uuid"])
            
            treatment_tab1 = pn.Column(pn.pane.Markdown("""# Treatment"""),
                                 pn.pane.Markdown("""### Were treatments used in this experiment?"""),
                                 pn.Row(pn.Spacer(width=10),has_treatment_switch),
                                 pn.layout.Divider())

            treatment_widgetbox1 = pn.WidgetBox(""" Step 1: Define treatments""",
                                                pn.Row(pn.Spacer(width=60),add_treatment_button, delete_treatment_button),
                                                treatment_table)
            
            treatment_widgetbox2 = pn.WidgetBox(""" Step 2: Define treatment groups""",
                                                pn.Row(pn.Spacer(width=60),add_treatmentgroup_button, delete_treatmentgroup_button),
                                                treatmentgroup_table)
            
            treatment_widgetbox3 = pn.WidgetBox(""" Step 3: Assign samples to groups""",
                                                treatment_widget_row)

            treatment_accordion1 = pn.Accordion(("Optional:",
                                                pn.Column(treatment_protocol_widget)))
            
            treatment_tab2 = pn.Column(treatment_widgetbox1,
                                       treatment_widgetbox2,
                                       treatment_widgetbox3,
                                       treatment_accordion1)
            
            treatment_tab2.visible=False
            treatment_tab = pn.Column(treatment_tab1,treatment_tab2)
            pn.bind(panelfunc.show_section, has_treatment_switch, treatment_tab2, watch=True)
            tabs.append(("Treatment", treatment_tab))




            #-------------------------Biomaterial tab-------------------------


            #------Non-Table Widgets------
            biomaterial_toggle = pn.widgets.MultiChoice(name="Biomaterial Toggle",
                                                        options = ["Commercial Cell Line", "Commercial Mouse Model", "Custom Biomaterial"],
                                                        #behavior="radio",
                                                        max_items=1,
                                                        value=[])
            

            growth_protocol_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Growth Protocol'].reset_index().iloc[0], 
                            save_button = save_button, 
                            folder = folder)

            extract_protocol_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Extract Protocol'].reset_index().iloc[0], 
                            save_button = save_button, 
                            folder = folder)
            
            #------MultiChoice for Assigning Samples to Biomaterials------
            biomaterialgroup_sample_widget_template = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Biomaterial Samples'].reset_index().iloc[0], 
                                                                save_button = save_button, 
                                                                folder = folder,
                                                                visible = False)
            
            biomaterial_widget_row = pn.Row()
            biomaterial_widget_row.options = biomaterialgroup_sample_widget_template.options


            #------Custom Biomaterial Table------
            organism_options = sqlfunc.get_options(table_name = 'parameter', field_name = 'parameter_name', field_uuid = 'parameter_uuid',
                                                    filter     =  " where parameter.parameter_type = 'organism'")
            biomaterialtype_options = sqlfunc.get_options(table_name = 'parameter', field_name = 'parameter_name', field_uuid = 'parameter_uuid',
                                                    filter     =  " where parameter.parameter_type = 'biomaterial'") 
            sex_options = sqlfunc.get_options(table_name = 'parameter', field_name = 'parameter_name', field_uuid = 'parameter_uuid',
                                                    filter     =  " where parameter.parameter_type = 'sex'")         
            cell_options = sqlfunc.get_options(table_name = 'cell', field_name = 'cell_name', field_uuid = 'cell_uuid')
            tissue_options = sqlfunc.get_options(table_name = 'tissue', field_name = 'tissue_name', field_uuid = 'tissue_uuid')
            
            custombiomaterial_editors = {"organism":{'name':"organism", 'type': 'list', 'values':list(organism_options.keys())},
                                   "sex":{'name':"sex", 'type': 'list', 'values':list(sex_options.keys())},
                                   "biomaterial_type":{'name':"biomaterial_type", 'type': 'list', 'values':list(biomaterialtype_options.keys())},
                                    "cell":{'name':"cell", 'type': 'list', 'values':list(cell_options.keys()), 'autocomplete':True, 'listOnEmpty':True},
                                    "tissue":{'name':"tissue", 'type': 'list', 'values':list(tissue_options.keys()), 'autocomplete':True, 'listOnEmpty':True},
                                   "age":{'name':"age", 'type': 'input', 'mask':'00Y, 00M, 00W','maskAutoFill':True, "maskNumberChar":"0"}}                                                                                                               
            custombiomaterial_table, add_custombiomaterial_button, delete_custombiomaterial_button = panelfunc.get_table(sql_table = "biomaterial_folder", 
                                                                                                     sql_columns = ["biomaterial_nickname","organism","biomaterial_type",
                                                                                                                    "sex","age","cell","tissue",
                                                                                                                    "biomaterial_uuid","folder_uuid"], 
                                                                                                        sql_linksto = "folder_uuid",
                                                                                                        formatters = {},
                                                                                                        folder = folder,
                                                                                                        editors = custombiomaterial_editors,
                                                                                                        titles = {  "biomaterial_nickname": "Biomaterial Nickname",
                                                                                                                  "biomaterial_type":"Type",
                                                                                                                   "organism": "Organism",
                                                                                                                    "age":"Age (Year-Month-Week)",
                                                                                                                    "sex": "Sex",
                                                                                                                    "cell": "Cell",
                                                                                                                    "tissue": "Tissue"},
                                                                                                        name = "Custom Biomaterial",
                                                                                                        addrow_uuid = "biomaterial_uuid", 
                                                                                                        addrow_newname = "biomaterial_nickname",
                                                                                                        widget_row = biomaterial_widget_row, 
                                                                                                        widget_template = biomaterialgroup_sample_widget_template, 
                                                                                                        effect_type="add_widget",
                                                                                                        hidden_columns = ["biomaterial_uuid","folder_uuid"])
            
            #------Commercial Cell Line Table------
            #cellline_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Cell Line'].reset_index().iloc[0], 
            #                                    save_button = save_button, 
            #                                    folder = folder)

            #------Commercial Mouse Model Table------
            strain_options = sqlfunc.get_options(table_name = 'strain', field_name = 'JAX_id', field_uuid = 'strain_uuid')
               
            mouse_editors = {"strain":{'name':"strain", 'type': 'list', 'values':list(strain_options.keys()), 'autocomplete':True, 'listOnEmpty':True},
                             "cell":{'name':"cell", 'type': 'list', 'values':list(cell_options.keys()), 'autocomplete':True, 'listOnEmpty':True},
                             "tissue":{'name':"tissue", 'type': 'list', 'values':list(tissue_options.keys()), 'autocomplete':True, 'listOnEmpty':True},
                             "sex":{'name':"sex", 'type': 'list', 'values':list(sex_options.keys()), 'autocomplete':True, 'listOnEmpty':True},
                             "age":{'name':"age", 'type': 'input', 'mask':'00Y, 00M, 00W','maskAutoFill':True, "maskNumberChar":"0"}}                                                                                                             
            mouse_table, add_mouse_button, delete_mouse_button = panelfunc.get_table(sql_table = "biomaterial_folder", 
                                                                                                     sql_columns = ["biomaterial_nickname",
                                                                                                                    "strain","cell","tissue","age","sex",
                                                                                                                    "biomaterial_uuid","folder_uuid"], 
                                                                                                        sql_linksto = "folder_uuid",
                                                                                                        formatters = {},
                                                                                                        folder = folder,
                                                                                                        editors = mouse_editors,
                                                                                                        titles = {  "biomaterial_nickname": "Biomaterial Nickname",
                                                                                                                    "strain":"Strain (JAX Laboratory ID)",
                                                                                                                    "age":"Age (Year-Month-Week)",
                                                                                                                    "cell": "Cell",
                                                                                                                    "sex":"Sex",
                                                                                                                    "tissue": "Tissue"},
                                                                                                        name = "Mouse Model",
                                                                                                        addrow_uuid = "biomaterial_uuid", 
                                                                                                        addrow_newname = "biomaterial_nickname",
                                                                                                        widget_row = biomaterial_widget_row, 
                                                                                                        widget_template = biomaterialgroup_sample_widget_template, 
                                                                                                        effect_type="add_widget",
                                                                                                        hidden_columns = ["biomaterial_uuid","folder_uuid"])
           
            #-------------------------
            biomaterial_tab_head = pn.Column(pn.pane.Markdown("""# Biomaterial"""),
                                         pn.pane.Markdown("""### What kind of biomaterial was used in this experiment?"""),
                                         pn.Row(pn.Spacer(width=10),biomaterial_toggle),
                                         pn.Spacer(height=30))
            
            biomaterial_tab_custom = pn.WidgetBox(pn.Spacer(height=10),
                                               pn.pane.Markdown("""### Step 1: Define custom biomaterial"""),
                                                pn.Row(pn.Spacer(width=60),add_custombiomaterial_button, delete_custombiomaterial_button),
                                                custombiomaterial_table,
                                                pn.Spacer(height=20))
            
            biomaterial_tab_known = pn.WidgetBox(pn.Spacer(height=10),
                                              pn.pane.Markdown("""### Step 1: Please select the commercially available cell line."""),
                                              pn.Spacer(height=20))
                                               #pn.Row(pn.Spacer(width=30), sex_widget),pn.Row(pn.Spacer(height=20)),
                                               #pn.Row(pn.Spacer(width=30), cellline_widget),pn.Row(pn.Spacer(height=20)),
                                              
                            
            biomaterial_tab_mouse = pn.WidgetBox(pn.Spacer(height=10),
                                        pn.pane.Markdown("""### Step 1: Please select the commercially available mouse model."""),
                                              pn.Row(pn.Spacer(width=60),add_mouse_button, delete_mouse_button),
                                              mouse_table,
                                              pn.Spacer(height=20))
            
            
            biomaterial_tab_tail = pn.WidgetBox(pn.pane.Markdown("""### Step 2: Assign samples to biomaterial groups"""),
                                                biomaterial_widget_row)
                        
            biomaterial_accordion1 = pn.Accordion(("Optional", 
                                                   pn.Column(growth_protocol_widget, extract_protocol_widget)))
                       
            biomaterial_tab_custom.visible=False
            biomaterial_tab_mouse.visible=False
            biomaterial_tab_known.visible=False
            biomaterial_tab_tail.visible=False

            biomaterial_tab = pn.Column(biomaterial_tab_head, 
                                        biomaterial_tab_custom, pn.Spacer(height=10),
                                        biomaterial_tab_known, pn.Spacer(height=10),
                                        biomaterial_tab_mouse, pn.Spacer(height=10),
                                        biomaterial_tab_tail, pn.Spacer(height=30),
                                        biomaterial_accordion1)
            
            pn.bind(panelfunc.show_section, 
                    biomaterial_toggle, 
                    {"Custom Biomaterial":biomaterial_tab_custom,
                    "Commercial Cell Line":biomaterial_tab_known, 
                     "Commercial Mouse Model": biomaterial_tab_mouse},
                    "Toggle", 
                    biomaterial_tab_tail,
                    watch=True)
            tabs.append(("Biomaterial", biomaterial_tab))
            #-------------------------Samples tab-------------------------




            #-------------------------Conditions tab-------------------------
            has_disease_switch = pn.widgets.Switch(name="Disease Switch",
                                                     width = 30)
            studying_disease_switch = pn.widgets.Switch(name="Disease Study Switch",
                                                     width = 30)
            disease_study_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Disease'].reset_index().iloc[0], 
                                                        save_button = save_button, 
                                                        folder = folder)
            
            disease_sample_widget_template = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Disease Samples'].reset_index().iloc[0], 
                                                                save_button = save_button, 
                                                                folder = folder,
                                                                visible = False)
            disease_widget_row = pn.Row()
            disease_widget_row.options = disease_sample_widget_template.options
            
            disease_options = sqlfunc.get_options(table_name = 'disease', field_name = 'disease_name', field_uuid = 'disease_uuid')
            disease_editors = {"disease_name":{'name':"disease_name", 'type': 'list', 'values':list(disease_options.keys()), 'autocomplete':True, 'listOnEmpty':True},
                               "stage":{'name':"stage", 'type': 'list', 'values':["Stage 0","Stage 1","Stage 2","Stage 3","Stage 4"]}}

            disease_table, add_disease_button, delete_disease_button = panelfunc.get_table(sql_table = "disease_folder", 
                                                                                                        sql_columns = ["disease_nickname","disease_name","stage",
                                                                                                                      "disease_uuid","folder_uuid"], 
                                                                                                        sql_linksto = "folder_uuid",
                                                                                                        formatters = {},
                                                                                                        folder = folder,
                                                                                                        editors = disease_editors,
                                                                                                        titles = { "disease_nickname": "Disease Nickname",
                                                                                                                  "disease_name": "Disease Name",
                                                                                                                  "stage": "Stage"},
                                                                                                        name = "Disease",
                                                                                                        addrow_uuid = "disease_uuid", 
                                                                                                        addrow_newname = "disease_nickname",
                                                                                                        widget_row = disease_widget_row, 
                                                                                                        widget_template = disease_sample_widget_template, 
                                                                                                        effect_type="add_widget",
                                                                                                        hidden_columns = ["disease_uuid","folder_uuid"])
            
            disease_tab1 = pn.Column(pn.pane.Markdown("""# Disease"""),
                                 pn.pane.Markdown("""### Are there any diseases associated with your biomaterial?"""),
                                 pn.Row(pn.Spacer(width=10),has_disease_switch))

            disease_tab2 = pn.Column(pn.Row(pn.Spacer(height=50)),
                                               pn.pane.Markdown("""### Step 1: Define diseases and stages"""),
                                                pn.Row(pn.Spacer(width=60),add_disease_button, delete_disease_button),
                                                pn.Row(pn.Spacer(height=10)),
                                                pn.Row(pn.Spacer(width=30),disease_table),
                                                pn.Row(pn.Spacer(height=50)))
            
            disease_tab_tail = pn.Column(pn.pane.Markdown("""### Step 2: Assign samples to disease groups"""),
                                            pn.Row(pn.Spacer(width=30), disease_widget_row),
                                            pn.Row(pn.Spacer(height=50)))
            
            disease_tab3 = pn.Column(pn.layout.Divider(),
                                     pn.pane.Markdown("""### If not, are there any diseases that this project aims to study indirectly?"""),
                                    pn.Row(pn.Spacer(width=10),studying_disease_switch))
            
            disease_tab4 = pn.Column(pn.pane.Markdown("""### Select the diseases being studied. """),
                                            pn.Row(pn.Spacer(width=30), disease_study_widget),
                                            pn.Row(pn.Spacer(height=50)))

            disease_tab2.visible=False
            disease_tab_tail.visible=False
            disease_tab4.visible=False
            disease_tab = pn.Column(disease_tab1,disease_tab2, disease_tab_tail, disease_tab3, disease_tab4)
            pn.bind(panelfunc.show_section, has_disease_switch, disease_tab2, watch=True)
            pn.bind(panelfunc.show_section, has_disease_switch, disease_tab_tail, watch=True)
            pn.bind(panelfunc.show_section, studying_disease_switch, disease_tab4, watch=True)
            tabs.append(("Disease", disease_tab))




            #-------------------------Genes tab-------------------------
            has_gene_switch = pn.widgets.Switch(name="Gene Switch",
                                                     width = 30)
            studying_gene_switch = pn.widgets.Switch(name="Gene Study Switch",
                                                     width = 30)
            gene_study_widget = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Gene'].reset_index().iloc[0], 
                                                        save_button = save_button, 
                                                        folder = folder)    
            gene_sample_widget_template = panelfunc.get_widgets(widget_row = tab_layout.loc[tab_layout['widget_name']=='Gene Samples'].reset_index().iloc[0], 
                                                                save_button = save_button, 
                                                                folder = folder,
                                                                visible = False)
            gene_widget_row = pn.Row()
            gene_widget_row.options = gene_sample_widget_template.options

            gene_options = sqlfunc.get_options(table_name = 'gene', field_name = 'gene_name', field_uuid = 'gene_uuid')
            knockout_options = sqlfunc.get_options(table_name = 'parameter', field_name = 'parameter_name', field_uuid = 'parameter_uuid',
                                                    filter     =  " where parameter.parameter_type = 'knockout'")

            gene_editors = {"gene_name":{'name':"gene_name", 'type': 'list', 'values':list(gene_options.keys()), 'autocomplete':True, 'listOnEmpty':True},
                               "knockout":{'name':"knockout", 'type': 'list', 'values':list(knockout_options.keys())}}

            gene_table, add_gene_button, delete_gene_button = panelfunc.get_table(sql_table = "gene_folder", 
                                                                                                        sql_columns = ["gene_nickname","gene_name","knockout",
                                                                                                                      "gene_uuid","folder_uuid"], 
                                                                                                        sql_linksto = "folder_uuid",
                                                                                                        formatters = {},
                                                                                                        folder = folder,
                                                                                                        editors = gene_editors,
                                                                                                        titles = { "gene_nickname": "Gene Nickname",
                                                                                                                  "gene_name": "Gene Name",
                                                                                                                  "knockout": "If modified, how?"},
                                                                                                        name = "gene",
                                                                                                        addrow_uuid = "gene_uuid", 
                                                                                                        addrow_newname = "gene_nickname",
                                                                                                        widget_row = gene_widget_row, 
                                                                                                        widget_template = gene_sample_widget_template, 
                                                                                                        effect_type="add_widget",
                                                                                                        hidden_columns = ["gene_uuid","folder_uuid"])
            
            gene_tab1 = pn.Column(pn.pane.Markdown("""# Gene"""),
                                 pn.pane.Markdown("""### Are there any genes associated with your biomaterial?"""),
                                 pn.Row(pn.Spacer(width=10),has_gene_switch))

            gene_tab2 = pn.Column(pn.Row(pn.Spacer(height=50)),
                                               pn.pane.Markdown("""### Step 1: Define genes and stages"""),
                                                pn.Row(pn.Spacer(width=60),add_gene_button, delete_gene_button),
                                                pn.Row(pn.Spacer(height=10)),
                                                pn.Row(pn.Spacer(width=30),gene_table),
                                                pn.Row(pn.Spacer(height=50)))
            
            gene_tab_tail = pn.Column(pn.pane.Markdown("""### Step 2: Assign samples to gene groups"""),
                                            pn.Row(pn.Spacer(width=30), gene_widget_row),
                                            pn.Row(pn.Spacer(height=50)))
            
            gene_tab3 = pn.Column(pn.layout.Divider(),
                                pn.pane.Markdown("""### If not, are there any genes that this project aims to study indirectly?"""),
                                 pn.Row(pn.Spacer(width=10),studying_gene_switch))
            
            gene_tab4 = pn.Column(pn.pane.Markdown("""### Select the genes being studied. """),
                                            pn.Row(pn.Spacer(width=30), gene_study_widget),
                                            pn.Row(pn.Spacer(height=50)))

            gene_tab2.visible=False
            gene_tab_tail.visible=False
            gene_tab4.visible=False
            gene_tab = pn.Column(gene_tab1,gene_tab2, gene_tab_tail, gene_tab3, gene_tab4)
            pn.bind(panelfunc.show_section, has_gene_switch, gene_tab2, watch=True)
            pn.bind(panelfunc.show_section, has_gene_switch, gene_tab_tail, watch=True)
            pn.bind(panelfunc.show_section, studying_gene_switch, gene_tab4, watch=True)

            tabs.append(("Gene", gene_tab))



            #----------------------------------------------------
            save_button.visible = True
            return tabs

    else:
        return "No folder selected"


#---------------------------------------------------------POPULATE TEMPLATE---------------------------------------------------------


# %% CREATE PANEL COMPONENTS
save_button = pn.widgets.Button(name='Save', 
                                button_type='primary', 
                                visible=False)

username_input = pn.widgets.TextInput(name="Username", 
                                      placeholder="Enter your username here...")

folder_mc = pn.widgets.MultiChoice(name="Folder Name", 
                                   max_items=1, 
                                   option_limit=200, 
                                   visible=False, 
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