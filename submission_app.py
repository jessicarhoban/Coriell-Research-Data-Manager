# %% NOTES

#panel serve /home/jhoban/data/20240430_research_management_app/submission_app2.py -—autoreload

#  To-do
#         - Add update_commands for: 
#               - treatment_folder table, 
#               - treatmentgroup_folder table, 
#               - widget_row
#           - Required
#           - Save dialog

# %% IMPORT LIBRARIES
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
pn.extension('mathjax')

# Import and establish database connection
import duckdb
con = duckdb.connect("./database/browser_cleaner.db")

# Import custom functions
import functions.sql_functions as sqlfunc
import functions.api_functions as apifunc
import functions.searchtab_functions as searchfunc
import functions.panel_functions as panelfunc

# Shortcuts for quick formatting
from functions.panel_functions import get_formatted_widget as gfw
from functions.panel_functions import get_formatted_column as gfc
from functions.panel_functions import get_spacer as gs


#---------------------------------------------------------UPDATE TABS---------------------------------------------------------
# %% UPDATE FOLDERS BASED ON USER INPUT
def update_folder_by_username(username_input):            

    # Initialize tab
    folder_tab = pn.Column(pn.pane.Markdown("### Project"),
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
# %% UPDATE TABS BASED ON FOLDER INPUT
def update_tabs_with_folder(folder_mc):            
    
    tabs = pn.Tabs()

    if len(folder_mc) > 0:

        for folder in folder_mc:

            tab_layout = pd.read_csv("./tab_layout.csv")
            widget_kwargs = {"save_button":save_button, "folder":folder, "tab_layout":tab_layout}
    
            # %%-----------------------------------Overview tab---------------------------------------
            # Create headers, definitions, etc.
            overview_markdown1 = pn.pane.Markdown("""# Overview""")

            # Create widgets
            title_widget = panelfunc.get_widgets(widget_name='Title', **widget_kwargs)
            summary_abstract_widget = panelfunc.get_widgets(widget_name='Summary (Abstract)', **widget_kwargs)
            experimental_design_widget = panelfunc.get_widgets(widget_name='Experimental Design', **widget_kwargs)

            # Format widgets into panes (i.e., accordions, columns, tabs)
            overview_section1 = gfc([overview_markdown1, gfw(title_widget)])
            overview_section2 = pn.Accordion(("Optional", gfc([summary_abstract_widget, experimental_design_widget])))
            overview_tab = gfc([overview_section1, overview_section2])
            
            # Append to tabs
            tabs.append(("Overview", overview_tab))


            # %%-----------------------------------Collaborator tab-----------------------------------
            # Create headers, definitions, etc.
            collaborator_markdown1 = pn.pane.Markdown("""# Collaborator""")

            # Create widgets
            collaborator_widget = panelfunc.get_widgets(widget_name='Collaborator',**widget_kwargs)
            lab_widget = panelfunc.get_widgets(widget_name='Lab', **widget_kwargs)
            institute_widget = panelfunc.get_widgets(widget_name='Collaborating Institute', **widget_kwargs)
            study_widget = panelfunc.get_widgets(widget_name='Study', **widget_kwargs)

            # Format widgets into panes (i.e., accordions, columns, tabs)
            collaborator_section1 = gfc([collaborator_markdown1,gfw(collaborator_widget),gfw(lab_widget)])
            collaborator_section2 = pn.Accordion(("Optional", gfc([institute_widget,study_widget])))
            collaborator_tab = gfc([collaborator_section1, collaborator_section2])

            # Append to tabs
            tabs.append(("Collaborators", collaborator_tab))


            # %%-----------------------------------Protocol tab---------------------------------------
            # Create headers, definitions, etc.
            protocol_markdown1 = pn.pane.Markdown("""# Library Protocol""")

            # Create widgets
            library_construction_protocol_widget = panelfunc.get_widgets(widget_name='Library Construction Protocol', **widget_kwargs)
            instrumentmodel_widget = panelfunc.get_widgets(widget_name='Instrument Model', **widget_kwargs)
            singlepaired_widget = panelfunc.get_widgets(widget_name='Single or Paired End', **widget_kwargs)
            library_strategy_widget = panelfunc.get_widgets(widget_name='Library Strategy', **widget_kwargs)
            data_processing_step_widget = panelfunc.get_widgets(widget_name='Data Processing Step', **widget_kwargs)
            genome_build_assembly_widget = panelfunc.get_widgets(widget_name='Genome Build / Assembly', **widget_kwargs)
            processed_data_files_format_and_content_widget = panelfunc.get_widgets(widget_name='Processed Data Files Format and Content', **widget_kwargs)
            molecule_widget = panelfunc.get_widgets(widget_name='Molecule', **widget_kwargs)

            # Format widgets into panes (i.e., accordions, columns, tabs)
            protocol_section1 = gfc([protocol_markdown1,
                                     gfw(library_strategy_widget),gfw(singlepaired_widget),
                                     gfw(molecule_widget),gfw(genome_build_assembly_widget)])
            
            protocol_section2 = pn.Accordion(("Optional", 
                                                gfc([gfw(instrumentmodel_widget),
                                                     library_construction_protocol_widget,
                                                     data_processing_step_widget,
                                                     processed_data_files_format_and_content_widget])))
            
            protocol_tab = gfc([protocol_section1, protocol_section2])

            # Append to tabs
            tabs.append(("Protocol", protocol_tab))



            # %%-----------------------------------Treatment tab--------------------------------------
            # Note: This tab will be divided into 5 sections:
                # HEAD
                    # Section 1: Header
                # TAIL
                    # Section 2: Defining treatments
                    # Section 3: Define concurrent treatments
                    # Section 4: Assign treatments to samples
                    # Section 5: Optional

            # (Out of necessity, the sections are defined out of order below)


            #____________________SECTION 1 (HEADER)____________________
            # Create headers, definitions, etc.
            treatment_markdown1a = pn.pane.Markdown("""# Treatment""")
            treatment_markdown1b = pn.pane.Markdown("""### Were treatments used in this experiment?""")

            # Create widgets
            has_treatment_switch = pn.widgets.Switch(name="Treatment Switch",width = 30)

            # Format widgets into panes (i.e., accordions, columns, tabs)
            treatment_section_head = gfc([treatment_markdown1a, treatment_markdown1b, gfw(has_treatment_switch), pn.layout.Divider()])


            #____________________SECTION 3 (CONCURRENT TREATMENTS)____________________
            # Create headers, definitions, etc.
            treatment_markdown3 = pn.pane.Markdown("""### Were there any samples given 2+ treatments?""")

            # Create widgets
            treatmentgroup_sample_widget_template = panelfunc.get_widgets(widget_name='Treatment Samples', **widget_kwargs, visible = False)
            treatment_widget_row = pn.Row()
            treatment_widget_row.options = treatmentgroup_sample_widget_template.options
            has_multiple_treatment_switch = pn.widgets.Switch(name="Multiple Treatment Switch",width = 30)
            
            # Create tables
            treatmentgroup_table, add_treatmentgroup_button, delete_treatmentgroup_button = panelfunc.get_table(sql_table = "treatmentgroup_folder", 
                                                                                                                        sql_columns = ["treatmentgroup_name", "treatmentgroup_uuid", "folder_uuid"], 
                                                                                                                        sql_linksto = "folder_uuid",
                                                                                                                        folder = folder,
                                                                                                                        formatters = {},
                                                                                                                        editors = {},
                                                                                                                        name = "Treatment Group",
                                                                                                                        save_button = save_button,
                                                                                                                        titles = {'treatmentgroup_name': "Group Name"},
                                                                                                                        hidden_columns = ["treatmentgroup_uuid","folder_uuid"],
                                                                                                                        widget_row = treatment_widget_row, 
                                                                                                                        widget_template = treatmentgroup_sample_widget_template, 
                                                                                                                        addrow_uuid = "treatmentgroup_uuid", 
                                                                                                                        addrow_newname = "treatmentgroup_name",
                                                                                                                        effect_type="add_widget"
                                                                                                                        )

            # Format widgets into panes (i.e., accordions, columns, tabs)
            treatment_section3a = gfc([treatment_markdown3,
                                        gfw(has_multiple_treatment_switch)])
            treatment_section3b = gfc([gfw(pn.Row(add_treatmentgroup_button, delete_treatmentgroup_button)),
                                        treatmentgroup_table])
            
            treatment_section3 = pn.Column(treatment_section3a, treatment_section3b)
            
            treatment_section3b.visible=False

            pn.bind(panelfunc.show_section, has_multiple_treatment_switch, treatment_section3b, watch=True)
            

            #____________________SECTION 2 (DEFINING TREATMENTS)____________________
            # Create headers, definitions, etc.
            treatment_markdown2 = pn.pane.Markdown("""""")

            # Create tables
            treatment_options = sqlfunc.get_options(table_name = 'treatment', field_name = 'treatment_name', field_uuid = 'treatment_uuid')

            treatment_table, add_treatment_button, delete_treatment_button = panelfunc.get_table(sql_table = "treatment_folder", 
                                                                                                     sql_columns = ["treatment_nickname","treatment_name", 
                                                                                                                    "dosage", "dosage_units", "timing", "timing_units",
                                                                                                                    "treatment_uuid","folder_uuid"], 
                                                                                                        sql_linksto = "folder_uuid",
                                                                                                        formatters = {},
                                                                                                        folder = folder,
                                                                                                        editors = { "treatment_name":{'name':"treatment_name", 'type': 'list', 'values':list(treatment_options.keys()), 'autocomplete':True, 'listOnEmpty':True},
                                                                                                                    "dosage":{'name':"dosage", 'type': 'number'},
                                                                                                                    "dosage_units":{'name':"dosage_units", 'type': 'list', 'values':["mg","nM"]},
                                                                                                                    "timing":{'name':"timing", 'type': 'number'},
                                                                                                                    "timing_units":{'name':"timing_units", 'type': 'list', 'values':["hrs","mins","days"]}},
                                                                                                        titles = {  "treatment_nickname": "Treatment Label",
                                                                                                                    "treatment_name": "Treatment Name",
                                                                                                                    "dosage": "Dosage",
                                                                                                                    "dosage_units": "Dosage Units",
                                                                                                                    "timing": "Timing",
                                                                                                                    "timing_units": "Timing Units"},
                                                                                                        #configuration={"field":"treatment_nickname", "validator":"required"},
                                                                                                        name = "Treatment",
                                                                                                        save_button = save_button,
                                                                                                        addrow_uuid = "treatment_uuid", 
                                                                                                        addrow_newname = "treatment_nickname",
                                                                                                        affected_table = treatmentgroup_table, 
                                                                                                        widget_row = treatment_widget_row,
                                                                                                        widget_template = treatmentgroup_sample_widget_template, 
                                                                                                        effect_type="add_column",
                                                                                                        hidden_columns = ["treatment_uuid","folder_uuid"],
                                                                                                        fill_template={"switch":has_multiple_treatment_switch,
                                                                                                                       "affected_uuid": "treatmentgroup_uuid",
                                                                                                                       "affected_cols": {"treatmentgroup_name":"Treatment Group"}})
            
            # Format widgets into panes (i.e., accordions, columns, tabs)
            treatment_section2 = gfc([treatment_markdown2,
                                        gfw(pn.Row(add_treatment_button, delete_treatment_button)),
                                        treatment_table])
        
        
            # If there is only one treatment per sample, fill the treatmentgroup table default and don't show
            has_multiple_treatment_switch.param.watch(partial(panelfunc.fill_hidden_table, 
                                                        treatment_table, #THIS ONE
                                                        "treatment_uuid", #addrow_uuid (GOOD)
                                                        "treatment_nickname", #addrow_uuid (GOOD)
                                                        treatmentgroup_table, #affected_table (GOOD)
                                                        "treatmentgroup_uuid", #affected_uuid (ADD)
                                                        "treatmentgroup_name", #affected_uuid (ADD)
                                                        folder,
                                                        "",
                                                        "folder_uuid",
                                                        treatment_widget_row, 
                                                        treatmentgroup_sample_widget_template,
                                                        "treatmentgroup_uuid",
                                                        "treatmentgroup_name",
                                                        "add_widget",
                                                        "",
                                                        "Treatment Group"
                                                    ), #affected_cols (ADD)),
                                                        ['value']
                                                        )
            has_multiple_treatment_switch.param.trigger()


            
            #____________________SECTION 4 (ASSIGN TO SAMPLE)____________________
            # Create headers, definitions, etc.
            treatment_markdown4 = pn.pane.Markdown("""""")
        
            # Format widgets into panes (i.e., accordions, columns, tabs)
            treatment_section4 = gfc([treatment_markdown4,treatment_widget_row])


            #____________________SECTION 5 (OPTIONAL)____________________
            # Create widgets
            treatment_protocol_widget = panelfunc.get_widgets(widget_name='Treatment Protocol', **widget_kwargs)

            # Format widgets into panes (i.e., accordions, columns, tabs)
            treatment_section5 = gfc([treatment_protocol_widget])
            

            # ____________________PUT TOGETHER ALL SECTIONS____________________
            # Put sections 1-4 together
            treatment_section_tail = pn.Accordion(("Step 1: Define treatments", treatment_section2),
                                                ("Step 2: Define concurrent treatments", treatment_section3),
                                                ("Step 3: Assign treatments to samples", treatment_section4),
                                                ("Optional:",treatment_section5))
            
            treatment_tab = pn.Column(treatment_section_head,treatment_section_tail)


            # Set them as invisible unless they said yes to "Has treatment?"
            treatment_section_tail.visible=False
            pn.bind(panelfunc.show_section, has_treatment_switch, treatment_section_tail, watch=True)
            
            # Append to tabs
            tabs.append(("Treatment", treatment_tab))





            # %%-----------------------------------Biomaterial tab------------------------------------


            #____________________SECTION 1 (HEADER)____________________
            # Create headers, definitions, etc.
            biomaterial_markdown1 = pn.pane.Markdown("""# Biomaterial""")
            biomaterial_markdown2 = pn.pane.Markdown("""What kind of biomaterial was used in this experiment?""")
        
            # Create widgets
            biomaterial_toggle = pn.widgets.ToggleGroup(name="Biomaterial Toggle",
                                                        options = ["Commercial Cell Line", "Commercial Mouse Model", "Custom Biomaterial"],
                                                        behavior='radio',
                                                        value="Commercial Cell Line")
            

            # Format widgets into panes (i.e., accordions, columns, tabs)
            biomaterial_section_head1 = gfc([biomaterial_markdown1])#, pn.layout.Divider()
            biomaterial_section_head2 = gfc([biomaterial_markdown2, gfw(biomaterial_toggle)])
            

            #____________________SECTION 6 (OPTIONAL)____________________
            # Create widgets
            growth_protocol_widget = panelfunc.get_widgets(widget_name='Growth Protocol', **widget_kwargs)
            extract_protocol_widget = panelfunc.get_widgets(widget_name='Extract Protocol', **widget_kwargs)
           
            # Format widgets into panes (i.e., accordions, columns, tabs)
            biomaterial_accordion1 = gfc([growth_protocol_widget, extract_protocol_widget])
            

            #____________________SECTION 5 (ASSIGN TO SAMPLE)____________________
            # Create widgets
            biomaterialgroup_sample_widget_template = panelfunc.get_widgets(widget_name='Biomaterial Samples',**widget_kwargs,visible = False)
            
            biomaterial_widget_row = pn.Row()
            biomaterial_widget_row.options = biomaterialgroup_sample_widget_template.options


            #biomaterial_header_tail = pn.pane.Markdown(""""### Step 2: Assign samples to biomaterial groups.""")
            
            biomaterial_tab_tail = gfc([biomaterial_widget_row])
                                                
            
            #____________________SECTION 2 (CUSTOM CELL LINE)____________________
            # Create headers, definitions, etc.
            biomaterial_markdown_custom = pn.pane.Markdown("""Please describe your custom biomaterial.""")
    
            # Create tables
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
                                                                                                        titles = {  "biomaterial_nickname": "Biomaterial Label",
                                                                                                                  "biomaterial_type":"Type",
                                                                                                                   "organism": "Organism",
                                                                                                                    "age":"Age (Year-Month-Week)",
                                                                                                                    "sex": "Sex",
                                                                                                                    "cell": "Cell",
                                                                                                                    "tissue": "Tissue"},
                                                                                                        name = "Custom Biomaterial",
                                                                                                        save_button = save_button,
                                                                                                        addrow_uuid = "biomaterial_uuid", 
                                                                                                        addrow_newname = "biomaterial_nickname",
                                                                                                        widget_row = biomaterial_widget_row, 
                                                                                                        widget_template = biomaterialgroup_sample_widget_template, 
                                                                                                        effect_type="add_widget",
                                                                                                        hidden_columns = ["biomaterial_uuid","folder_uuid"])
            
            # Format widgets into panes (i.e., accordions, columns, tabs)
            biomaterial_tab_custom =  gfc([biomaterial_markdown_custom,
                                                gfw(pn.Row(add_custombiomaterial_button, delete_custombiomaterial_button)),
                                                custombiomaterial_table])

            

            #____________________SECTION 3 (COMMERCIAL CELL LINE)____________________
            
            # Create headers, definitions, etc.
            biomaterial_markdown_known = pn.pane.Markdown("""Please select the commercially available cell line.""")
    
            # Create widgets
            
            
            #cellline_widget = panelfunc.get_widgets(widget_name='Cell Line', **widget_kwargs)
            
            # Format widgets into panes (i.e., accordions, columns, tabs)
            #biomaterial_tab_known =  gfc([biomaterial_markdown_known, cellline_widget])

            # Create tables
            cellline_options = sqlfunc.get_options(table_name = 'cell_line', field_name = 'cell_line_name', field_uuid = 'cell_line_uuid')
            knownbiomaterial_editors = {"cell_line_name":{'name':"cell_line_name", 'type': 'list', 'values':list(cellline_options.keys()), 'autocomplete':True, 'listOnEmpty':True}}
            
            knownbiomaterial_table, add_knownbiomaterial_button, delete_knownbiomaterial_button = panelfunc.get_table(sql_table = "biomaterial_folder", 
                                                                                                        sql_columns = ["biomaterial_nickname","cell_line_uuid","biomaterial_uuid","folder_uuid","cell_line_name"], 
                                                                                                        sql_linksto = "folder_uuid",
                                                                                                        formatters = {},
                                                                                                        folder = folder,
                                                                                                        editors = knownbiomaterial_editors,
                                                                                                        titles = {  "biomaterial_nickname": "Biomaterial Label",
                                                                                                                  "cell_line_name":"Cell Line Name"},
                                                                                                        name = "Known Biomaterial",
                                                                                                        save_button = save_button,
                                                                                                        addrow_uuid = "biomaterial_uuid", 
                                                                                                        addrow_newname = "biomaterial_nickname",
                                                                                                        widget_row = biomaterial_widget_row, 
                                                                                                        widget_template = biomaterialgroup_sample_widget_template, 
                                                                                                        effect_type="add_widget",
                                                                                                        hidden_columns = ["cell_line_uuid","biomaterial_uuid","folder_uuid"])
                                                             
            
            
            # 
            #biomaterial_tab_known.param.watch(partial(panelfunc.fill_hidden_table, 
            #                                          treatment_table, #THIS ONE
            #                                            "treatment_uuid", #addrow_uuid (GOOD)
            #                                            "treatment_nickname", #addrow_uuid (GOOD)
            #                                            treatmentgroup_table, #affected_table (GOOD)
            #                                            "treatmentgroup_uuid", #affected_uuid (ADD)
            #                                            "treatmentgroup_name", #affected_uuid (ADD)
            #                                            folder,
            #                                            "",
            #                                            "folder_uuid",
            #                                            treatment_widget_row, 
            #                                            treatmentgroup_sample_widget_template,
            #                                            "treatmentgroup_uuid",
            #                                            "treatmentgroup_name",
            #                                            "add_widget",
            #                                            "",
            #                                            "Treatment Group"
            #                                        ), #affected_cols (ADD)),
            #                                            ['value']
            #                                            )
            #has_multiple_treatment_switch.param.trigger()

            # Format widgets into panes (i.e., accordions, columns, tabs)
            biomaterial_tab_known =  gfc([biomaterial_markdown_known,
                                                gfw(pn.Row(add_knownbiomaterial_button, delete_knownbiomaterial_button)),
                                                knownbiomaterial_table])


            #____________________SECTION 4 (COMMERCIAL MOUSE MODEL)____________________
            # Create headers, definitions, etc.
            biomaterial_markdown_mouse = pn.pane.Markdown("""Please select the commercially available mouse model.""")
    
            # Create table
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
                                                                                                        titles = {  "biomaterial_nickname": "Biomaterial Label",
                                                                                                                    "strain":"Strain (JAX Laboratory ID)",
                                                                                                                    "age":"Age (Year-Month-Week)",
                                                                                                                    "cell": "Cell",
                                                                                                                    "sex":"Sex",
                                                                                                                    "tissue": "Tissue"},
                                                                                                        name = "Mouse Model",
                                                                                                        save_button = save_button,
                                                                                                        addrow_uuid = "biomaterial_uuid", 
                                                                                                        addrow_newname = "biomaterial_nickname",
                                                                                                        widget_row = biomaterial_widget_row, 
                                                                                                        widget_template = biomaterialgroup_sample_widget_template, 
                                                                                                        effect_type="add_widget",
                                                                                                        hidden_columns = ["biomaterial_uuid","folder_uuid"])
           
            # Format widgets into panes (i.e., accordions, columns, tabs)
            biomaterial_tab_mouse = gfc([biomaterial_markdown_mouse,
                                              gfw(pn.Row(add_mouse_button, delete_mouse_button)),
                                              mouse_table])
            
        
            
    
            #____________________PUT ALL SECTIONS TOGETHER____________________
            
            biomaterial_tab_custom.visible=False
            biomaterial_tab_mouse.visible=False
            #biomaterial_tab_known.visible=False
            
            biomaterial_tab= pn.Column(biomaterial_section_head1,
                                        pn.Accordion(("Step 1: Define biomaterial type",biomaterial_section_head2),
                                        ("Step 2: Describe biomaterial", pn.Column(biomaterial_tab_custom,biomaterial_tab_known,biomaterial_tab_mouse)),
                                        ("Step 3: Assign treatments to samples", biomaterial_tab_tail),
                                        ("Optional", biomaterial_accordion1)))
            

            pn.bind(panelfunc.show_section, 
                    biomaterial_toggle, 
                    {"Custom Biomaterial":biomaterial_tab_custom,
                    "Commercial Cell Line":biomaterial_tab_known, 
                     "Commercial Mouse Model": biomaterial_tab_mouse},
                    "Toggle", 
                    #biomaterial_tab_tail,
                    watch=True)
            
            tabs.append(("Biomaterial", biomaterial_tab))
            
            # %%-----------------------------------Samples tab----------------------------------------




            # %%-----------------------------------Conditions tab-------------------------------------
            
            #____________________SECTION 1 (DISEASES TO BIOMATERIAL)____________________
            # Create headers, definitions, etc.
            disease_markdown_1 = pn.pane.Markdown("""# Disease""")
            disease_markdown_2 = pn.pane.Markdown("""### Are there any diseases associated with your biomaterial?""")
            disease_markdown_3 = pn.pane.Markdown("""Please define the diseases in further detail.""")
            #disease_markdown_4 = pn.pane.Markdown("""### Step 2: Assign samples to disease groups""")
            
            # Create widgets
            has_disease_switch = pn.widgets.Switch(name="Disease Switch",width = 30)
            disease_sample_widget_template = panelfunc.get_widgets(widget_name='Disease Samples', **widget_kwargs, visible = False)
            disease_widget_row = pn.Row()

            # Create tables
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
                                                                                                        titles = { "disease_nickname": "Disease Label",
                                                                                                                  "disease_name": "Disease Name",
                                                                                                                  "stage": "Stage"},
                                                                                                        name = "Disease",
                                                                                                        save_button = save_button,
                                                                                                        addrow_uuid = "disease_uuid", 
                                                                                                        addrow_newname = "disease_nickname",
                                                                                                        widget_row = disease_widget_row, 
                                                                                                        widget_template = disease_sample_widget_template, 
                                                                                                        effect_type="add_widget",
                                                                                                        hidden_columns = ["disease_uuid","folder_uuid"])
            

            # Format widgets into panes (i.e., accordions, columns, tabs)
            disease_tab1 = gfc([disease_markdown_2, gfw(has_disease_switch)])
            disease_tab2 = gfc([disease_markdown_3, gfw(pn.Row(add_disease_button, delete_disease_button)),disease_table])
            disease_tab3 = gfc([disease_widget_row])
            
            disease_tab123 = pn.Accordion(("Step 1: Describe diseases and stages", disease_tab2),
                                          ("Step 2: Assign disease to sample", disease_tab3))


            #____________________SECTION 2 (DISEASES STUDIED)____________________
            # Create headers, definitions, etc.
            disease_markdown_5 = pn.pane.Markdown("""### If not, are there any diseases that this project aims to study indirectly?""")
            disease_markdown_6 = pn.pane.Markdown("""Select the diseases being studied. """)

            # Create widgets
            studying_disease_switch = pn.widgets.Switch(name="Disease Study Switch",width = 30)
            disease_study_widget = panelfunc.get_widgets(widget_name='Disease', **widget_kwargs)
            
            # Format widgets into panes (i.e., accordions, columns, tabs)
            disease_tab4 = gfc([disease_markdown_5, gfw(studying_disease_switch)])
            disease_tab5 = gfw(pn.Column(disease_markdown_6, disease_study_widget))


            # ____________________PUT TOGETHER ALL SECTIONS____________________
            disease_tab123.visible=False
            disease_tab5.visible=False
            disease_tab = pn.Column(gfc([disease_markdown_1]),
                                    gfc([disease_tab1, disease_tab123]), 
                                    pn.layout.Divider(),
                                    gfc([disease_tab4, disease_tab5]))

            pn.bind(panelfunc.show_section, has_disease_switch, disease_tab123, watch=True)
            pn.bind(panelfunc.show_section, studying_disease_switch, disease_tab5, watch=True)
            tabs.append(("Disease", disease_tab))




            # %%-----------------------------------Genes tab------------------------------------------
            
            #____________________SECTION 1 (GENES TO BIOMATERIAL)____________________
            # Create headers, definitions, etc.
            gene_markdown_1 = pn.pane.Markdown("""# Gene Modifications""")
            gene_markdown_2 = pn.pane.Markdown("""### Are there any genes associated with your biomaterial?""")
            gene_markdown_3 = pn.pane.Markdown("""Please define any genetic modification performed in further detail.""")
            #gene_markdown_4 = pn.pane.Markdown("""### Step 2: Assign samples to gene groups""")


            # Create widgets
            has_gene_switch = pn.widgets.Switch(name="Gene Switch",width = 30)
            gene_sample_widget_template = panelfunc.get_widgets(widget_name='Gene Samples', **widget_kwargs, visible = False)
            gene_widget_row = pn.Row()
            gene_widget_row.options = gene_sample_widget_template.options

            # Create tables
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
                                                                                                        titles = { "gene_nickname": "Gene Label",
                                                                                                                  "gene_name": "Gene Name",
                                                                                                                  "knockout": "If modified, how?"},
                                                                                                        name = "gene",
                                                                                                        save_button = save_button,
                                                                                                        addrow_uuid = "gene_uuid", 
                                                                                                        addrow_newname = "gene_nickname",
                                                                                                        widget_row = gene_widget_row, 
                                                                                                        widget_template = gene_sample_widget_template, 
                                                                                                        effect_type="add_widget",
                                                                                                        hidden_columns = ["gene_uuid","folder_uuid"])
            
            # Format widgets into panes (i.e., accordions, columns, tabs)
            gene_tab1 = gfc([gene_markdown_2, gfw(has_gene_switch)])
            gene_tab2 = gfc([gene_markdown_3, gfw(pn.Row(add_gene_button, delete_gene_button)),gene_table])
            gene_tab3 = gfc([gene_widget_row])
            
            gene_tab123 = pn.Accordion(("Step 1: Describe genes and modifications", gene_tab2),
                                          ("Step 2: Assign genes to sample", gene_tab3))




            #____________________SECTION 2 (GENES STUDIED)____________________
            # Create headers, definitions, etc.
            gene_markdown_5 = pn.pane.Markdown("""### If not, are there any genes that this project aims to study indirectly?""")
            gene_markdown_6 = pn.pane.Markdown("""Select the genes being studied. """)

            # Create widgets
            studying_gene_switch = pn.widgets.Switch(name="Gene Study Switch",width = 30)
            gene_study_widget = panelfunc.get_widgets(widget_name='Gene', **widget_kwargs)    
            
            # Format widgets into panes (i.e., accordions, columns, tabs)
            gene_tab4 = gfc([gene_markdown_5, gfw(studying_gene_switch)])
            gene_tab5 = gfw(pn.Column(gene_markdown_6, gene_study_widget))


            # ____________________PUT TOGETHER ALL SECTIONS____________________
            gene_tab123.visible=False
            gene_tab5.visible=False
            gene_tab = pn.Column(gfc([gene_markdown_1]),
                                    gfc([gene_tab1, gene_tab123]), 
                                    pn.layout.Divider(),
                                    gfc([gene_tab4, gene_tab5]))

            pn.bind(panelfunc.show_section, has_gene_switch, gene_tab123, watch=True)
            pn.bind(panelfunc.show_section, studying_gene_switch, gene_tab5, watch=True)
            tabs.append(("Gene Modification", gene_tab))





            # %%-----------------------------------Save and return tabs-------------------------------
            save_button.visible = True
            return tabs

    else:
        return "No project selected"





# %% CREATE MAIN PANEL COMPONENTS
save_button = pn.widgets.Button(name='Save', 
                                button_type='primary', 
                                visible=False)

username_input = pn.widgets.TextInput(name="Username", 
                                      placeholder="Enter your username here...")

folder_mc = pn.widgets.MultiChoice(name="Project Name", 
                                   max_items=1, 
                                   option_limit=200, 
                                   visible=False, 
                                   placeholder="Please select a project to edit...")

folder_tab = pn.bind(update_folder_by_username, username_input)
main_tab = pn.bind(update_tabs_with_folder, folder_mc)

# %% POPULATE TEMPLATE AND SERVE
template = pn.template.MaterialTemplate(
        site="Coriell",
        title="Research Data Manager (Submission Form)",
        sidebar=pn.Column(username_input,
                          folder_tab),
        main = pn.Column(pn.Spacer(height=35),
                         save_button,
                         pn.Spacer(height=35),
                         main_tab))


template.servable()
