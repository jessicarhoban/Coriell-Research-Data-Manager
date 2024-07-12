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

# Import custom functions
import functions.sql_functions as sqlfunc
import functions.api_functions as apifunc
import functions.searchtab_functions as searchfunc


#---------------------------------------------------------CREATE TABS---------------------------------------------------------
def get_nonsearchable_folder_tab(tab_layout, tab_name, header, folder_mc1, save_button):
    tab = pn.Column(header)

    for index, row in tab_layout.iterrows():

        if "MultiChoice" in row["widget_type"]:

            options = sqlfunc.get_options(row["table_name"], row["column_name"], row["table_uuid"])
            options = {v:k for k,v in sorted(options.items(), key=lambda item: item[1])}
            values = sqlfunc.get_values(row["table_view"], row["table_name"], row["column_name"], row["table_uuid"],folder_mc1)
            row_widget = pn.widgets.MultiChoice(name=row["column_title"], options=options, value=values)

            if str(row["geo_description"]) != "nan":
                row_widget.description = str(row["geo_description"])

            if row["widget_type"] == "MultiChoice (one)":
                row_widget.max_items = 1

            update_commands = pn.bind(sqlfunc.get_update_commands, row_widget, row["table_view_editable"],
                                      row["table_view"], row["table_uuid"], folder_mc1, is_searchable=False)
            pn.bind(sqlfunc.execute_update_commands, save_button, update_commands, watch=True)
            tab.append(pn.Row(pn.Spacer(height=20)))
            tab.append(pn.Row(pn.Spacer(width=30),row_widget))
            tab.append(pn.Row(pn.Spacer(height=20)))
        
        elif (row["widget_type"] == "TextAreaInput"):

            value = sqlfunc.get_text_values(row["column_name"], folder_mc1)

            row_widget = pn.widgets.TextAreaInput(name=row["column_title"], 
                                                  value=value, 
                                                  height=int(row["height"]))
            if str(row["geo_description"]) != "nan":
                row_widget.description = str(row["geo_description"])
                
            update_commands = pn.bind(sqlfunc.get_text_update_commands, 
                                      row_widget, 
                                      row["column_name"], folder_mc1)
            
            pn.bind(sqlfunc.execute_update_commands, save_button, update_commands, watch=True)

            tab.append(pn.Row(pn.Spacer(height=20)))
            tab.append(pn.Row(pn.Spacer(width=30),row_widget))
            tab.append(pn.Row(pn.Spacer(height=20)))
        
    return tab




#---------------------------------------------------------CREATE TABS---------------------------------------------------------








#---------------------------------------------------------CREATE TABS---------------------------------------------------------
def get_nonsearchable_sample_tab(tab_layout, header, folder_mc1):
    
    # Get all samples linked to this folder
    for val in folder_mc1:

        tabulator_formatters = {}
        df = sqlfunc.get_folder_samples(str(val))
        tabulator_titles = {"sample_name": "Library Name (Sample Name)",
                            "sample_id": "Title (Sample ID)"}
        tabulator_editors = {"sample_name":None, "sample_id":None}  
        all_options = {}
        mc_row = pn.Row(pn.Spacer(width=170))

        for _, row in tab_layout.iterrows():

            col_name = row["column_name"]
            col_title = row["column_title"]
            options = sqlfunc.get_options(row["table_name"], col_name, row["table_uuid"])

            top_mc = pn.widgets.MultiChoice(name=col_title, placeholder="Pre-filter options for samples", width=170,
                                            options = {v:k for k,v in sorted(options.items(), key=lambda item: str(item[1]))})#, reverse=True
            mc_row.append(top_mc)

            col_info = {'name':col_name,
                        'type': 'list',
                        'values':{k:v for k,v in sorted(options.items(), key=lambda item: str(item[1]))}}
            
            col_info['autocomplete'] = True
            col_info['listOnEmpty'] = True
            df[col_name] = ""
            #values = sqlfunc.get_values(table["table_view"], table["table_name"], col_name, table["table_uuid"], str(val))
            
            tabulator_editors[col_name] =  col_info
            tabulator_titles[col_name] = col_title
            tabulator_formatters[col_name] = {'type':'lookup'}
            tabulator_formatters[col_name].update(options)
            all_options[col_name] = options

            

        df = pd.DataFrame(df, dtype=str)
        tab_table = pn.widgets.Tabulator(df,
                                        editors = tabulator_editors, 
                                        formatters = tabulator_formatters, 
                                        width = 1200,
                                        widths = 170,
                                        #selection=[],
                                        selectable = 'toggle',
                                        #widths = {'index': '5%', 'A': '15%', 'B': '15%', 'C': '25%', 'D': '40%'},
                                        titles = tabulator_titles,
                                        layout='fit_data_stretch',
                                        hidden_columns = [col for col in df.columns if "uuid" in col],
                                        show_index=False,
                                        sorters= [{'field': 'sample_name', 'dir': 'asc'}])

        tab_table.on_edit(lambda e:print(e, tab_table.selection))
        #tab_table.add_filter(("Human"),"organism_name")

        tab = pn.Column(header, 
                        mc_row,
                        tab_table, 
                        pn.Spacer(styles=dict(background='white'),   sizing_mode='stretch_both'))

        return tab












"""
def get_nonsearchable_sample_tab(tab_layout, tab_name, header, folder_mc1):
  
    # Get all samples linked to this folder
    for val in folder_mc1:

        tabulator_formatters = {}

        df = sqlfunc.get_folder_samples(str(val))
        tabulator_titles = {"sample_name": "Sample Name"}
        tabulator_editors = {"sample_name":None}  
        tables = tab_layout.loc[tab_layout["tab_name"]==tab_name]
        all_options = {}
        mc_row = pn.Row(pn.Spacer(width=170))

        for _,table in tables.iterrows():
            columns = dict(zip([k for k in table["columns"]["key"]],[k for k in table["columns"]["value"]]))
            for col_name,col_title in columns.items():
                
                options = sqlfunc.get_options(table["table_name"], col_name, table["table_uuid"])
                
                col_info = {'name':col_name,
                            'type': 'list',
                            'values':{k:v for k,v in sorted(options.items(), key=lambda item: str(item[1]))}}
                
                
                df[col_name] = ""
                col_info['autocomplete'] = True
                col_info['listOnEmpty'] = True
                #values = sqlfunc.get_values(table["table_view"], table["table_name"], col_name, table["table_uuid"], str(val))
                
                tabulator_editors[col_name] =  col_info
                tabulator_titles[col_name] = col_title
                tabulator_formatters[col_name] = {'type':'lookup'}
                tabulator_formatters[col_name].update(options)
                all_options[col_name] = options

                top_mc = pn.widgets.MultiChoice(name=col_title, placeholder="Pre-filter options for samples", width=170,
                                                options = {v:k for k,v in sorted(options.items(), key=lambda item: str(item[1]))})#, reverse=True
                mc_row.append(top_mc)

        df = pd.DataFrame(df, dtype=str)
        tab_table = pn.widgets.Tabulator(df,
                                         editors = tabulator_editors, 
                                         formatters = tabulator_formatters, 
                                         width = 1200,
                                         widths = 170,
                                         #selection=[],
                                         selectable = 'toggle',
                                         #widths = {'index': '5%', 'A': '15%', 'B': '15%', 'C': '25%', 'D': '40%'},
                                         titles = tabulator_titles,
                                         layout='fit_data_stretch',
                                         hidden_columns = [col for col in df.columns if "uuid" in col],
                                         show_index=False,
                                         sorters= [{'field': 'sample_name', 'dir': 'asc'}])

        tab_table.on_edit(lambda e:print(e, tab_table.selection))
        #tab_table.add_filter(("Human"),"organism_name")

        tab = pn.Column(header, 
                        mc_row,
                        tab_table, 
                        pn.Spacer(styles=dict(background='white'),   sizing_mode='stretch_both'))

    return tab
"""