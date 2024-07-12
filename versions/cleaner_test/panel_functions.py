# %% INITIAL SETUP

# Import required libraries
import pandas as pd
import requests
import time
import uuid
from functools import partial
import warnings
#warnings.filterwarnings('ignore')


# Import panel library and extensions
import panel as pn
from bokeh.models.widgets.tables import NumberFormatter, BooleanFormatter
pn.extension(design="material", sizing_mode="stretch_width")
pn.extension('tabulator')
#pn.extension('tabulator-tables')
pn.extension('mathjax')

# Import custom functions
import functions.sql_functions as sqlfunc
import functions.api_functions as apifunc
import functions.searchtab_functions as searchfunc

stylesheet = """
            .tabulator-cell {
                font-size: 14px;
                font-weight: normal;
            }
            .tabulator-col-title {
                font-size: 14px;
                font-weight: 600;
            }
            .bk-btn {
                font-size: 14px;
                font-weight: normal;
            }
            .widget-button .bk-btn .bk-btn-group button {
                font-size: 10px,
            }
            """

#---------------------------------------------------------CREATE WIDGETS/TABLES---------------------------------------------------------
def get_widgets(widget_row, save_button, folder = "", sample="", visible=True):

    #print(widget_row["widget_name"])
    #---------------------------------
    # Initialize widget based on type
    if widget_row["widget_type"] == "TextAreaInput":
        widget = pn.widgets.TextAreaInput()

    elif "MultiChoice" in widget_row["widget_type"]:
        widget = pn.widgets.MultiChoice()

    #--------------------------------- 
    # Set styling / parameters
    widget.name = widget_row["widget_name"]

    if str(widget_row["height"]) != "nan" :
        widget.height = int(widget_row["height"])

    if widget_row["widget_type"] == "MultiChoice (one)":
        widget.max_items = 1

    if str(widget_row["description"]) != "nan":
        widget.description = str(widget_row["description"])

    if visible == False:
        widget.visible = False

    #---------------------------------
    # Get options
    if widget_row["options_type"] == "table":

        if str(widget_row["parameters_filter"]) != "nan":
            filter = " where parameter.parameter_type = '" + widget_row["parameters_filter"] + "'"
        
        elif widget_row["widget_name"] in ["Treatment Samples","Biomaterial Samples","Disease Samples","Gene Samples"]:
            filter = " where sample.folder_uuid = '" + str(folder) + "' "

        else:
            filter = ""

        if str(widget_row["misc_filter"]) != "nan":
            filter += widget_row["misc_filter"]

        options = sqlfunc.get_options(table_name = widget_row["options_table"], 
                                      field_name = widget_row["options_column"], 
                                      field_uuid = widget_row["options_uuid"],
                                      filter     = filter)
        
        widget.options = options
        

    else:
        options = {}
    
    #---------------------------------
    # Get values
    value = sqlfunc.get_values( options = options,
                                values_type = widget_row["values_type"],
                                values_table = widget_row["values_table"],
                                values_column = widget_row["values_column"],
                                values_uuid = widget_row["values_uuid"],
                                links_to = widget_row["links_to"],
                                folder = folder,
                                sample = sample)
        
    widget.value = value
    #---------------------------------
    # Bind updates
    update_commands = pn.bind(sqlfunc.get_update_commands, 
                                widget,
                                original_values = value,
                                values_type = widget_row["values_type"],
                                values_table = widget_row["values_table"],
                                values_column = widget_row["values_column"],
                                values_uuid = widget_row["values_uuid"],
                                links_to = widget_row["links_to"],
                                folder = folder,
                                sample = sample)
    
    pn.bind(sqlfunc.execute_update_commands, 
            save_button, 
            update_commands, 
            watch=True)

    return widget

def get_cloned_widget(clone, new_name, widget_row, is_new = ""):

    # Must stay nested -- no optional keyword arguments for param.watch
    def remove_from_options(*events):

            for event in events:
                if event.name == "value":
                    
                    # Current option
                    old = set(event.old)
                    new = set(event.new)
                    delete_objs = old - new
                    add_objs = new - old

                    # Apply changes to widgets
                    for w in widget_row:
                        if w != event.obj:
                            for add_obj in add_objs:
                                options = {k:v for k,v in w.options.items() if v != add_obj}
                                w.options =  options
                                w.param.trigger('options') # Needs to be called to re-render the affected table

                            for delete_obj in delete_objs:
                                options = w.options
                                options.update({k:v for k,v in widget_row.options.items() if v == delete_obj})
                                w.options = options
                                w.param.trigger('options') # Needs to be called to re-render the affected table

    new_w = clone.clone()
    new_w.visible = True
    new_w.param.update(name=str(new_name))
    new_w.param.watch(remove_from_options, ["options","value"])
    new_w.param.trigger('name') # Needs to be called to re-render the affected table

    if is_new == True:
        # Find which values are already in use
        add_objs = set()
        for _, w in enumerate(widget_row):
            add_objs.update(set(w.value))
        
        # Remove them as an option for this widget
        for add_obj in add_objs:
            options = {k:v for k,v in new_w.options.items() if v != add_obj}
            new_w.options =  options
            new_w.param.trigger('options') # Needs to be called to re-render the affected table
        
    return new_w

def get_table(sql_table, sql_columns, sql_linksto, 
                formatters, editors, titles, 
                hidden_columns, name, 
                widget_row="", widget_template="", addrow_uuid="", addrow_newname="", affected_table = "", effect_type="",
                selectable = 'toggle', show_index=False,
                folder = "", sample = ""):
    
    df = sqlfunc.get_table_values(table = sql_table, columns = sql_columns, links_to = sql_linksto, 
                                    folder = folder, sample = sample)
    


    table = pn.widgets.Tabulator(df, 
                                    hidden_columns = hidden_columns,
                                    editors = editors,
                                    formatters = formatters,
                                    titles = titles,
                                    show_index = show_index,
                                    selectable = selectable,
                                    stylesheets = [stylesheet])
    
    table.on_edit(lambda e: edit_name(event=e, table = table, affected_table = affected_table, table_uuid = addrow_uuid, widget_row = widget_row))

    add_button = pn.widgets.Button(name="Add " + name, width = 200)
    add_button.on_click(partial(add_row, folder, name, widget_row, widget_template,table, addrow_uuid, addrow_newname, sql_linksto, affected_table, effect_type))
    
    delete_button = pn.widgets.Button(name="Remove " + name, width = 200, button_style='outline', button_type='danger')
    delete_button.on_click(partial(delete_row, table, addrow_uuid, addrow_newname, affected_table, effect_type, widget_row))

    

    #update_commands = pn.bind(sqlfunc.get_update_commands, 
    #                            widget,
    #                            original_values = value,
    #                            values_type = widget_row["values_type"],
    #                            values_table = widget_row["values_table"],
    #                            values_column = widget_row["values_column"],
    #                            values_uuid = widget_row["values_uuid"],
    #                            links_to = widget_row["links_to"],
    #                            folder = folder,
    #                            sample = sample)
    
    #pn.bind(sqlfunc.execute_update_commands, 
    #        save_button, 
    #        update_commands, 
    #        watch=True)
            
    return table, add_button, delete_button

#---------------------------------------------------------UPDATE WIDGETS/TABLES---------------------------------------------------------
def get_df_from_table(table):

    df = table.value[[col for col in table.value.columns if col != "index"]]

    if "index" in table.value.columns:
        df.index = table.value["index"]
    
    new_index = df.index.max()

    if str(new_index) == "nan":
        new_index=1 
    else:
        new_index+=1

    new_titles = table.titles
    new_editors = table.editors
    new_formatters = table.formatters

    return df, new_index, new_titles, new_editors, new_formatters

#---------------------------------------------------------
def set_table_from_df(df, table, titles = "", editors = "", formatters = "", clear_selection=False):
    table.value = df
    table.value["index"] = df.index

    if titles != "":
        table.titles = titles
    
    if editors != "":
        table.editors = editors
    
    if formatters != "":
        table.formatters = formatters

    if clear_selection == True:
        table.selection = []

#---------------------------------------------------------
def edit_name(event, table, table_uuid, affected_table="", widget_row = ""):

    if event.column in ["treatment_nickname"]:
        
        row = table.value.iloc[event.row]
        update_uuid = row[table_uuid]
        update_title = event.value

        affected_df, _, new_titles, _, _ = get_df_from_table(table = affected_table)
        new_titles.update({str(update_uuid):update_title})
        affected_table.param.update(titles=new_titles)
        set_table_from_df(df = affected_df, table = affected_table, titles = new_titles)
        affected_table.param.trigger('value') # Needs to be called to re-render the affected table

    elif event.column in ["treatmentgroup_name", "biomaterial_nickname","disease_nickname", "gene_nickname"]:
        for idx, w in enumerate(widget_row):
            
            if w.name == event.old:

                new_w = get_cloned_widget(clone = w.clone(), new_name = str(event.value), widget_row = widget_row)
                widget_row[idx] = new_w
        
#---------------------------------------------------------
def add_row(folder, name, widget_row, widget_template, 
            main_table, main_uuid, main_newname, links_to,
            affected_table="", effect_type="", 
            event=""):
    
    #------------------Add new row------------------
    main_df, new_index, _, _, _ = get_df_from_table(table = main_table)

    # Add new row to Treatment table
    new_uuid = uuid.uuid4()

    main_df.loc[new_index] = ""
    main_df.loc[new_index, main_newname] = name.title() + " " + str(new_index)
    main_df.loc[new_index, main_uuid] = str(new_uuid)
    main_df.loc[new_index, links_to] = folder
    set_table_from_df(df = main_df, table = main_table)
        
    #------------------Execute effects------------------
    if effect_type == "add_column":

        affected_df, _, new_titles, new_editors, new_formatters = get_df_from_table(table = affected_table)
        affected_df[str(new_uuid)] = 0
        new_titles.update({str(new_uuid): name.title() + " " + str(new_index)})
        new_editors.update({str(new_uuid):{'name':str(new_uuid), 'type': 'tickCross'}})
        new_formatters.update({str(new_uuid):BooleanFormatter()})
        
        set_table_from_df(df = affected_df, table = affected_table, titles = new_titles, editors=new_editors, formatters = new_formatters)

    elif effect_type == "add_widget":

        new_widget = get_cloned_widget(clone = widget_template.clone(), 
                                       new_name = name.title() + " " + str(new_index), 
                                       widget_row = widget_row, 
                                       is_new = True)

        widget_row.append(new_widget)

#--------------------------------------------------------- 
def delete_row(main_table, main_uuid, main_name, affected_table, effect_type,  widget_row="",  event=""):
    
    to_remove_idx = main_table.selected_dataframe["index"].unique()
    to_remove_uuid = main_table.selected_dataframe[main_uuid].unique()

    main_df, _, _, _, _ = get_df_from_table(table = main_table)
    main_df = main_df.drop(to_remove_idx)

    if effect_type == "add_column":

        affected_df, _, _, _, _  = get_df_from_table(affected_table)
        good_cols = [col for col in affected_df.columns if col not in to_remove_uuid]
        affected_df = affected_df[good_cols]
        set_table_from_df(df = affected_df, table = affected_table)

    elif effect_type == "add_widget":

        delete_objs = set()
        i=0
        for _, w in enumerate(widget_row):
            if w.name in main_table.selected_dataframe[main_name].unique():
                delete_objs.update(set(w.values))
                widget_row.pop(i)
                i-=1
            i+=1
        
        for _, w in enumerate(widget_row):
            for delete_obj in delete_objs:
                options = w.options
                options.update({k:v for k,v in widget_row.options.items() if v == delete_obj})
                w.options = options
                w.param.trigger('options') # Needs to be called to re-render the affected table

    set_table_from_df(df = main_df, table = main_table, clear_selection = True)
    
#--------------------------------------------------------- 
def show_section(switch, section, switch_type = "Switch", lower_section=""):
    if switch_type == "Switch":
        if switch == False:
            section.visible=False

        elif switch == True:
            section.visible=True

    elif switch_type == "Toggle":
        print(switch)
        if len(switch)>0:
            for tog, sec in section.items():
                if tog!=switch[0]:
                    sec.visible = False
                elif tog == switch[0]:
                    sec.visible = True
                    lower_section.visible=True



"""

    if button == "treatment1":
        
        treatment_df, _, _, _, _ = get_df_from_table(table = treatment_table)
        treatment_df = treatment_df.drop(treatment_table.selected_dataframe["index"].unique())
        set_table_from_df(df = treatment_df, table = treatment_table, clear_selection = True)
        
        treatmentgroup_df, _, _, _, _  = get_df_from_table(treatmentgroup_table)

        good_cols = ["group_name","treatment_group_uuid","folder_uuid"] + [col for col in treatmentgroup_df.columns 
                            if col in treatment_df["treatment_uuid"].unique()]
        
        treatmentgroup_df = treatmentgroup_df[good_cols]
        set_table_from_df(df = treatmentgroup_df, table = treatmentgroup_table)


    elif button == "treatment_group":
        
        treatmentgroup_df, _, _, _, _  = get_df_from_table(table = treatmentgroup_table)
        treatmentgroup_df = treatmentgroup_df.drop(treatmentgroup_table.selected_dataframe["index"].unique())
        
        i=0
        for idx, w in enumerate(widget_row):
            if w.name in treatmentgroup_table.selected_dataframe["group_name"].unique():
                widget_row.pop(i)
                i-=1
            i+=1

        set_table_from_df(df = treatmentgroup_df, table = treatmentgroup_table, clear_selection = True)

        
"""

        
        

                      
#---------------------------------------------------------CREATE TABS---------------------------------------------------------


"""
def add_row1(treatment_df, treatmentgroup_df,  treatment_table, treatmentgroup_table, 
            folder, button, widget_row, widget_template, 
            main_table, main_df1, main_uuid, affected_table, affected_df, 
            event):
    
    # Must stay nested -- no optional keyword arguments for param.watch
    def remove_from_options(*events):
        #print(events)
        for event in events:
            if event.name == "value":
                
                # Current option
                old = set(event.old)
                new = set(event.new)
                delete_objs = old - new
                add_objs = new - old

                # Apply changes to widgets
                for w in widget_row:
                    if w != event.obj:
                        for add_obj in add_objs:
                            options = {k:v for k,v in w.options.items() if v != add_obj}
                            w.options =  options
                            w.param.trigger('options') # Needs to be called to re-render the affected table

                        for delete_obj in delete_objs:
                            options = w.options
                            options.update({k:v for k,v in widget_row.options.items() if v == delete_obj})
                            w.options = options
                            w.param.trigger('options') # Needs to be called to re-render the affected table


    if button == "treatment":
        
        #------------------Add to treatment------------------
        main_df, new_index, _, _, _ = get_df_from_table(table = main_table)

        # Add new row to Treatment table
        new_uuid = uuid.uuid4()

        cols = main_df.columns 

        #cols = ["treatment_nickname","treatment_name", 
        #        "dosage", "dosage_units", "timing", "timing_units",
        #        "treatment_uuid","folder_uuid"]

        main_df.loc[new_index] = ""
        main_df.loc[new_index]["treatment_nickname"] = "Treatment " + str(new_index)
        main_df.loc[new_index]["treatment_uuid"] = str(new_uuid)
        main_df.loc[new_index]["folder_uuid"] = folder

        #main_df.loc[new_index][cols] = ["Treatment " + str(new_index),
        #                                "Enter Name",
        #                                "Enter Dosage",
        #                                "Enter Dosage Units",
        #                                "Enter Timing",
        #                                "Enter Timing Units",
        #                                str(new_uuid),
        #                                folder]
        
        set_table_from_df(df = main_df, table = main_table)
        
        #------------------Add to treatment group------------------
        # Add new column to Treatment Groups table
        treatmentgroup_df, _, new_titles, new_editors, new_formatters = get_df_from_table(table = treatmentgroup_table)
        
        treatmentgroup_df[str(new_uuid)] = 0
        new_titles.update({"treatmentgroup_name": "Group Name"})
        new_titles.update({str(new_uuid): "Treatment " + str(new_index)})
        new_editors.update({str(new_uuid):{'name':str(new_uuid), 'type': 'tickCross'}})
        new_formatters.update({str(new_uuid):BooleanFormatter()})
        
        set_table_from_df(df = treatmentgroup_df, table = treatmentgroup_table, titles = new_titles, editors=new_editors, 
                            formatters = new_formatters)

    elif button == "treatment1":
        
        #------------------Add to treatment------------------
        treatment_df, new_index, _, _, _ = get_df_from_table(table = treatment_table)

        # Add new row to Treatment table
        treatment_uuid = uuid.uuid4()

        cols = ["treatment_nickname","treatment_name", 
                "dosage", "dosage_units", "timing", "timing_units",
                "treatment_uuid","folder_uuid"]

        treatment_df.loc[new_index] = ""
        treatment_df.loc[new_index][cols] = ["Treatment " + str(new_index),
                                "Enter Name",
                                "Enter Dosage",
                                "Enter Dosage Units",
                                "Enter Timing",
                                "Enter Timing Units",
                                str(treatment_uuid),
                                folder]
        
        set_table_from_df(df = treatment_df, table = treatment_table)
        
        #------------------Add to treatment group------------------
        # Add new column to Treatment Groups table
        treatmentgroup_df, _, new_titles, new_editors, new_formatters = get_df_from_table(table = treatmentgroup_table)
        
        treatmentgroup_df[str(treatment_uuid)] = 0
        new_titles.update({"treatmentgroup_name": "Group Name"})
        new_titles.update({str(treatment_uuid): "Treatment " + str(new_index)})
        new_editors.update({str(treatment_uuid):{'name':str(treatment_uuid), 'type': 'tickCross'}})
        new_formatters.update({str(treatment_uuid):BooleanFormatter()})
        
        set_table_from_df(df = treatmentgroup_df, table = treatmentgroup_table, titles = new_titles, editors=new_editors, 
                            formatters = new_formatters)

    
    elif button == "treatment_group":

        #------------------Add to treatment group------------------
        # Add new row to Treatment table
        treatmentgroup_uuid = uuid.uuid4()

        treatmentgroup_df, new_index, _, _, _ = get_df_from_table(table = treatmentgroup_table)
        cols = treatmentgroup_df.columns #["treatmentgroup_name", "treatmentgroup_uuid", "folder_uuid"] + [0] * (len(treatmentgroup_df.columns) - 3)

        treatmentgroup_df.loc[new_index] = ""
        treatmentgroup_df.loc[new_index][cols] = ["Treatment Group " + str(new_index), 
                                                    str(treatmentgroup_uuid), 
                                                    folder] + [0] * (len(treatmentgroup_df.columns) - 3)
        
        set_table_from_df(df = treatmentgroup_df, table = treatmentgroup_table)

        # Add new treatment widget 
        new_widget = widget_template.clone()
        new_widget.name =  "Treatment Group " + str(new_index) #treatmentgroup_df.loc[new_index]["group_name"] #
        new_widget.visible = True

        new_widget.param.watch(remove_from_options, ["options","value"])
        
        widget_row.append(new_widget)
        

"""


#def get_nonsearchable_folder_tab(tab_layout, header, folder_mc1, save_button):
#    tab = pn.Column(header)#

#    for folder in folder_mc1:
#        for _, row in tab_layout.iterrows():
#            widget = get_widgets(widget_row = row, 
#                                 save_button = save_button, 
#                                 folder = folder)
#            tab.append(pn.Row(pn.Spacer(height=20)))
#            tab.append(pn.Row(pn.Spacer(width=30),widget))
#            tab.append(pn.Row(pn.Spacer(height=20)))
    
#    return tab
            

"""
def get_nonsearchable_folder_tab(tab_layout, tab_name, header, folder_mc1, save_button):
    tab = pn.Column(header)

    for index, row in tab_layout.iterrows():

        if "MultiChoice" in row["widget_type"]:

            if row["options_type"] == "table":

                options = sqlfunc.get_options(table_name = row["table_name"], 
                                              field_name = row["column_name"], 
                                              field_uuid = row["table_uuid"])
                options = {v:k for k,v in sorted(options.items(), key=lambda item: item[1])}

                values = sqlfunc.get_values(table_view = row["table_view"], 
                                            table_name = row["table_name"], 
                                            field_name = row["column_name"], 
                                            field_uuid = row["table_uuid"],
                                            folder_mc1 = folder_mc1)
                
                row_widget = pn.widgets.MultiChoice(name=row["column_title"], options=options, value=values)

                if str(row["geo_description"]) != "nan":
                    row_widget.description = str(row["geo_description"])

                if row["widget_type"] == "MultiChoice (one)":
                    row_widget.max_items = 1

                update_commands = pn.bind(sqlfunc.get_update_commands, 
                                          row_widget, 
                                          row["table_view_editable"],
                                          row["table_view"], 
                                          row["table_uuid"], 
                                          folder_mc1, 
                                          is_searchable=False)
                
                pn.bind(sqlfunc.execute_update_commands, 
                        save_button, 
                        update_commands, 
                        watch=True)
                
                tab.append(pn.Row(pn.Spacer(height=20)))
                tab.append(pn.Row(pn.Spacer(width=30),row_widget))
                tab.append(pn.Row(pn.Spacer(height=20)))
            
            #elif row["options_type"] == "multichoice_options":


        
        
        elif (row["widget_type"] == "TextAreaInput"):
            
            value = sqlfunc.get_text_values(field_name = row["column_name"], 
                                            folder_mc1 = folder_mc1)

            
            row_widget = pn.widgets.TextAreaInput(name=row["column_title"], 
                                                  value=value, 
                                                  height=int(row["height"]))
            if str(row["geo_description"]) != "nan":
                row_widget.description = str(row["geo_description"])
                
            update_commands = pn.bind(sqlfunc.get_text_update_commands, 
                                      row_widget, 
                                      row["column_name"], 
                                      folder_mc1)
            
            pn.bind(sqlfunc.execute_update_commands,
                     save_button, 
                     update_commands, 
                     watch=True)

            tab.append(pn.Row(pn.Spacer(height=20)))
            tab.append(pn.Row(pn.Spacer(width=30),row_widget))
            tab.append(pn.Row(pn.Spacer(height=20)))
        
    return tab
"""



#---------------------------------------------------------CREATE TABS---------------------------------------------------------








#---------------------------------------------------------CREATE TABS---------------------------------------------------------
"""
def get_nonsearchable_sample_tab(tab_layout, header, folder_mc1):
    
    # Get all samples linked to this folder
    for val in folder_mc1:

        tabulator_formatters = {}
        df = sqlfunc.get_folder_samples(folder_val = str(val))
        tabulator_titles = {"sample_name": "Library Name (Sample Name)",
                            "sample_id": "Title (Sample ID)"}
        tabulator_editors = {"sample_name":None, "sample_id":None}  
        all_options = {}
        mc_row = pn.Row(pn.Spacer(width=170))

        for _, row in tab_layout.iterrows():

            col_name = row["column_name"]
            col_title = row["column_title"]
            options = sqlfunc.get_options(table_name = row["table_name"], 
                                          field_name = col_name, 
                                          field_uuid = row["table_uuid"])

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
# %%
