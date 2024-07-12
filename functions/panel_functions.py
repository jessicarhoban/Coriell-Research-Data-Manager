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

#---------------------------------------------------------CREATE WIDGETS/TABLES---------------------------------------------------------

def get_widgets(widget_name, tab_layout, save_button, folder = "", sample="", visible=True):

    widget_row = tab_layout.loc[tab_layout['widget_name']==widget_name].reset_index().iloc[0]

    #---------------------------------
    # Initialize widget based on type
    if widget_row["widget_type"] == "TextAreaInput":
        widget = pn.widgets.TextAreaInput()

    elif "MultiChoice" in widget_row["widget_type"]:
        widget = pn.widgets.MultiChoice()

    elif "RadioButton" in widget_row["widget_type"]:
        widget = pn.widgets.RadioButtonGroup()

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
                hidden_columns, name, save_button,
                widget_row="", widget_template="", addrow_uuid="", addrow_newname="", affected_table = "", effect_type="",
                selectable = 'toggle', show_index=False,
                folder = "", sample = "",
                hidden_table = False,
                fill_template = {},
                ):
    
    #df = sqlfunc.get_table_values(table = sql_table, columns = sql_columns, links_to = sql_linksto, 
    #                               folder = folder, sample = sample)

    df = sqlfunc.get_values(options = "", values_type ="complex_table_link", values_table = sql_table, 
                            values_column = sql_columns, values_uuid="", links_to = sql_linksto, 
                            sample = sample, folder = folder)

    
    table = pn.widgets.Tabulator(df, 
                                    hidden_columns = hidden_columns,
                                    editors = editors,
                                    formatters = formatters,
                                    titles = titles,
                                    theme = 'semantic-ui',
                                    show_index = show_index,
                                    selectable = selectable)
    
    table.on_edit(lambda e: edit_name(event=e, table = table, affected_table = affected_table, 
                                      table_uuid = addrow_uuid, widget_row = widget_row, 
                                      effect_type = effect_type, fill_template=fill_template))

    add_button = pn.widgets.Button(name="Add " + name, width = 200)
    add_button.on_click(partial(add_row, folder, sample, name, widget_row, widget_template,table, addrow_uuid, addrow_newname, 
                                sql_linksto, affected_table, effect_type, hidden_table, fill_template))
    
    delete_button = pn.widgets.Button(name="Remove " + name, width = 200, button_style='outline', button_type='danger')
    delete_button.on_click(partial(delete_row, table, addrow_uuid, addrow_newname, affected_table, 
                                   effect_type, widget_row, hidden_table, fill_template))

    #table.on_edit(lambda e: sqlfunc.get_update_commands
    update_commands = pn.bind(sqlfunc.get_update_commands, 
                              value_list = table.value, 
                              original_values = '', 
                              values_type = "complex_table_link", 
                              values_table = sql_table, 
                              values_column = '', 
                              values_uuid = '', 
                              links_to = sql_linksto, 
                              sample = sample, 
                              folder = folder)
                  
    pn.bind(sqlfunc.execute_update_commands, 
            save_button, 
            update_commands, 
            watch=True)
            
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

def edit_name(event, table, table_uuid, affected_table="", widget_row = "", effect_type="", fill_template=""):

    print(event)

    if event.column in ["treatmentgroup_name","treatment_nickname","biomaterial_nickname","gene_nickname","disease_nickname"]:

        if effect_type == "add_column":
            
            row = table.value.iloc[event.row]
            update_uuid = row[table_uuid]
            update_title = event.value

            affected_df, _, new_titles, _, _ = get_df_from_table(table = affected_table)
            new_titles.update({str(update_uuid):update_title})
            affected_table.param.update(titles=new_titles)
            set_table_from_df(df = affected_df, table = affected_table, titles = new_titles)
            affected_table.param.trigger('value') # Needs to be called to re-render the affected table
            
        elif effect_type == "add_widget":
            for idx, w in enumerate(widget_row):
                if w.name == event.old:
                    new_w = get_cloned_widget(clone = w.clone(), new_name = str(event.value), widget_row = widget_row)
                    widget_row[idx] = new_w
            
        if len(fill_template)>0:
            if fill_template["switch"].value == False:
                for idx, w in enumerate(widget_row):
                    if w.name == event.old:
                        new_w = get_cloned_widget(clone = w.clone(), new_name = str(event.value), widget_row = widget_row)
                        widget_row[idx] = new_w
        
def add_row(folder, sample, name, widget_row, widget_template, 
            main_table, main_uuid, main_newname, links_to,
            affected_table="", effect_type="", hidden_table=False, fill_template="",
            event=""):

    #------------------Add new row------------------
    main_df, new_index, _, _, _ = get_df_from_table(table = main_table)

    # Add new row to Treatment table
    new_uuid = uuid.uuid4()

    main_df.loc[new_index] = ""
    main_df.loc[new_index, main_uuid] = str(new_uuid)

    if links_to == "folder_uuid":
        main_df.loc[new_index, links_to] = folder

    elif links_to == "sample_uuid":
        main_df.loc[new_index, links_to] = sample

    #-----------------------------------
    if hidden_table == False:
        main_df.loc[new_index, main_newname] = name.title() + " " + str(new_index)

    elif hidden_table != False:
        main_df.loc[new_index, hidden_table["source_uuid"]] = 1
        main_df.loc[new_index, hidden_table["target_name_col"]] = hidden_table["source_name"]
        
    
    set_table_from_df(df = main_df, table = main_table)
    main_table.param.trigger('value') # Needs to be called to re-render the affected table      


    #------------------Execute effects------------------
    if effect_type == "add_column":

        affected_df, _, new_titles, new_editors, new_formatters = get_df_from_table(table = affected_table)
        affected_df[str(new_uuid)] = 0
        new_titles.update({str(new_uuid): name.title() + " " + str(new_index)})
        new_editors.update({str(new_uuid):{'name':str(new_uuid), 'type': 'tickCross'}})
        new_formatters.update({str(new_uuid):BooleanFormatter()})
        
        set_table_from_df(df = affected_df, table = affected_table, titles = new_titles, editors=new_editors, formatters = new_formatters)

    elif effect_type == "add_widget" :

        if hidden_table == False:
            new_name = name.title() + " " + str(new_index)
        else:
            new_name = hidden_table["source_name"]

        new_widget = get_cloned_widget(clone = widget_template.clone(), 
                                       new_name = new_name,
                                       widget_row = widget_row, 
                                       is_new = True)

        widget_row.append(new_widget)

    if len(fill_template)>0:
        if fill_template["switch"].value == False:
            new_name = name.title() + " " + str(new_index)
            
            new_widget = get_cloned_widget(clone = widget_template.clone(), 
                                        new_name = new_name,
                                        widget_row = widget_row, 
                                        is_new = True)

            widget_row.append(new_widget)

def delete_row(main_table, main_uuid, main_name, affected_table, effect_type,  widget_row="",  hidden_table=False, fill_template="", event=""):
    
    main_df, _, _, _, _ = get_df_from_table(table = main_table)

    if hidden_table == False:
        to_remove_idx = main_table.selected_dataframe["index"].unique()
        to_remove_uuid = main_table.selected_dataframe[main_uuid].unique()
        to_remove_name = main_table.selected_dataframe[main_name].unique()
        
    else:
    #    if hidden_table["type"]=="All":
    #        to_remove_idx = main_table.value["index"].unique()
    #        to_remove_uuid = main_table.value[main_uuid].unique()
    #        to_remove_name = main_table.value[main_name].unique()

    #    elif hidden_table["type"]=="Existing":
        to_remove_idx = hidden_table["to_remove_idx"]
        to_remove_uuid = hidden_table["to_remove_uuid"]
        to_remove_name = hidden_table["to_remove_name"]
    
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
            if w.name in to_remove_name:
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

    if len(fill_template)>0:
        if fill_template["switch"].value == False:
            delete_objs = set()
            i=0
            for _, w in enumerate(widget_row):
                if w.name in to_remove_name:
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
    
def show_section(switch, section, switch_type = "Switch", lower_section=""):
    if switch_type == "Switch":
        if switch == False:
            section.visible=False

        elif switch == True:
            section.visible=True

    elif switch_type == "Toggle":
        
        for tog, sec in section.items():
            if tog!=switch:
                sec.visible = False
            elif tog == switch:
                sec.visible = True
                if lower_section != "":
                    lower_section.visible=True

def get_formatted_widget(widget, type="widget"):
    return pn.Row(pn.Spacer(width=30),widget)

def get_formatted_column(objects):

    col = pn.Column(pn.Row(pn.Spacer(height=20)))

    for obj in objects:
        col.append(obj)
        col.append(pn.Row(pn.Spacer(height=20)))

    return col

def get_spacer(type, height=20, width=30):
    if type == "h":
        return pn.Row(pn.Spacer(height=height))
    elif type == "w":  
        return pn.Spacer(width=width)

def fill_hidden_table(source_table, source_uuid, source_name, target_table, target_uuid,
                      target_name, folder, sample, linksto, widget_row, widget_template, 
                      addrow_uuid, addrow_newname, effect_type, affected_table, name, *events):

    sql_linksto = linksto
    table = target_table

    to_remove_idx = target_table.value["index"].unique()
    to_remove_uuid = target_table.value[addrow_uuid].unique()
    to_remove_name = [w.name for w in widget_row] #target_table.value[addrow_newname].unique()
    to_delete = {"to_remove_idx":to_remove_idx,"to_remove_uuid":to_remove_uuid,"to_remove_name":to_remove_name}

    for event in events:

        if event.new == False:
            
            delete_row(table,  addrow_uuid, addrow_newname, affected_table, effect_type, widget_row, to_delete)

            # Iterate over unique source table values
            for i, unique_row in source_table.value[[source_uuid, source_name]].drop_duplicates().iterrows():
                
                unique_source_uuid = unique_row[source_uuid]
                unique_source_name = unique_row[source_name]

                to_send = {"source_uuid": unique_source_uuid,
                           "target_name_col":target_name, 
                           "source_name": unique_source_name}

                add_row(folder, sample, 
                        name, widget_row, widget_template,
                        table, addrow_uuid, addrow_newname, sql_linksto, 
                        affected_table, effect_type,
                        to_send)


        elif event.new == True:

            delete_row(table,  addrow_uuid, addrow_newname, affected_table, effect_type, widget_row, to_delete)



