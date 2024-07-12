# %% INITIAL SETUP

# Import required libraries
import requests
import panel as pn
import pandas as pd

# Import and establish database connection
import duckdb
con = duckdb.connect("./browser_cleaner.db")

#---------------------------------------------------------UPDATE DATABASE---------------------------------------------------------
# TO CLEAN
def execute_update_commands(event, commands):
    if not event:
        return
    for command in commands:
        print(command)
        #con.execute(command)

def get_update_commands(value_list, original_values, values_type, values_table, values_column, values_uuid, 
                        links_to, sample = "", folder = ""):
    commands = [] 

    if links_to == "folder_uuid":
        end_filter = [" where ", links_to, " = '", str(folder), "'"]
        end_insertdelete = ["','", str(folder) , "')"]

    elif links_to == "sample_uuid":
        end_filter = [" where ", links_to, " = '", str(sample), "'"]
        end_insertdelete = ["','", str(sample) , "')"]

    #-------------Text column---------------
    if values_type == "free_text":
        if str(value_list) != "None":
            update_command = ["UPDATE ", values_table, " SET ", values_column, " = '", value_list, "' "]
            update_command.extend(end_filter)
            update_command = "".join(update_command)
            commands.append(update_command)
    
    # TODO check if delete here works?
    elif values_type == "column_link":
        if value_list:
            if len(value_list)>0:
                update_command = ["UPDATE ", values_table, " SET ", values_column, " = '", value_list[0], "' "]
                update_command.extend(end_filter)
                update_command = "".join(update_command)
                commands.append(update_command)
            
    # TODO change in tab_layout to simple_table_link
    elif values_type == "simple_table_link":
        
        add_ids = set(value_list).difference(set(original_values))
        delete_ids = set(original_values).difference(set(value_list))
        
        for add_id in add_ids:

            insert_command = ["INSERT INTO ",  values_table ," (", values_uuid, ", ", links_to ,")",
                                " values ('", str(add_id) ]
            insert_command.extend(end_insertdelete)
            insert_command = "".join(insert_command)
            commands.append(insert_command)

        for delete_id in delete_ids:

            delete_command = ["DELETE FROM ",  values_table ,
                              " where ", values_uuid, " = '", str(delete_id), "' and "]
            
            delete_command.extend(end_filter[1:])
            delete_command = "".join(delete_command)
            commands.append(delete_command)
        
    elif values_type == "complex_table_link":

        delete_command = ["DELETE FROM ",  values_table , " where "] # Delete fully and reinsert
        delete_command.extend(end_filter[1:])
        delete_command = "".join(delete_command)
        commands.append(delete_command)

        for _, row in value_list.iterrows():       
            insert_command = 'INSERT INTO '+ values_table +' ('+ str(', '.join(value_list.columns))+ ') VALUES '+ str(tuple(row.values))        
            commands.append(insert_command)

    return commands

#---------------------------------------------------------RETRIEVE FROM DATABASE---------------------------------------------------------
def get_options(table_name, field_name, field_uuid, filter=""):
    sql_command = "SELECT distinct " + field_name + "," + field_uuid + " FROM " + table_name + filter
    sql_result = con.sql(sql_command).df().drop_duplicates()
    dict_result = {str(result[field_uuid]):result[field_name] for _,result in sql_result.iterrows()}
    dict_result = {v:k for k,v in sorted(dict_result.items(), key=lambda item: str(item[1]))}
    return dict_result

def get_values(options, values_type, values_table, values_column, values_uuid, links_to, sample = "", folder = ""):

    #-------------Text column---------------
    if values_type in ["free_text", "column_link"]:

        sql_command = ["SELECT distinct ", values_column, ", ", links_to ,
                        " FROM ", values_table]
        
        if links_to == "folder_uuid":
            sql_command.extend([" where ", links_to, " = '", str(folder), "' limit 1"])

        elif links_to == "sample_uuid":
            sql_command.extend([" where ", links_to, " = '", str(sample), "' limit 1"])

        sql_command = "".join(sql_command)
        sql_result = con.sql(sql_command).df()
        value = sql_result[values_column][0]

        if value == "None":
            value = ""

        if values_type == "column_link":
            value = [value]

        return value #String (either free text or a UUID)
    

    #-------------Table link (multichoice many)---------------
    elif values_type == "table_link":

        sql_command = ["SELECT distinct " , values_uuid ,
                        " FROM " , values_table]
        
        if links_to == "folder_uuid":
            sql_command.extend([" where ", links_to, " = '", str(folder), "' limit 1"])

        elif links_to == "sample_uuid":
            sql_command.extend([" where ", links_to, " = '", str(sample), "' limit 1"])

        sql_command = "".join(sql_command)
        sql_result = con.sql(sql_command).df()

        values = [str(s) for s in sql_result[values_uuid].unique() if str(s) in list(options.values())]

        return values #list of string UUIDs

    elif values_type == "complex_table_link":

        columns = ",".join(values_column)
        sql_command = ["SELECT distinct ", columns, 
                        " FROM ", table]
        
        if links_to == "folder_uuid":
            #sql_command.extend([" where ", links_to, " = '", str(folder), "' limit 1"]) # TODO why limit 1?
            sql_command.extend([" where ", links_to, " = '", str(folder)])

        elif links_to == "sample_uuid":
            #sql_command.extend([" where ", links_to, " = '", str(sample), "' limit 1"])
            sql_command.extend([" where ", links_to, " = '", str(sample)])

        sql_command = "".join(sql_command)
        sql_result = con.sql(sql_command).df()

        return sql_result

    #-------------Table link (multichoice many)---------------
    # elife values_type == "view_link"

#def get_table_values(table, columns, links_to, folder="", sample=""):

#    columns = ",".join(columns)
#    sql_command = ["SELECT distinct ", columns, 
#                    " FROM ", table]
    
#    if links_to == "folder_uuid":
#        #sql_command.extend([" where ", links_to, " = '", str(folder), "' limit 1"]) # TODO why limit 1?
#        sql_command.extend([" where ", links_to, " = '", str(folder)])

#    elif links_to == "sample_uuid":
#        #sql_command.extend([" where ", links_to, " = '", str(sample), "' limit 1"])
#        sql_command.extend([" where ", links_to, " = '", str(sample)])

#    sql_command = "".join(sql_command)
##    #print(sql_command)
#    sql_result = con.sql(sql_command).df()
#    return sql_result


