# %% INITIAL SETUP

# Import required libraries
import requests
import panel as pn

# Import and establish database connection
import duckdb
con = duckdb.connect("./browser_cleaner.db")

#---------------------------------------------------------UPDATE DATABASE---------------------------------------------------------
# TO CLEAN
def execute_update_commands(event, commands):
    if not event:
        return
    else:
        pn.pane.Alert('## Save successful').servable()
    #for command in commands:
        #print(command)
        #con.execute(command)

#---------------------------------------------------------------
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
    
    elif values_type == "column_link":
        if value_list:
            if len(value_list)>0:
                update_command = ["UPDATE ", values_table, " SET ", values_column, " = '", value_list[0], "' "]
                update_command.extend(end_filter)
                update_command = "".join(update_command)
                commands.append(update_command)
            
    elif values_type == "table_link":
        
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
        
    return commands

#---------------------------------------------------------RETRIEVE FROM DATABASE---------------------------------------------------------
def get_options(table_name, field_name, field_uuid, filter=""):
    sql_command = "SELECT distinct " + field_name + "," + field_uuid + " FROM " + table_name + filter
    sql_result = con.sql(sql_command).df().drop_duplicates()
    dict_result = {str(result[field_uuid]):result[field_name] for _,result in sql_result.iterrows()}
    dict_result = {v:k for k,v in sorted(dict_result.items(), key=lambda item: str(item[1]))}
    return dict_result

#---------------------------------------------------------------
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


    #-------------Table link (multichoice many)---------------
    # elife values_type == "view_link"

def get_table_values(table, columns, links_to, folder="", sample=""):

    columns = ",".join(columns)
    sql_command = ["SELECT distinct ", columns, 
                    " FROM ", table]
    
    if links_to == "folder_uuid":
        sql_command.extend([" where ", links_to, " = '", str(folder), "' limit 1"])

    elif links_to == "sample_uuid":
        sql_command.extend([" where ", links_to, " = '", str(sample), "' limit 1"])

    sql_command = "".join(sql_command)
    #print(sql_command)
    sql_result = con.sql(sql_command).df()
    return sql_result













#def get_text_update_commands(text_value, field_name, folder_mc1, by_sample=False):
#    
#    for val in folder_mc1:
#        if by_sample == False:
#            sql_command = "UPDATE folder SET " + \
#                            field_name + " = '" + str(text_value) + \
#                            "' where folder_uuid = '" + str(val) + "'"
#            return [sql_command]


#def get_user_folders(user_uuid):
#    folder_commands = ["SELECT f.foldername, f.folder_uuid",
#                        " FROM user_researcher ur",
#                        " join researcher_folder rf on ur.researcher_uuid = rf.researcher_uuid  ",
#                        " join folder f on f.folder_uuid = rf.folder_uuid ",
#                        " WHERE ur.user_uuid = '", user_uuid,
#                        "' and f.is_root = 1"]
#    folder_command = "".join(folder_commands)
#    folder_options = con.sql(folder_command).df().drop_duplicates()
#    folder_options = {str(result[f"foldername"]):result["folder_uuid"] for _,result in folder_options.iterrows()}
#    return folder_options


# OLD
#def get_values_old(table_view, table_name, field_name, field_uuid, 
#                     folder_mc1, 
#                     value_uuid="field_uuid"):
#    sql_command = "SELECT distinct " + field_name + "," + field_uuid + " FROM " + table_name
#    sql_result = con.sql(sql_command).df()#

#    for val in folder_mc1:
#        sql_command2 = "SELECT distinct " + field_uuid  + ",folder_uuid " + " FROM " + table_view 
#        sql_result2 = con.sql(sql_command2).df().drop_duplicates()
#        sql_result2 = sql_result2.loc[sql_result2["folder_uuid"] == str(val)]
#        filtered_result = sql_result.merge(sql_result2, on=field_uuid)
#        if value_uuid == "field_uuid":
#            result = [result[field_uuid] for _,result in filtered_result.iterrows()]
#        else:
#            result = [str(result[value_uuid]) for _,result in filtered_result.iterrows()]
#        return result


#def get_text_values_old(field_name, folder_mc1, by_sample=False):
#    
#    for val in folder_mc1:
#        if by_sample == False:
#            sql_command = ["SELECT distinct ", field_name, 
#                           ", folder_uuid FROM folder where folder_uuid = '",
#                           str(val), "' limit 1"]
#            sql_command = "".join(sql_command)
#            sql_result = con.sql(sql_command).df()
#            value = sql_result[field_name][0]
#            if value == "None":
#                value = ""
#            return value
