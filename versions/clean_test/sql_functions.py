# %% INITIAL SETUP

# Import required libraries
import requests

# Import and establish database connection
import duckdb
con = duckdb.connect("./database/browser_test.db")

#---------------------------------------------------------UPDATE DATABASE---------------------------------------------------------

def execute_update_commands(event, commands):
    if not event:
        return
    for command in commands:
        #print(command)
        con.execute(command)


#---------------------------------------------------------------
def get_update_commands(value_list, table_view_editable, table_view, table_uuid, folder_mc1, 
                        is_searchable=False, searchable_field="", table_name = ""):
    
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
            options = get_options(table_name, searchable_field, table_uuid)
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
            options = get_options(table_name, searchable_field, table_uuid)
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
            delete_command = ["DELETE FROM ",  
                              table_view_editable , 
                              " WHERE ", 
                              table_uuid, 
                              " = '", 
                              delete_id,
                              "' AND folder_uuid = '", 
                              val, 
                              "'"]
            delete_command = "".join(delete_command)
            commands.append(delete_command)
            if is_searchable == True:
                con.execute(delete_command)

    return commands


#---------------------------------------------------------RETRIEVE FROM DATABASE---------------------------------------------------------

def get_options(table_name, field_name, field_uuid, filter=""):
    sql_command = "SELECT distinct " + field_name + "," + field_uuid + " FROM " + table_name + filter
    sql_result = con.sql(sql_command).df().drop_duplicates()
    #dict_result = {str(result[field_name]):result[field_uuid] for _,result in sql_result.iterrows()}
    dict_result = {str(result[field_uuid]):result[field_name] for _,result in sql_result.iterrows()}
    return dict_result


#---------------------------------------------------------------
def get_values(table_view, table_name, field_name, field_uuid, 
                     folder_mc1, 
                     value_uuid="field_uuid"):
    sql_command = "SELECT distinct " + field_name + "," + field_uuid + " FROM " + table_name
    sql_result = con.sql(sql_command).df()

    for val in folder_mc1:
        sql_command2 = "SELECT distinct " + field_uuid  + ",folder_uuid " + " FROM " + table_view 
        sql_result2 = con.sql(sql_command2).df().drop_duplicates()
        sql_result2 = sql_result2.loc[sql_result2["folder_uuid"] == str(val)]
        filtered_result = sql_result.merge(sql_result2, on=field_uuid)
        if value_uuid == "field_uuid":
            result = [result[field_uuid] for _,result in filtered_result.iterrows()]
        else:
            result = [str(result[value_uuid]) for _,result in filtered_result.iterrows()]
        return result

#---------------------------------------------------------------
def get_text_values(field_name, folder_mc1, by_sample=False):
    
    for val in folder_mc1:
        if by_sample == False:
            sql_command = "SELECT distinct " + field_name +  ", folder_uuid FROM folder"
            sql_result = con.sql(sql_command).df()
            sql_result = sql_result.loc[sql_result["folder_uuid"] == str(val)].reset_index()
            value = str(sql_result[field_name].unique()[0])
            if value == "None":
                value = ""
            return value

#---------------------------------------------------------------
def get_text_update_commands(text_value, field_name, folder_mc1, by_sample=False):
    
    for val in folder_mc1:
        if by_sample == False:
            sql_command = "UPDATE folder SET " + \
                            field_name + " = '" + str(text_value) + \
                            "' where folder_uuid = '" + str(val) + "'"
            return [sql_command]

#---------------------------------------------------------------
def get_user_folders(user_uuid):
    folder_commands = ["SELECT f.foldername, f.folder_uuid",
                        " FROM user_researcher ur",
                        " join researcher_folder rf on ur.researcher_uuid = rf.researcher_uuid  ",
                        " join folder f on f.folder_uuid = rf.folder_uuid ",
                        " WHERE ur.user_uuid = '", user_uuid,
                        "' and f.is_root = 1"]
    folder_command = "".join(folder_commands)
    folder_options = con.sql(folder_command).df().drop_duplicates()
    folder_options = {str(result[f"foldername"]):result["folder_uuid"] for _,result in folder_options.iterrows()}
    return folder_options


#---------------------------------------------------------------
def get_folder_samples(folder_val):
    sample_commands = ["select distinct s.sample_uuid, s.sample_name from sample s join sample_folder sf on sf.sample_uuid = s.sample_uuid ",
                            "where sf.folder_uuid = '", str(folder_val) ,"'"]
    sample_command = "".join(sample_commands)
    sample_options = con.sql(sample_command).df().drop_duplicates()
    #sample_options = {str(result[f"sample_name"]):result["sample_uuid"] for _,result in sample_options.iterrows()}
    return sample_options
