




#---------------------------------------------------------CREATE TABS---------------------------------------------------------
def get_nonsearchable_sample_tab(tab_layout, tab_name, header, folder_mc1):
    
    #tab = pn.Column(header)

    # Get all samples linked to this folder
    for val in folder_mc1:

        tabulator_formatters = {}
        df = sqlfunc.get_folder_samples(str(val))
        tabulator_titles = {"sample_name": "Sample Name"}
        tabulator_editors = {"sample_name":None}  
        all_options = {}
        mc_row = pn.Row(pn.Spacer(width=170))

        for _, row in tab_layout.iterrows():

            
            col_name = row["column_name"]
            col_title = row["col_title"]
                #for col_name,col_title in columns.items():
            
            options = sqlfunc.get_options(row["table_name"], col_name, row["table_uuid"])
            
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


















from bokeh.models.widgets.tables import CheckboxEditor, NumberEditor, SelectEditor

bokeh_editors = {
    'str': SelectEditor(options=['A', 'B', 'C', 'D']),
}

df_newtab =  pn.widgets.Tabulator(pd.DataFrame([{"str":"A"}]), editors=bokeh_editors)

df_newtab.on_edit(lambda e: print(e.column, e.row, e.old, e.value))

template.main.append(df_newtab)




tabulator_editors = {
    'int': None,
    'float': {'type': 'number', 'max': 10, 'step': 0.1},
    'bool': {'type': 'tickCross', 'tristate': True, 'indeterminateValue': None},
    'str': {'type': 'list', 'valuesLookup': True, 'multiselect':True},
	'str2': {'type': 'list', 'valuesLookup': True, 'autocomplete':True},
    'date': 'date',
    'datetime': 'datetime',
}

df_newtab2 =  pn.widgets.Tabulator(pd.DataFrame([{"str":"App", "str2":"App"},{"str":"Ann","str2":"Ann"},{"str":"Baa","str2":"Baa"}]), editors=tabulator_editors)

df_newtab2.on_edit(lambda e: print(e.column, e.row, e.old, e.value))

template.main.append(df_newtab2)



#------------------------------------------------------------------------------------------------------------------------------




#folder_df = sample_folder[["foldername", "folder_uuid", "sample_name"]].drop_duplicates()

#sample_folder = sample_folder[[col for col in sample_folder if "uuid" not in col]]
sample_folder["sample_uuid"] = sample_folder["sample_uuid"].astype(str)
sample_folder["file_uuid"] = sample_folder["file_uuid"].astype(str)

def content_function(row):
    sample_df = sample_folder[sample_folder.apply(lambda x: str(x["sample_uuid"]) == str(row["sample_uuid"]), axis=1)]
    sample_df = sample_df[["filename", "file_uuid"]].drop_duplicates()
    #sample_df["sample_uuid"] = sample_df["sample_uuid"].astype(str)
    sample_table = pn.widgets.Tabulator(sample_df,selectable=1, disabled=True, show_index=False, hidden_columns = ["file_uuid"])
    return pn.Row(pn.Spacer(width=30),sample_table)

#sample_folder.set_index(["foldername", "sample_name","filename"])#[["filename"]]

#table = pn.widgets.Tabulator(sample_folder, pagination = 'local', row_content = content_function, #groupby=["sample_name"], #hierarchical=True, #row_content = content_function,
#							    selectable=1, disabled=True, show_index=False, hidden_columns=["sample_uuid", "file_uuid","filename"])

#template.main.append(table)










#------------------------------------------------------------------------------------------------------------------------------

tables = con.sql("select s.sample_name, sf.folder_uuid from sample s join sample_folder sf on s.sample_uuid = sf.sample_uuid").df()#.tail(5)


test_table = pn.widgets.Tabulator(tables,  groupby = ['folder_uuid'], configuration={'movableRows':True, 
                                                          'groupBy':'folder_uuid',
                                                          'groupValues':[t for t in tables["folder_uuid"].unique()]})#, 
                                                        # 'groupBy':'folder_uuid',
                                                        # 'groupValues':[['13228699-4dec-4e9f-a905-3e05f565b1d9',
                                                        #                 '74b2dec9-8bad-43d8-a0b7-4b31038fc27d',
                                                        #                 '952242d6-7feb-4650-8ef8-d95c0641266f']]})

#test_table1 = pn.widgets.Tabulator(pd.DataFrame(columns=["user_name"]))

template.main.append(test_table)
#template.main.append(test_table1)







#------------------------------------------------------------------------------------------------------------------------------


tables = con.sql("select s.sample_name, sf.folder_uuid from sample s join sample_folder sf on s.sample_uuid = sf.sample_uuid").df().head(20)
tables["groupcol"] = "green"

test_table1 = pn.widgets.Tabulator(tables[10:],  configuration={'movableRows':True, 'movableRowsReceiver': "add"})
test_table = pn.widgets.Tabulator(tables[:10],  configuration={'movableRows':True, 'movableRowsConnectedTables': 'test_table1', 'movableRowsSender': "delete"}) 

def click(event):
    print(event)

test_table1 = pn.widgets.Tabulator(tables[10:],  configuration={'movableRows':True,  'movableRowsConnectedElements': "#drop-area"})
test_table1.on("movableRowsElementDrop", print("THIS"))

                                                          #'groupBy':'groupcol',
                                                          #'groupValues':[['green','blue','red']]})
                                                          #'columns':[{'title':'groupcol', 'field':'groupcol'},
                                                          #           {'title':'sample_name', 'field':'sample_name'}]})
                                                          
                                                          
                                                          #, 
                                                        # 'groupBy':'folder_uuid',
                                                        # 'groupValues':[['13228699-4dec-4e9f-a905-3e05f565b1d9',
                                                        #                 '74b2dec9-8bad-43d8-a0b7-4b31038fc27d',
                                                        #                 '952242d6-7feb-4650-8ef8-d95c0641266f']]})



template.main.append(test_table)
template.main.append(test_table1)








#------------------------------------------------------------------------------------------------------------------------------










sample_folder = con.sql("".join(["select f.foldername, f.folder_uuid, s.sample_numfiles, s.sample_uuid, fi.file_uuid, s.sample_name, fi.filename from sample s join sample_folder sf on sf.sample_uuid = s.sample_uuid ",
                                "join folder f on f.folder_uuid = sf.folder_uuid ",
                                "join file_sample_folder fsf on fsf.sample_uuid = s.sample_uuid ",
                                "join file fi on fi.file_uuid = fsf.file_uuid ", 
                                ""])).df()

folder_df = sample_folder[["foldername", "folder_uuid"]].drop_duplicates()

#content_fn = lambda row: pn.widgets.Tabulator(sample_folder[sample_folder.apply(lambda x: str(x["folder_uuid"]) == str(row["folder_uuid"]), axis=1)]# #str(sample_folder["folder_uuid"])==str(row["folder_uuid"])]
#                                              [["foldername", "folder_uuid", "sample_name", "sample_uuid"]].drop_duplicates(), 
#                                              selectable=1, disabled=True, show_index=False, hidden_columns = ["foldername", "folder_uuid", "sample_uuid"])


def content_function(row):
    sample_df = sample_folder[sample_folder.apply(lambda x: str(x["folder_uuid"]) == str(row["folder_uuid"]), axis=1)]
    sample_df = sample_df[["sample_name", "sample_uuid"]].drop_duplicates()
    sample_df["sample_uuid"] = sample_df["sample_uuid"].astype(str)
    sample_table = pn.widgets.Tabulator(sample_df,selectable=1, disabled=True, show_index=False, hidden_columns = ["sample_uuid"])
    return pn.Row(pn.Spacer(width=30),sample_table)

#content_fn = lambda row: 

#sample_folder = sample_folder.set_index(["foldername", "sample_name","filename"])#[["filename"]]
table = pn.widgets.Tabulator(folder_df, pagination = 'local', row_content = content_function, #groupby=["foldername"], #hierarchical=True, #
							    selectable=1, disabled=True, show_index=False, hidden_columns = ["folder_uuid"])


template.main.append(table)