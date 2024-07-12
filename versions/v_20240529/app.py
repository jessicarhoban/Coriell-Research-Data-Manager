# %%
import hvplot.pandas
import numpy as np
import pandas as pd
import panel as pn

# %% Import DFs
folder_df = pd.read_csv("./tables/folder_df.csv", dtype=str)

cell_line_df = pd.read_csv("./tables/cell_line_df.csv", dtype=str)
cell_line_folder_df = pd.read_csv("./tables/cell_line_folder_df.csv", dtype=str)

tissue_df = pd.read_csv("./tables/tissue_df.csv", dtype=str)
tissue_folder_df = pd.read_csv("./tables/tissue_folder_df.csv", dtype=str)
cell_line_tissue_df = pd.read_csv("./tables/cell_line_tissue_df.csv", dtype=str)

organism_df = pd.read_csv("./tables/organism_df.csv", dtype=str)
organism_folder_df = pd.read_csv("./tables/organism_folder_df.csv", dtype=str)
cell_line_organism_df = pd.read_csv("./tables/cell_line_organism_df.csv", dtype=str)


# %% Create extension

pn.extension(design="material", sizing_mode="stretch_width")
pn.extension('tabulator')

# %% Test Autocomplete
#-----------Test out autocomplete-----------
#autocomplete = pn.widgets.AutocompleteInput(
#    name='Autocomplete Input', options=list(cell_line_df[cell_line_df["cell_line_name"].notnull()]["cell_line_name"].unique()),
#    case_sensitive=False, search_strategy='includes',
#    placeholder='Cell Line')


# %% Test MultiChoice
#-----------Test out MultiChoice-----------
cell_line_mc = pn.widgets.MultiChoice(name='MultiSelect',
                                      options=list(cell_line_df[cell_line_df["cell_line_name"].notnull()]["cell_line_name"].unique()))

#-----------Test out MultiChoice-----------
tissue_mc = pn.widgets.MultiChoice(name='MultiSelect',
                                      options=list(tissue_df[tissue_df["tissue_name"].notnull()]["tissue_name"].unique()))

#-----------Test out MultiChoice-----------
organism_mc = pn.widgets.MultiChoice(name='MultiSelect',
                                      options=list(organism_df[organism_df["organism_name"].notnull()]["organism_name"].unique()))


#mc_dicts = [{"field":"organism_name", "mc":organism_mc},
#            {"field":"tissue_name", "mc":tissue_mc},
#            {"field":"cell_line_name", "mc":cell_line_mc}]

# %% Test filtering
#-----------Test out df-----------
def get_df(organism_mc, tissue_mc, cell_line_mc):#age, name # = list(cell_line_df["age"].unique())):
    mergedf = folder_df.merge(cell_line_folder_df, on="folder_uuid", how="left")
    mergedf = mergedf.merge(tissue_folder_df, on="folder_uuid", how="left")
    mergedf = mergedf.merge(organism_folder_df, on="folder_uuid", how="left")
    mergedf = mergedf.merge(cell_line_df, on="cell_line_uuid", how="left")
    #mergedf = mergedf.merge(cell_line_tissue_df, on="cell_line_uuid", how="left")
    #mergedf = mergedf.merge(cell_line_organism_df, on="cell_line_uuid", how="left")
    mergedf = mergedf.merge(tissue_df, on="tissue_uuid", how="left")
    mergedf = mergedf.merge(organism_df, on="organism_uuid", how="left")

    #mask1 = mergedf["age"].isin(age)
    #mask2 = mergedf["cell_line_name"]==name
    
    #masks= []
    #masks.append()
    #masks.append()
    #masks.append()
    #masks2 = [any(tup) for tup in zip(masks)]

    #for mc_dict in [organism_mc, tissue_mc, cell_line_mc]:
    #    mask = mergedf[mc_dict["field"]].isin(mc_dict["mc"])
    #    masks.append(mask)

    df_pane = pn.widgets.Tabulator(mergedf.loc[mergedf["organism_name"].isin(organism_mc) | 
                                               mergedf["tissue_name"].isin(tissue_mc) |
                                               mergedf["cell_line_name"].isin(cell_line_mc)],
                                               selectable=True, disabled=True)
    
    #df_pane = pn.widgets.Tabulator(mergedf, selectable=True, disabled=True)

    return  df_pane

bound_df = pn.bind(get_df, organism_mc=organism_mc, tissue_mc = tissue_mc, cell_line_mc = cell_line_mc)#age = multi_choice, name = autocomplete)

# %%
fields_choice = pn.widgets.MultiChoice(name='MultiSelect',
                                      options=["Cell Line","Tissue","Organism"])

#def on_click_event(event):
    

# event.column, event.row, event.value
def get_filtered_df(mc, df, field_name):
    filtered_df_pane = pn.widgets.Tabulator(df.loc[df[field_name].isin(mc)], selectable=True, disabled=True)
    return filtered_df_pane

def get_field_tabs(fields, event_row):
    field_tabs = pn.Tabs()
    for field in fields:
        if field == "Cell Line":
            which_cell_line = selected_rows["cell_line_name"].unique()
            cell_df = pn.bind(get_filtered_df, mc = list(which_cell_line), df = cell_line_df, field_name="cell_line_name")
        
            field_tabs.append(cell_df)

        else:
            field_tabs.append(field)

    return field_tabs

bound_tabs = pn.bind(get_field_tabs, fields = fields_choice, event_row = event.row)



template = pn.template.BootstrapTemplate(
    site = "Site",
    title = "Title",

    sidebar = [fields_choice, cell_line_mc, tissue_mc, organism_mc])#autocomplete,multi_choice])

template.main.append(
    pn.Row(
        bound_df,
        bound_tabs  #pn.Tabs('Scatter',"Try2")#pn.Spacer(styles=dict(background='red'))
    )
)
#gspec = pn.GridSpec(sizing_mode='stretch_both', max_height=800)
#gspec[:, :2] = bound_df
#gspec[:, 2] = pn.Spacer(styles=dict(background='red'))

#template.main.append(gspec)
#toggle_group = pn.widgets.ToggleGroup(name='ToggleGroup', options=['Biology', 'Chemistry', 'Physics'], behavior="radio")

template.servable()

# On click
# Use tabulator filter instead?
#groupby
# row contents / expanded
# client side filtering



# %%
