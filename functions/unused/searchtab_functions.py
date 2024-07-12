# %% INITIAL SETUP

# Import required libraries
import time

# Import custom functions
import functions.sql_functions as sqlfunc
import functions.api_functions as apifunc




#---------------------------------------------------------GENE TAB---------------------------------------------------------
# Definitions
def get_clean_genes(options1, is_value):
    
    if is_value == 0:
        clean_options = {}

    elif is_value == 1:
        clean_options = []

    for n in options1:
        name = "("+n["name"] + ") " + n["description"].title() + " [" + n["organism"] +"]" 
        uuid = str(n["uid"])
        
        if is_value == 0:
            clean_options[name] = uuid

        else:
            if n["is_value"] == 1:
                clean_options.append(uuid)

    return clean_options

#---------------------------------------------------------------
def get_genes(gene_text, gene_checkbox, folder_mc1):
   
    # Filter for organism checkbox
    organisms = []
    for val in gene_checkbox:
        if val == "Human":
            organisms.append("Homo sapiens")
        elif val == "Mouse":
            organisms.append("Mus musculus")

    if len(gene_checkbox) == 0:
        organisms = ["Homo sapiens", "Mus musculus"]
        #gene_checkbox.value.append("Human")
    
    #--------------------------------
    # Append ones already there 
    #values = sqlfunc.get_values("v_folder_gene", "gene", "gene_id_ncbi", "gene_uuid", folder_mc1, value_uuid="gene_id_ncbi")
    values = []
    
    all_genes = []
    filter_genes = []

    if len(values)>0:
        idlist =  ",".join(values)
        current_genes = apifunc.get_ncbi_gene_info(idlist, organisms=["Homo sapiens", "Mus musculus"], is_value=1)
        filter_genes.extend([c["uid"] for c in current_genes])
        all_genes.extend(current_genes)
        #time.sleep(0.5) 
    
    # If they searched for something
    if len(gene_text)>0:
        distinct_searches = gene_text.split(",")
        for search in distinct_searches:
            idlist = apifunc.get_ncbi_gene_ids(search)
            if len(idlist)>0:
                new_genes = apifunc.get_ncbi_gene_info(idlist, organisms, filter_uids = filter_genes)
                all_genes.extend(new_genes)
                time.sleep(0.5) 
        #all_genes = sorted(all_genes, key=lambda x: x['index'])
    
    else:
        options = list(sqlfunc.get_options("gene", "gene_id_ncbi", "gene_uuid").keys())
        if len(options)>0:
            options = [o for o in options if o not in values]
            idlist =  ",".join(options)
            current_options = apifunc.get_ncbi_gene_info(idlist, organisms=["Homo sapiens", "Mus musculus"], ret_max=str(len(options)))
            all_genes.extend(current_options)
            time.sleep(0.5) 
        
    #all_genes = sorted(all_genes, key=lambda x: x['name'])

    return all_genes



#---------------------------------------------------------DISEASE TAB---------------------------------------------------------


#---------------------------------------------------------CELL LINE TAB---------------------------------------------------------


#---------------------------------------------------------TREATMENT TAB---------------------------------------------------------
