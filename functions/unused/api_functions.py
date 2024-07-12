# %% INITIAL SETUP

# Import required libraries
import requests


#---------------------------------------------------------GENE TAB (NCBI)---------------------------------------------------------
def get_ncbi_gene_ids(search):
    search_text =  '"' + search + '"[Gene/Protein Name]'
    search_params = {"db":"gene", "retmax":"30", "retmode":"json", "term":search_text}
    url_base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?&"
    for param,value in search_params.items():
        url_base += "&"
        url_base += param
        url_base += "="
        url_base += value
    
    response = requests.get(url_base)
    if "esearchresult" in response.json().keys():
        idfiltered = [i for i in response.json()["esearchresult"]["idlist"]]
        idlist = ",".join(idfiltered)
        return idlist
    
    else:
        return ""
        

#---------------------------------------------------------------
def get_ncbi_gene_info(idlist, organisms, ret_max='10', is_value=0, filter_uids=[]):
    
    # Initialize empty values
    all_genes = []

    url3 = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=gene&retmode=json&retmax=' + ret_max + '&id=' + idlist
    response3  = requests.get(url3)

    if "result" in response3.json().keys():
        uids1 = [i for i in response3.json()["result"]["uids"]]

        if len(filter_uids)>0:
            uids1 = [u for u in uids1 if u not in filter_uids]

        genes = [{"name":response3.json()["result"][j]["name"] ,
                    "description":response3.json()["result"][j]["description"],
                    "organism":response3.json()["result"][j]["organism"]["scientificname"],
                    "uid":response3.json()["result"][j]["uid"],
                    "is_value":is_value,
                    "index":i}
                    for i,j in enumerate(uids1)
                    if response3.json()["result"][j]["organism"]["scientificname"] in organisms]
            
        for gene in genes:
            all_genes.append(gene) 
    
    return all_genes



#---------------------------------------------------------DISEASE TAB (NCIT)---------------------------------------------------------
