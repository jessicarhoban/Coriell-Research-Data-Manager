

#----------------------------------------------------------------------------------------------------------------------------------------
# TO DO

foldertemp = folder_df[[col for col in folder_df.columns if col not in cell_df.columns]].merge(cell_line_cell_df, on = 'cell_line_uuid', how = 'left').merge(cell_df, on="cell_uuid")
folder3 = pd.concat([folder_df,foldertemp], ignore_index=True)

foldertemp = folder_df[[col for col in folder_df.columns if col not in disease_df.columns]].merge(cell_line_disease_df, on = 'cell_line_uuid', how = 'left').merge(disease_df, on="disease_uuid")
folder3 = pd.concat([folder3,foldertemp], ignore_index=True)

foldertemp = folder_df[[col for col in folder_df.columns if col not in gene_df.columns]].merge(cell_line_gene_df, on = 'cell_line_uuid', how = 'left').merge(gene_df, on="gene_uuid")
folder3 = pd.concat([folder3,foldertemp], ignore_index=True)

foldertemp = folder_df[[col for col in folder_df.columns if col not in organism_df.columns]].merge(cell_line_organism_df, on = 'cell_line_uuid', how = 'left').merge(organism_df, on="organism_uuid")
folder3 = pd.concat([folder3,foldertemp], ignore_index=True)

foldertemp = folder_df[[col for col in folder_df.columns if col not in tissue_df.columns]].merge(cell_line_tissue_df, on = 'cell_line_uuid', how = 'left').merge(tissue_df, on="tissue_uuid")
folder3 = pd.concat([folder3,foldertemp], ignore_index=True)

#foldertemp = folder_df[[col for col in folder_df.columns if col not in gene_df.columns]].merge(disease_gene_df, on = 'disease_uuid', how = 'left').merge(gene_df, on="gene_uuid")
#folder3 = pd.concat([folder3,foldertemp], ignore_index=True)

foldertemp = folder_df[[col for col in folder_df.columns if col not in organism_df.columns]].merge(gene_organism_df, on = 'gene_uuid', how = 'left').merge(organism_df, on="organism_uuid")
folder3 = pd.concat([folder3,foldertemp], ignore_index=True)

foldertemp = folder_df[[col for col in folder_df.columns if col not in institute_df.columns]].merge(researcher_institute_df, on = 'researcher_uuid', how = 'left').merge(institute_df, on="institute_uuid")
folder3 = pd.concat([folder3,foldertemp], ignore_index=True)

foldertemp = folder_df[[col for col in folder_df.columns if col not in lab_df.columns]].merge(researcher_lab_df, on = 'researcher_uuid', how = 'left').merge(lab_df, on="lab_uuid")
folder3 = pd.concat([folder3,foldertemp], ignore_index=True)


foldertemp1 = folder_df[[col for col in folder_df.columns if col not in gene_df.columns]].merge(cell_line_gene_df, on = 'cell_line_uuid', how = 'left').merge(gene_df, on="gene_uuid")
#foldertemp2 = folder_df[[col for col in folder_df.columns if col not in gene_df.columns]].merge(disease_gene_df, on = 'disease_uuid', how = 'left').merge(gene_df, on="gene_uuid")
#f3 = foldertemp1[["gene_uuid", "folder_uuid"]].merge(foldertemp2[["gene_uuid", "folder_uuid"]], on = ["gene_uuid","folder_uuid"], how="outer", indicator = True)
  """

Purpose: 
  
  - Inside a "maps" folder should be various CSV files, one per eventual SQL table:
       Ex: cell_line_map.csv --> cell_line SQL table
           disease_map.csv   --> disease SQL table
           
  - Each of these CSV files contain a column called "search_term", which is a comma-delimited list of 
    phrases found within the "mnt/data/research_data/" folders. These "search_terms" map to a specific
    classificiation (either cell lines, experiment types, etc)
       Ex:  search_term = "crc,colon_cancer" <-->  disease_name = "Colorectal Cancer"
            search_term = "atac"             <-->  experiment_name = "ATAC-Seq"
            
  - This code section:
        1) Reads in all map CSV files as DataFrames, representing future SQL tables (held in map_dfs)
        2) Iterates over all folders in mnt/data/research_data/ (held in folder_df)
        3) Creates new "linker" DataFrames, connecting map DFs to folder DFs
              Ex:  map_df      <-->  folder_df  ==  l_map_folder
                   disease_df  <-->  folder_df  ==  l_disease_folder
        
  - This allows us to search, say, for folders with ATAC-Seq data, or folders containing experiments
    on the cell line MIA PaCa-2. By having a separate "linker" table, this also allows for many-many relationships,
    rather than the 1-1 relationships created by simply linking two column names on their own.

"""

#----------------------------------------------------------------------------------------------------------------------------------------

#for _, folder in folders_df.iterrows():
  
  
  
 
  # Add in link_generated_by type (API, manual and by who/what time)
    # If link already generated, ignore. If not, look for data and populate
  
  
  


  # Links to cell
  #bto = re.findall("DR\s*BTO;\s(.*?)\n",response.text)
  #clo = re.findall("DR\s*CLO;\s(.*?)\n",response.text)
  #for tissue in tissue_id_uberon:
  #  row = pd.DataFrame({"tissue_id_uberon":tissue_id_uberon, 
  #                      "cell_line_uuid":cell_line["cell_line_uuid"]})
  #  cell_line_organism_links.append(row)
  
  
  


      #if len(so_term)>0:
      #  url = "http://www.sequenceontology.org/browser/current_svn/term/" + so_term[0]
      #  response  = requests.get(url)
      #  so_name = re.findall('<th colspan="2"><h2>(.*?)&nbsp;&nbsp;', str(response.text))
      #  row["functional_consequence_name"] =  so_name[0] if len(so_name)>0 else ""


              
#__________________________________________________________________________________________________
      #________Fill in Empty________
      #fill_dicts = [{"primary_col":"var_type",
      #              "var_type":"Gene deletion",
      #              "functional_consequence_name":"loss_of_function_variant",
      #              "functional_consequence_so":"SO:0002054"},
                    
      #              {"primary_col":"molecular_consequence",
      #              "molecular_consequence":"nonsense",
      #              "functional_consequence_name":"loss_of_function_variant",
      #              "functional_consequence_so":"SO:0002054"},
                    
      #              {"primary_col":"molecular_consequence",
      #              "molecular_consequence":"synonymous variant",
      #              "functional_consequence_name":"functionally_normal",
      #              "functional_consequence_so":"SO:0002219"}]
                    
      #for fill_dict in fill_dicts:
      #  if fill_dict["primary_col"] in row.keys():
      #    print(row)
      #    print(fill_dict)
      #    print(row[fill_dict["primary_col"]])
      #    print(fill_dict[fill_dict["primary_col"]])
      #    print()
      #    if row[fill_dict["primary_col"]] == fill_dict[fill_dict["primary_col"]]:
      #      for key in [k for k in fill_dict.keys() if k not in [fill_dict["primary_col"],"primary_col"]]:
      #        if key not in row:
      #          row[key] = fill_dict[key]
        
        
        
          
#----------------------------------------------------------------------------------------------------------------------------------------
#_________________________Disease <-> Gene Links_________________________
# Initialize disease links
disease_gene_links = []
newgene_map = []

for _, disease in disease_df[disease_df["disease_id_ncit"].notnull()].iterrows():

  url = "https://api-evsrest.nci.nih.gov/api/v1/concept/ncit/"+disease["disease_id_ncit"]+"?include=full"
  response  = requests.get(url)
  result = json.loads(response.text)
  
  if "inverseRoles" in result:
    assoc_genes = [{k: t[k] for k in ["type","relatedCode"]} for t in result["inverseRoles"] if t["type"] == "Gene_Associated_With_Disease"]
  
    for assoc_gene in assoc_genes:
      url = "https://api-evsrest.nci.nih.gov/api/v1/concept/ncit/"+assoc_gene["relatedCode"]+"?include=full"
      response  = requests.get(url)
      result = json.loads(response.text)
      hgnc_id = [t for t in result["properties"] if t["type"]=="HGNC_ID"]
      hgnc_id = hgnc_id[0]["value"].replace("HGNC:","") if len(hgnc_id)>0 else ""
    
      # Check if already in gene_df
      if len(gene_df[gene_df["gene_id_hgnc"]==hgnc_id]) > 0:
        gene_uuid = gene_df[gene_df["gene_id_hgnc"]==hgnc_id]["gene_uuid"].unique()[0]
        
      else:
        gene_uuid = str(uuid.uuid4())
        gene = {"gene_id_hgnc":hgnc_id, "gene_symbol": result["name"].replace(" Gene",""), "gene_uuid":gene_uuid}
        if not any(d["gene_id_hgnc"] == hgnc_id for d in newgene_map):
          newgene_map.append(gene)
          
      row = {"gene_uuid":gene_uuid,
             "disease_uuid":disease["disease_uuid"]}
                
      disease_gene_links.append(row)

disease_gene_df = pd.DataFrame(disease_gene_links)
gene_df = pd.concat([gene_df,pd.DataFrame(newgene_map)], ignore_index=True)

