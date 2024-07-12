
from intermine.webservice import Service
service = Service("https://www.mousemine.org/mousemine/service")
query=service.new_query("Strain")
query.select("*")
query.add_constraint("primaryIdentifier","=","MGI:3050593")
for row in query.rows():
  print(row["primaryIdentifier"])
  print(row["name"])
  print(row["attributeString"])
  print(row["description"])
  print(row["symbol"])

query=service.new_query("Organism") 
query.select('*')
query.add_constraint("genus","=","Mus")
for row in query.rows():
    print(row)


service = Service("https://www.intermine.org/so/service")
query=service.new_query()
query.select("*")
for row in query.rows():
  print(row)

url = "http://www.sequenceontology.org/browser/current_svn/term/" + "SO:0002053"
response  = requests.get(url)
name = re.findall('<th colspan="2"><h2>(.*?)&nbsp;&nbsp;', str(response.text))


      if "var_type" == "Gene deletion":
        
        if "variation_type" not in row.keys():
          row["variation_type"] = "Deletion"
          
        if "molecular_consequence" not in row.keys():
          


  query.add_constraint("primaryIdentifier","=",cell_line["mgi_id"])
  for row in query.rows():
    cell_line_df.loc[index]["cell_line_name"] = row["name"] if row["name"]!="None" else ""
    cell_line_df.loc[index]["category"] = row["attributeString"] if row["attributeString"]!="None" else ""
    cell_line_df.loc[index]["cell_line_symbol"] = row["symbol"] if row["symbol"]!="None" else ""
    cell_line_df.loc[index]["description"] = row["description"] if row["description"]!="None" else ""
  enablePrint()

url = "https://www.ncbi.nlm.nih.gov/clinvar/variation/13961"
response  = requests.get(url)
soterm = re.findall("(?:Sequence Ontology[\S\s]*?)>(SO:\d{7})<", str(response.text))

if soterm
  
  
  
soterm = re.findall("(?:Sequence Ontology[\S\s]*?)>(SO:\d{7})<", str(response.text))


import time

url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=clinvar&id=" + "13961"  + "&retmode=json"
response  = requests.get(url)
print(response.json()["result"]["13961"]["variation_set"][0]["variant_type"])

print(response.json()["result"]["13961"]["molecular_consequence_list"])


print(response.json()["result"]["13961"]["clinical_impact_classification"]["description"])
time.sleep(1)
print(response.json()["result"]["13961"]["oncogenicity_classification"])




url = "https://api-evsrest.nci.nih.gov/api/v1/concept/ncit/"+disease["disease_id_ncit"]+"?include=full"




url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pccompound&id=" +  "679"   + "&retmode=json"
response  = requests.get(url)
response.json()["result"]["679"].keys()

result = json.loads(response.text)
 
 
 
 
 
disgen_df = pd.read_csv("./data/test-app/BIOSNAP Datasets/DG-AssocMiner_miner-disease-gene.tsv", sep="\t")
discat_df = pd.read_csv("./data/test-app/BIOSNAP Datasets/D-DoPathways_diseaseclasses (1).csv")

#------------------------


import requests
url = "https://api.cellosaurus.org/cell-line/CVCL_7526?format=txt&fields=id%2Ccc"
test  = requests.get(url)
test_l = [t for t in test.text.split("\n") if "CC   Sequence variation:" in t]
test_l

#------------------------

import requests
url = "https://api-evsrest.nci.nih.gov/api/v1/concept/ncit/C3224?include=full"
test  = requests.get(url)
pretty_print = json.loads(test.text)
set([t["type"] for t in pretty_print["inverseRoles"]])




import requests
url = "https://api-evsrest.nci.nih.gov/api/v1/concept/ncit/"+ "C21389" +"?include=full"
test  = requests.get(url)
pretty_print = json.loads(test.text)
[t for t in pretty_print["properties"] if t["type"]=="HGNC_ID"][0]["value"]



#------------------------

import requests
url = "https://api-evsrest.nci.nih.gov/api/v1/concept/ncit/C3224?include=full"
test  = requests.get(url)
pretty_print = json.loads(test.text)


import requests
url = "https://www.ebi.ac.uk/ols4/api/ontologies/bto/terms/http%253A%252F%252Fpurl.obolibrary.org%252Fobo%252FBTO_0000751"
test  = requests.get(url)
pretty_print = json.loads(test.text)








      # Get info for new gene
      url = "https://rest.genenames.org/fetch/hgnc_id/"+hgnc_id
      response  = requests.get(url, headers = {'Content-type': 'application/json'})
      #result = json.loads(response.text)
      
      if i==1:
        print(response.text)
        i+=1


