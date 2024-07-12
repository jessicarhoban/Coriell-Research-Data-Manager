
import requests


#
molecule_options = ['total RNA','polyA RNA','cytoplasmic RNA',
                    'nuclear RNA','genomic DNA','protein', 'other']



biomaterial_options = ['Cell line', 'Cell suspension', 'Donor organism', 
                       'Imaged specimen', 'Organoid', 'Specimen from organism']






# CELL LINE OPTIONS --
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=id&rows=500000"
response  = requests.get(url)
cellline_opts =  set([t.strip() for t in response.text.split("\n") if t.startswith("ID")])

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=id&rows=500000"
response2  = requests.get(url2)
cellline_opts2 =  set([t.strip() for t in response2.text.split("\n") if t.startswith("ID")])

cellline_opts.update(cellline_opts2)





# TISSUE OPTIONS -- 882
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=site&rows=500000"
response  = requests.get(url)
tissue_opts =  set([t.split(";")[1].strip() for t in response.text.split("\n") if "Derived from site:" in t])

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=site&rows=500000"
response2  = requests.get(url2)
tissue_opts2 =  set([t.split(";")[1].strip() for t in response2.text.split("\n") if "Derived from site:" in t])

tissue_opts.update(tissue_opts2)




# CELL OPTIONS -- 223
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=cell&rows=400000"
response  = requests.get(url)
cell_opts =  set([t.split(":")[1].split(";")[0].strip() for t in response.text.split("\n") if "Cell type:" in t])

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=cell&rows=400000"
response2  = requests.get(url2)
cell_opts2 =  set([t.split(":")[1].split(";")[0].strip() for t in response2.text.split("\n") if "Cell type:" in t])

cell_opts.update(cell_opts2)




# DISEASE OPTIONS -- 3055
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=di&rows=500000"
response  = requests.get(url)
disease_opts =  set([t.split(";")[-1].strip() for t in response.text.split("\n") if t.startswith("DI")])

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=di&rows=500000"
response2  = requests.get(url2)
disease_opts2 =  set([t.split(";")[-1].strip() for t in response2.text.split("\n") if t.startswith("DI")])

disease_opts.update(disease_opts2)





# CATEGORY OPTIONS -- 
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=ca&rows=500000"
response  = requests.get(url)
category_opts =  set([t.strip() for t in response.text.split("\n") if t.startswith("CA")])

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=ca&rows=500000"
response2  = requests.get(url2)
category_opts2 =  set([t.strip() for t in response2.text.split("\n") if t.startswith("CA")])

category_opts.update(category_opts2)





# SEX OPTIONS -- 
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=sx&rows=500000"
response  = requests.get(url)
sex_opts =  set([t.strip() for t in response.text.split("\n") if t.startswith("SX")])

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=sx&rows=500000"
response2  = requests.get(url2)
sex_opts2 =  set([t.strip() for t in response2.text.split("\n") if t.startswith("SX")])

sex_opts.update(sex_opts2)





# GENE OPTIONS -- 1416
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=sequence-variation&rows=500000"
response  = requests.get(url)
gene_opts =  set([t.split(";")[3].strip() for t in response.text.split("\n") if t.startswith("CC")])

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=sequence-variation&rows=500000"
response2  = requests.get(url2)
gene_opts2 =  set([t.split(";")[3].strip() for t in response2.text.split("\n") if t.startswith("CC")])

gene_opts.update(gene_opts2)






# KNOCKOUT -- 
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=knockout&rows=500000"
response  = requests.get(url)
etc_opts =  set([t.strip() for t in response.text.split("\n")])

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=knockout&rows=500000"
response2  = requests.get(url2)
etc_opts2 =  set([t.strip() for t in response2.text.split("\n")])

etc_opts.update(etc_opts2)

