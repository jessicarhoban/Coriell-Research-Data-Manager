
import requests


#

biomaterial_options = ['Cell line', 'Cell suspension', 'Donor organism', 
                       'Imaged specimen', 'Organoid', 'Specimen from organism']





# AGE INFO (TEXT)
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=ag&rows=500000"
response  = requests.get(url)
age_opts =  set([t.strip() for t in response.text.split("\n") if "AG" in t])

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=ag&rows=500000"
response2  = requests.get(url2)
age_opts2 =  set([t.strip() for t in response2.text.split("\n") if "AG" in t])

age_opts.update(age_opts2)


# Range lower, range upper ()





# DONOR INFO (TEXT)
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=donor-info&rows=500000"
response  = requests.get(url)
donor_opts =  set([t.strip() for t in response.text.split("\n") if "Donor information" in t])

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=donor-info&rows=500000"
response2  = requests.get(url2)
donor_opts2 =  set([t.strip() for t in response2.text.split("\n") if "Donor information" in t])

donor_opts.update(donor_opts2)













# %% CELL LINE OPTIONS -- ??
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=id&rows=500000"
response  = requests.get(url)
cellline_opts =  set([t.strip().replace("ID   ","")  for t in response.text.split("\n") if t.startswith("ID")])

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=id&rows=500000"
response2  = requests.get(url2)
cellline_opts2 =  set([t.strip().replace("ID   ","") for t in response2.text.split("\n") if t.startswith("ID")])

cellline_opts.update(cellline_opts2)
cellline_df = pd.DataFrame(cellline_opts, columns=["cellline_name"])















# GENE OPTIONS -- 1416
url = "https://api.cellosaurus.org/search/cell-line?q=ox:sapiens&format=txt&fields=knockout&rows=500000"
response  = requests.get(url)
etc_opts =  set([t.strip() for t in response.text.split("\n")])

url2 = "https://api.cellosaurus.org/search/cell-line?q=ox:musculus&format=txt&fields=knockout&rows=500000"
response2  = requests.get(url2)
etc_opts2 =  set([t.split(";")[3].strip() for t in response2.text.split("\n")])

etc_opts.update(etc_opts2)

