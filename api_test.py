import requests
import pandas as pd
import json
import yaml
import pprint

pheno_ino_api = requests.get(
    "https://phenotypes.healthdatagateway.org/api/v1/public/phenotypes/PH215/version/430/detail/?format=json")
# pheno_info_json= pheno_ino_api.text
# print(pheno_info_json)
# pheno_into_yaml = yaml.dump(pheno_ino_api.text, allow_unicode=True)


# pp = pprint.PrettyPrinter(indent=2)
# pp.pprint(pheno_info_json)

code_list_api = requests.get(
    "https://phenotypes.healthdatagateway.org/api/v1/public/phenotypes/PH215/version/430/export/codes/?format=json")
code_list_json = code_list_api.json()


# print(code_list_json[0].get("coding_system"))

def find_coding_systems(codelist_dict):
    list_coding_systems = []
    for code_dict in codelist_dict:
        list_coding_systems.append(code_dict.get("coding_system"))
    return set(list_coding_systems)

def search_value(codelist_dict, key, value):
    return [code for code in codelist_dict if code.get(key) == value]

coding_systems = find_coding_systems(code_list_json)
print(coding_systems)

final_dict = {}
for code_sys in coding_systems:
    final_dict[code_sys] = search_value(code_list_json, "coding_system", code_sys)

for key in final_dict.keys():

    print(key)
    print(len(final_dict.get(key)))
#print(final_dict.get("ICD10 codes"))
# print(code_list_json)
# pp.pprint(code_list_json)
pheno_json = pheno_ino_api.json()
# print(pheno_json[0])
# search  = requests.get("https://phenotypes.healthdatagateway.org/api/v1/public/phenotypes/?search=Alcohol")
# print(search.text)

# d =code_list_api.__dict__
# pp.pprint(d)
# y = json.load(pheno_info_json)
y = pheno_json[0]
# print(y["phenotype_id"])
# print(len(y.get("concepts")))
concept_dict = {}
# for cpt in  y.get("concepts"):
#    concept_dict[cpt.get("coding_system")] = cpt
# print(cpt.get("coding_system"))
# print(concept_dict.keys())

# print(type(concept_dict.get('Read codes v2')))
# a_dict = json.loads(y)
# print(a_dict.get("phenotype_id"))

# df = pd.read_csv('https://phenotypes.healthdatagateway.org/phenotypes/PH215/version/430/export/codes/')
# print(df)
