import sys
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

def triple_id2triple_name_query(head:str,relation:str,tail:str) -> list:
    wikidata_sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    wikidata_sparql.setReturnFormat(JSON)

    query = """
    SELECT ?elementLabel WHERE {{
    VALUES (?element ?order) {{
        (wd:{id_head} 1)  
        (wd:{id_rel} 2)  
        (wd:{id_tail} 3) 
    }}
    ?element rdfs:label ?elementLabel .
    FILTER(LANG(?elementLabel) = "en")  # Filters to ensure we get the English label
    }}
    ORDER BY ?order
    """
    sparql_query = query.format(id_head=head, id_rel=relation, id_tail=tail)
    wikidata_sparql.setQuery(sparql_query)
    try:
        ret = wikidata_sparql.queryAndConvert()
    except Exception as e:
        print(e)
    elem_list = []
    print(ret)
    for elem in ret['results']['bindings']:
        elem_list.append(elem['elementLabel']['value'])
    return elem_list

def id_df2name_df_query(df:pd.DataFrame) -> pd.DataFrame:
    name_dict = {}
    id_dict = 0
    for row in df.iterrows():
        row_id = row[1]
        head = row_id[0]
        relation = row_id[1]
        tail = row_id[2]
        elem_list = triple_id2triple_name(head, relation, tail)
        name_dict[id_dict] = elem_list
        id_dict += 1

    name_df = pd.DataFrame.from_dict(name_dict, orient='index',columns=['Head','Relation','Tail'])
    return name_df