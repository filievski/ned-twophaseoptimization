import os
from KafNafParserPy import *
from SPARQLWrapper import SPARQLWrapper, JSON

def execute_query(q, x):
     sparql = SPARQLWrapper("http://live.dbpedia.org/sparql")
     sparql.setQuery(q)
     sparql.setReturnFormat(JSON)
     
     try:
          results = sparql.query().convert()
          result=results["results"]["bindings"][0]     
          return result[x]["value"]
     except:
          return None

reds={"http://dbpedia.org/resource/Ford": "http://dbpedia.org/resource/Ford_Motor_Company", "http://dbpedia.org/resource/Federal_reserve_system": "http://dbpedia.org/resource/Federal_Reserve_System", "http://dbpedia.org/resource/FTSE_100_index": "http://dbpedia.org/resource/FTSE_100_Index", "http://dbpedia.org/resource/US_dollar": "http://dbpedia.org/resource/United_States_dollar", "http://dbpedia.org/resource/Bear_market": "http://dbpedia.org/resource/Market_trend", "http://dbpedia.org/resource/Subprime_loan": "http://dbpedia.org/resource/Subprime_lending", "http://dbpedia.org/resource/Subprime_loan": "http://dbpedia.org/resource/Subprime_lending", "http://dbpedia.org/resource/Russell_2000_Index": "http://dbpedia.org/resource/Russell_2000", "http://dbpedia.org/resource/Taro_Aso": "http://dbpedia.org/resource/Tar?_As?"}
i=0
corpus="Airbus_boeing_CROMER_VUA_200115"
#corpus="Stock_market_CROMER_FBK_200115"
#corpus="Airbus_boeing_CROMER_VUA_200115"
for inputfile in os.listdir("wikinews_CROMER_200115/" + corpus):
    f="wikinews_CROMER_200115/" + corpus + "/" + inputfile
    output="../data/Gold_standard/CATNAFCROMER_airbus/" + inputfile.split("_")[0] + ".naf"
    try:    
        # Parse using the KafNafParser
        my_parser = KafNafParser(f)         
    except:
        print "Error with " + f
        continue
    
    for ent in my_parser.get_entities():
        for ref in ent.get_external_references():
            resource=ref.get_reference()
            if resource is not None:
                try: # try to get the resource redirect
                    result=reds[resource].encode("utf-8")
                    print "KNOWN:" + resource + " redirects to " + result
                    i+=1
                    ref.set_reference(result)
                except:
                    q="SELECT ?q WHERE { <" + resource + "> <http://dbpedia.org/ontology/wikiPageRedirects> ?q }"
                    result = execute_query(q, "q")
                    if result is not None:
                        result=result.encode("utf-8")
                        print resource + " redirects to " + result
                        reds[resource]=result
                        i+=1
                        ref.set_reference(result)
    my_parser.dump(output)
print i