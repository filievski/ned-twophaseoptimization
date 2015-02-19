import os
from KafNafParserPy import *
from SPARQLWrapper import SPARQLWrapper, JSON

def create_db_resource(res):
    return "http://dbpedia.org/resource/" + res

def execute_query(q, x):
     sparql = SPARQLWrapper("http://dbpedia.org/sparql")
     sparql.setQuery(q)
     sparql.setReturnFormat(JSON)
     
     try:
          results = sparql.query().convert()
          result=results["results"]["bindings"][0]     
          return result[x]["value"]
     except:
          return None

def right_sentence(ent, parser):
    
        for ref in ent.get_references():
            
            target_ids = ref.get_span().get_span_ids()
            
            if target_ids and len(target_ids):
    
                token_id=target_ids[0].replace("t", "w")
                sent=parser.get_token(token_id).get_sent()
                
                return (int(sent)<=6)

def get_tids_key(ent, parser):
     for ref in ent.get_references():
          target_ids = ref.get_span().get_span_ids()
      
          token_id=target_ids[0].replace("t", "w")
          sent=parser.get_token(token_id).get_sent()
          return "-".join(target_ids)

def simple_empty_links_fixer(tids, my_parser):
     words = []
     for tid in tids.split("-"):
          words.append(my_parser.get_token(tid.replace("t", "w")).get_text())
     res=("_").join(words)

     db_res=create_db_resource(res)
     return db_res

    
def check_if_exists(resource):
     q="SELECT ?q WHERE { <" + resource + "> <http://dbpedia.org/ontology/wikiPageRedirects> ?q }"
     result = execute_query(q, "q")
     if result is not None:
          return result
     else:
          q="SELECT ?q WHERE { <" + resource + "> ?p ?q }"
          result = execute_query(q, "q")
          if result is not None:
               return resource
          else:
               return None
      
            
p = "stock_market"
#p="airbus"
#p="gm_chrysler_ford"
os.chdir("/Users/filipilievski/cltl/thesis/data/corpus_NAF_output_141214/corpus_" + p + "/")

countie = 0
empties = 0

for inputje in os.listdir("."):

    print inputje

    output="/Users/filipilievski/cltl/thesis/data/mynafs/" + p + "/self/" + inputje
    
#    if os.path.isfile(output):
#       continue

    # parse the XML
    try:
        my_parser = KafNafParser(inputje)
    except:
        print "Mission impossible: " + inputje
        continue  
     
    for entity in my_parser.get_entities():
        if right_sentence(entity, my_parser):
            empty=True

            for exts in entity.get_external_references():
                empty=False
                break
            if empty:
                empties+=1
                tids=get_tids_key(entity, my_parser)
                db_res=simple_empty_links_fixer(tids, my_parser)
    
                simple_try=check_if_exists(db_res)
            
                if simple_try:
                    print tids, simple_try
                    ext_ref = CexternalReference()
                    ext_ref.set_resource("selfixer")
                    ext_ref.set_confidence("0.2")
                    ext_ref.set_reference(simple_try)
                    entity.add_external_reference(ext_ref)
                    countie+=1

    my_parser.dump(output)
    
print countie, empties
