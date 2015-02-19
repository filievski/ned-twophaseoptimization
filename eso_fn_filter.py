#!/usr/bin/python

import sys, os
import json
from SPARQLWrapper import SPARQLWrapper, JSON
from KafNafParserPy import *

def intersect(a, b):
     return list(set(a) & set(b))

def extract_resource(dbr):
     return dbr.replace("http://dbpedia.org/resource/", "").replace("http://dbpedia.org/page/", "").strip()
     #.replace(".", "").replace("_", "").replace("-")

def create_db_ont(res):
     return "http://dbpedia.org/ontology/" + res     

def execute_ask_query(q):
     sparql = SPARQLWrapper("http://dbpedia.org/sparql")
     sparql.setQuery(q)
     sparql.setReturnFormat(JSON)
     results = sparql.query().convert()
     
     return results["boolean"]

def right_sentence(ent, parser):
    
        for ref in ent.get_references():
            
            target_ids = ref.get_span().get_span_ids()
            
            if target_ids and len(target_ids):
    
                token_id=target_ids[0].replace("t", "w")
                sent=parser.get_token(token_id).get_sent()
                
                return (int(sent)<=6)

def get_all_entities_with_references(parser):
     allent={}
     sr = {}
     entities = parser.get_entities()
     for ent in entities:
          for ref in ent.get_references():
               target_ids = ref.get_span().get_span_ids()
               
               token_id=target_ids[0].replace("t", "w")
               sent=parser.get_token(token_id).get_sent()
               if int(sent)<=6:
                    key = "-".join(target_ids)
                    externals = []
                    for ext_ref in ent.get_external_references():
                            externals.append(ext_ref.get_reference())
                    allent[key] = externals
                    sr[key] = []
     return [allent,sr]

def get_tids_key(ent, parser):
     for ref in ent.get_references():
          target_ids = ref.get_span().get_span_ids()
      
          token_id=target_ids[0].replace("t", "w")
          sent=parser.get_token(token_id).get_sent()
          return "-".join(target_ids)
              
def intersect_restrictions(domain, rset):
     translation_json = {"animate": ["Person", "Species"], "organization": ["Organisation"], "location": ["Place"], "place": ["Place"], "vehicle": ["MeanOfTransportation"], "concrete": ["Person", "Species", "MeanOfTransportation"] }
     new_set=[]
     for r in rset:
          try:
               tr=translation_json[r]
               for x in tr:
                    new_set.append(x)
          except KeyError: # if the class is outside of the scope, do not impose restrictions
               new_set = ["Place", "Organisation", "Person", "Species", "MeanOfTransportation"]
               break


     return intersect(new_set, domain)

def get_forbids(domain):
     forbidden=[]
     for t in ["Place", "Organisation", "Person", "Species", "MeanOfTransportation"]:
          if t not in domain:
               forbidden.append(create_db_ont(t))
               
     return forbidden

def create_eso_restrictions(parser, rests, all_restrictions):
     

     # Iterate over the predicates and check for ESO predicates in the external references
     for predicate in parser.get_predicates():
          for role in predicate.get_roles():

               # Get the head target id for the role                         
               the_span = role.get_span()
               id_head = the_span.get_id_head()
               
               #Next, check if the head can be found as (in) an entity
               for entity_key in rests:
                    if id_head in entity_key.split("-"):
                         temp_sr=[]
                         for ext_ref in role.get_external_references():
                              if ext_ref.get_resource()=='FrameNet' or ext_ref.get_resource()=='FrameNet+':
                                   fn_from_naf=ext_ref.get_reference().split("@")
                                   fn_pred=fn_from_naf[0]
                                   fn_role=fn_from_naf[1]
                                   try:
                                        for r in all_restrictions[fn_pred][fn_role].split(','):
                                             if r!="Thing":
                                                  temp_sr.append(r)
                                             else:
                                                  temp_sr = ["Place", "Organisation", "Person", "Species", "MeanOfTransportation"]
                                                  break
                                   except:
                                        temp_sr = ["Place", "Organisation", "Person", "Species", "MeanOfTransportation"]
                                        break
                         if temp_sr!=[] and temp_sr!=["Place", "Organisation", "Person", "Species", "MeanOfTransportation"]:
                              rests[entity_key].append(temp_sr)                              

     return rests
    

def clean_restrictions(s):
     new_sr = {}
     for skey in s:
          if s[skey] and s[skey]!=[]:
               domain= ["Place", "Organisation", "Person", "Species", "MeanOfTransportation"]
               for types_set in s[skey]:
                    new_domain=[]
                    for t in types_set:
                         for d in domain:
                              if t.strip()==d:
                                   new_domain.append(d)
                                   
                    domain=new_domain

               new_sr[skey]=domain
     return new_sr

#p="airbus"
#p="gm_chrysler_ford"
p="stock_market"
os.chdir("/Users/filipilievski/cltl/thesis/data/mynafs/" + p + "/vn_filter")

jd = open("/Users/filipilievski/cltl/thesis/Resources/OWLSOM.json")
all_restrictions=json.load(jd)

rest_entities=0
dominants=0
links=0

for input in os.listdir("."):


    output="/Users/filipilievski/cltl/thesis/data/mynafs/" + p + "/fn_filter/" + input     
    if os.path.isfile(output):
        continue

     
    # parse the XML
    try:
            my_parser = KafNafParser(input)
    except:
            print "Mission impossible: " + input
            continue
    
    print input

    [all_entities,sr]=get_all_entities_with_references(my_parser)
    
    mid=create_eso_restrictions(my_parser, sr, all_restrictions)
    print mid
    sr=clean_restrictions(mid)
    print sr
    
    for entity in my_parser.get_entities():
        if right_sentence(entity, my_parser):
            ek=get_tids_key(entity, my_parser)
            if len(all_entities[ek])<1:
                 continue
            elif ek in sr: #If there is restricted set for this entity
                 forbidden=get_forbids(sr[ek])
                 if len(forbidden)==0:
                      continue
                 rest_entities+=1
                 cntr=0
                 print forbidden
                 for ext_ref in entity.get_external_references():
                      if ext_ref.get_resource() in ["spotlight_v1", "selfixer"]:
                         q = "ASK WHERE {"
                         for rest in forbidden:
                              q+= " { <" + ext_ref.get_reference() + "> a <" + rest + "> } UNION"
                         q = q[:-6]
                         q += "}"
                         if execute_ask_query(q):
                              if cntr==0:
                                 dominants+=1
                                 print "Dominant!"
                              links+=1
                              ex=CexternalReference()
                              ex.set_confidence("0.0")
                              ex.set_resource("fn_filter_0.2")
                              ex.set_reference(ext_ref.get_reference())
                              entity.add_external_reference(ex)
                              print ex.get_reference(), forbidden

                      cntr+=1
                      

    my_parser.dump(output)
			
print rest_entities, links, dominants