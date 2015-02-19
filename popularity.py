#!/usr/bin/python
# -- coding: utf-8 --

import sys, os
from KafNafParserPy import *
from random import randrange
from py2neo import Graph, Node, Relationship
import math

graph=Graph()

def extract_resource(dbr):
     return dbr.replace("http://dbpedia.org/resource/", "").replace("http://dbpedia.org/page/", "").strip()
     #.replace(".", "").replace("_", "").replace("-")
     
def neo_check_for_node(name):
    record=graph.cypher.execute("MATCH (a {name:{n1}}) RETURN a", {"n1": name})
    if record and record[0]:
        return record[0]
    else:
        print "ERROR: " + name
        return False

def right_sentence(ent, parser):
    
        for ref in ent.get_references():
            
            target_ids = ref.get_span().get_span_ids()
            
            if target_ids and len(target_ids):
    
                token_id=target_ids[0].replace("t", "w")
                sent=parser.get_token(token_id).get_sent()
                
                return (int(sent)<=6)


p="gm_chrysler_ford"
os.chdir("/Users/filipilievski/cltl/thesis/data/mynafs/" + p + "/ned_reranker")

len_dbpedia = 4233000

for input in os.listdir("."):

    output="/Users/filipilievski/cltl/thesis/data/mynafs/" + p + "/popularity/" + input     
    if os.path.isfile(output):
         continue

    # parse the XML
    try:
            my_parser = KafNafParser(input)
    except:
            print "Mission impossible: " + input
            continue
    # Get the entities 
    for entity in my_parser.get_entities():
        if right_sentence(entity, my_parser):
            for ext_ref in entity.get_external_references():
               if ext_ref.get_resource() in ["spotlight_v1", "selfixer"]:
                    uri_refs=neo_check_for_node(extract_resource(ext_ref.get_reference()))
                    if uri_refs:
                        uri_ref=uri_refs[0]
                        #print uri_ref["name"]
                        ins=float(uri_ref["ins"])
                        outs=float(uri_ref["outs"])
                        new_reference = CexternalReference()			
                        new_reference.set_resource('popularity-v0.2')
                        new_reference.set_reference(ext_ref.get_reference())
                        if ins!=0.0 or outs!=0.0:
                             new_reference.set_confidence(str(math.log(ins+outs)/math.log(len_dbpedia)))
                        else:
                             new_reference.set_confidence("0.0")
                        my_parser.add_external_reference_to_entity(entity.get_id(),new_reference)
                        

    my_parser.dump(output)
			
