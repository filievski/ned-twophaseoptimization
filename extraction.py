#!/usr/bin/env python

from KafNafParserPy import *

from rdflib import URIRef, Namespace
from rdflib.namespace import RDF,Namespace, NamespaceManager
from rdflib.graph import Graph    
from lxml import etree
import sys
import os
import math
from SPARQLWrapper import SPARQLWrapper, JSON
import itertools
import json
import urllib2
from py2neo import Graph, Node, Relationship
import re

def intersect(a, b):
     return list(set(a) & set(b))

########## NEO4J FUNCTIONS #####################

graph=Graph()

def neo_create_relationship(name1, name2, rel_json):
     print name1, name2, rel_json
     graph.cypher.execute("""
        MATCH (a:Thing),(b:Thing)
        WHERE a.name = {n1} AND b.name = {n2}
        CREATE (a)-[r:SEMS { ins : {ins}, outs: {outs}, props: {props}, path: {path} }]->(b)
        RETURN r
     """, {"n1": name1, "n2": name2, "ins": rel_json["ins"], "outs": rel_json["outs"], "props": rel_json["props"], "path": rel_json["path"]})
    
def neo_check_for_relation(name1, name2):

     record=graph.cypher.execute("MATCH (a {name:{n1}}), (b {name:{n2}}) MATCH (a)-[r:SEMS]->(b) RETURN r", {"n1": name1, "n2":name2})
     if record and record[0]:
          return record[0]
     else:
          return False
     
def neo_check_for_node(name):
    record=graph.cypher.execute("MATCH (a {name:{n1}}) RETURN a", {"n1": name})
    if record and record[0]:
        return record[0]
    else:
        return False

def neo_create_node(name, ins, outs):
    graph.cypher.execute("CREATE (c:Thing {name: {N}, ins:{ins}, outs:{outs}}) RETURN c", {"N": name, "ins": ins, "outs": outs})

########## end NEO4J FUNCTIONS #################

def get_all_entities_with_references(parser):
     allent={}
     sr = {}
     entities = parser.get_entities()
     for ent in entities:
          if ent.get_id()[0]=="e":
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

def lame_word(word): # a function to remove "a" , "the", "'s" and such words which affect the evaluation unnecessarily
    return word in ["the", "a"]

def sublistExists(listx, sublist):
    for i in range(len(listx)-len(sublist)+1):
        if sublist == listx[i:i+len(sublist)]:
            return True #return position (i) if you wish
    return False #or -1

def get_all_entities(parser):
     allent={}
     entities = parser.get_entities()
     for ent in entities:
        for ref in ent.get_references():
            
            target_ids = ref.get_span().get_span_ids()
            
            if target_ids and len(target_ids):
    
                token_id=target_ids[0].replace("t", "w")
                sent=parser.get_token(token_id).get_sent()
                
                if int(sent)<=6:
                
                    word=parser.get_term(target_ids[0]).get_lemma().lower()
                    if lame_word(word):
                        target_ids.pop(0)
                
                    key = "-".join(target_ids).replace("t", "")
                    externals = []
                    for ext_ref in ent.get_external_references():
                        externals.append(ext_ref.get_reference())
                    allent[key] = externals
     
     to_remove=[]               
     for ent_key in allent:
         for other_ent_key in allent:
            if other_ent_key!=ent_key:
                if sublistExists(ent_key.split("-"), other_ent_key.split("-")):
                    print ent_key, other_ent_key
                    to_remove.append(ent_key)
    
     new_all_ent = {}
     for this_key in allent:
        if this_key not in to_remove:
            new_all_ent[this_key]=allent[this_key]
    
     #print allent
     #if allent==[]:
     #   print "no  entities for this file"
     return new_all_ent

def normalize_filenames(path):
     os.chdir(path)
     for ifile in os.listdir("./"):
          if "_" in ifile:
               os.rename(ifile, ifile.split("_")[0] + ".naf").replace(".naf.naf", ".naf")

def get_sentences(p):
     path="/Users/filipilievski/cltl/thesis/data/corpus_NAF_output_141214/corpus_" + p + "/"
     os.chdir(path)

     for inputfile in os.listdir("."):
          print inputfile
          sentences={}
          try:    
               # Parse using the KafNafParser
               my_parser = KafNafParser(inputfile)
               
          except:
               print "Error with" + inputfile
               continue
          for token in my_parser.get_tokens():
               num=token.get_sent()
               sentences[token.get_id()]=num
          return sentences     

def get_entity_sentences(parser, entities):
     
     ent_sent={}
     for entity_key in entities:
          token_id=entity_key.split("-")[0]
          token_id="w" + token_id.replace("t", "")
          sent=int(parser.get_token(token_id).get_sent())
          if sent<=6:
               try:
                   ent_sent[sent].append(entity_key)
               except KeyError:
                    ent_sent[sent]=[entity_key]

     return ent_sent

def pred_filter(p, restrictions, parser, ents):
     
          
          sr2=restrictions
          # 1. VN restrictions          
          mid=create_vn_restrictions(parser, restrictions)
          
          sr1 = vn_to_dbpedia(mid)
          
          #print "SR1: "
          #print sr1
          
          #sr2=create_eso_restrictions(my_parser, sr2, all_restrictions)
          #print sr2
          #sr2=clean_restrictions(sr2)
     
          #print "SR2: "
          #print sr2
          
          # Intersect sr1 and sr2
          
          #final_sr = {}
          #for key in sr:
          #     try:
          #          if sr1[key]!=[] and sr2[key]!=[]:
          #               final_sr[key]=intersect(sr1[key], sr2[key])
          #          elif sr1[key]!=[]:
          #               final_sr[key]=sr1[key]
          #          elif sr2[key]!=[]:
          #               final_sr[key]=sr2[key]
          #     except KeyError:
          #          try:
          #               if sr1[key]!=[]:
          #                    final_sr[key]=sr1[key]
          #          except KeyError:
          #               try:
          #                    if sr2[key]!=[]:
          #                         final_sr[key]=sr2[key]
          #               except KeyError:
          #                    final_sr[key]=[]

          #print "SR: "
          #print final_sr
          
          # Apply restrictions to filter (TODO)
          print ents
          ents = apply_restrictions(sr1, ents)

          
          print "####################################"
          return ents


def get_resource_outs(ent):
     q1 = """
     SELECT count(*) as ?cnt
     WHERE { <""" + ent + """> ?prop ?anchor }
     """
     return execute_query(q1, "cnt")
     
def get_resource_ins(ent):
     q2 = """
     SELECT count(*) as ?cnt
     WHERE { ?outer ?prop <""" + ent + """> }
     """
     return execute_query(q2, "cnt")
     
def extract_popularity_params(ents):
     for e in ents:
          ename=extract_resource(e)
          if not neo_check_for_node(ename):
               ins = get_resource_ins(e)
               outs = get_resource_outs(e)
               full_rel=neo_create_node(ename, ins, outs)
               print ename, ins, outs
     
#def extract_popularity(ents):
#     for e in ents:
#          ename=extract_resource(e)
#          if not neo_check_for_node(ename):
#               full_rel=neo_create_node(ename, compute_popularity_sum(e), compute_popularity_division(e))
#               print ename

def extract_semantics(all_pairs):
     for pair in all_pairs:
          p1=extract_resource(pair[0])
          p2=extract_resource(pair[1])
          #try:
               
          if not neo_check_for_relation(p1, p2): # If relation does not exist, create it
               full_rel=create_relation(pair)
          #except UnicodeEncodeError:
          #     print "Encoding error for " + p1 + " and " + p2
                    
def create_relation(pair):
     coef_ins=compute_shared_ins_for_pair(pair)
     coef_outs=compute_shared_outs_for_pair(pair)
     coef_props=compute_shared_props_for_pair(pair)
     coef_path=discover_rels_for_pair(pair)
     if coef_ins is not None and coef_outs is not None and coef_path is not None and coef_props is not None:
          return neo_create_relationship(extract_resource(pair[0]), extract_resource(pair[1]), {"ins":coef_ins, "outs": coef_outs, "props": coef_props, "path": coef_path})
     else:
          print "error with " + str(pair)
          return

def compute_shared_ins_for_pair(pair):

     queryAB="SELECT count(?ab) as ?cntab { ?ab ?prop1 <" + pair[0] + "> . ?ab ?prop2 <" + pair[1] + "> }"
     len_ab = int(execute_query(queryAB, "cntab"))
     
     return len_ab

def compute_shared_outs_for_pair(pair):

     queryAB="SELECT count(?ab) as ?cntab { <" + pair[0] + "> ?prop1 ?ab . <" + pair[1] + "> ?prop2 ?ab }"
     len_ab = int(execute_query(queryAB, "cntab"))
     
     return len_ab

def compute_shared_props_for_pair(pair):
     
     queryAB="SELECT count(?prop) as ?cntab { <" + pair[0] + "> ?prop ?a . <" + pair[1] + "> ?prop ?b }"
     len_ab = int(execute_query(queryAB, "cntab"))
     
     return len_ab
               
def execute_query(q, x):
     sparql = SPARQLWrapper("http://dbpedia.org/sparql")
     sparql.setQuery(q)
     sparql.setReturnFormat(JSON)
     results = sparql.query().convert()
     
     try:
          result=results["results"]["bindings"][0]     
          return result[x]["value"]
     except:
          return None
     
def discover_rels_for_pair(pair):
     e1=extract_resource(pair[0])
     e2=extract_resource(pair[1])
     some_url="http://lod2.inf.puc-rio.br/scs/similarities.json?entity1=db:" + e1 + "&entity2=db:" + e2
     some_url = some_url.encode('utf-8')
     try:
          content = json.loads(urllib2.urlopen(some_url).read())
     except urllib2.HTTPError, e:
          print e.read()
          return None
     except urllib2.URLError:
          print "Oops, Timeout occured. Go to the next one"
          return None
          
     if content["semanticconnectivity"]!="":
          scs = content["semanticconnectivity"]["scs"]
     else:
          scs=0.0
     return scs
     
def extract_resource(dbr):
     return dbr.replace("http://dbpedia.org/resource/", "").replace("http://dbpedia.org/page/", "").strip()
     #.replace(".", "").replace("_", "").replace("-")

def create_db_resource(res):
     return "http://dbpedia.org/resource/" + res

def create_db_ont(res):
     return "http://dbpedia.org/ontology/" + res       
     
def get_all_pairs_for_sentence(sent, entities):
     # Run the shared properties & graph distance

     ents_of_interest=[]
     for es in sent:
          ents_of_interest.append(entities[es])
     all_combinations=list(itertools.product(*ents_of_interest))
     
     all_pairs=[]
     for combi in all_combinations:
          for pair in list(itertools.combinations(combi, 2)):
               if pair[0].strip()!=pair[1].strip():
                    all_pairs.append(pair)
          
     return all_pairs

def store_for_phase_2():     
     sparql = SPARQLWrapper("http://dbpedia.org/sparql")
     #    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     
     all_pairs = []
     #num_unique_uris = 0
     all_entity_uris = []
     for p in ["gm_chrysler_ford"]:
     
          path="/Users/filipilievski/cltl/thesis/data/mynafs/" + p + "/self/"
          os.chdir(path)
          


          for inputfile in os.listdir("."):
               print inputfile
               try:    
                    # Parse using the KafNafParser
                    my_parser = KafNafParser(inputfile)
                    
               except:
                    print "Error with" + inputfile
                    continue
          
               entities = get_all_entities(my_parser)
               entity_sent = get_entity_sentences(my_parser, entities)
               for sent in entity_sent:
                    if len(entity_sent[sent])>1:
                         ap=get_all_pairs_for_sentence(entity_sent[sent], entities)
                         for pair in ap:
                              rev_pair=[pair[1], pair[0]]
                              if pair not in all_pairs and rev_pair not in all_pairs:
                                   all_pairs.append(pair)
               
          
               # Store the popularity params
               for ek in entities:
                    for uri in entities[ek]:
                         uri=uri.strip()
                         if uri not in all_entity_uris:
                              all_entity_uris.append(uri)
     num_unique_uris = len(all_entity_uris)
     extract_popularity_params(all_entity_uris)

     print "Popularity done. Now extract the relations. Total: "
     print len(all_pairs)
     extract_semantics(all_pairs)

               #pop_ent = owesom_popularity(entities[ek])
          
          #all_pairs = []
          #
          ## Run the shared properties & graph distance
          #for sentence in entity_sent:
          #     if len(entity_sent[sentence])>1:
          #          ents_of_interest=[]
          #          for es in entity_sent[sentence]:
          #               ents_of_interest.append(entities[es])
          #          all_combinations=list(itertools.product(*ents_of_interest))
          #          
          #          for combi in all_combinations:
          #               for pair in list(itertools.combinations(combi, 2)):
          #                    if pair not in all_pairs:
          #                         all_pairs.append(pair)
          #               
          #print "Now the relations..."               
          #extract_semantics(all_pairs)
          #break
     

def process_phase_2(p):

     sparql = SPARQLWrapper("http://dbpedia.org/sparql")
     #    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     
     path="/Users/filipilievski/cltl/thesis/data/corpus_NAF_output_141214/corpus_" + p + "/"
     os.chdir(path)
     
     for inputfile in os.listdir("."):
          print inputfile
          try:    
               # Parse using the KafNafParser
               my_parser = KafNafParser(inputfile)
               
          except:
               print "Error with" + inputfile
               continue
     
          entities, restrictions = get_all_entities_with_references(my_parser)
          #print entities
          entity_sent = get_entity_sentences(my_parser, entities)
          #print entity_sent

          # Run the popularity entity
          for ek in entities:
               extract_popularity(entities[ek])

               #pop_ent = owesom_popularity(entities[ek])
          
          all_pairs = []
          
          # Run the shared properties & graph distance
          for sentence in entity_sent:
               if len(entity_sent[sentence])>1:
                    ents_of_interest=[]
                    for es in entity_sent[sentence]:
                         ents_of_interest.append(entities[es])
                    all_combinations=list(itertools.product(*ents_of_interest))
                    
                    for combi in all_combinations:
                         for pair in list(itertools.combinations(combi, 2)):
                              if pair not in all_pairs:
                                   all_pairs.append(pair)
                         
          print "Now the relations..."               
          extract_semantics(all_pairs)
          break

     
if __name__ == "__main__":

     #normalize_filenames("/Users/filipilievski/cltl/thesis/data/Gold_standard/CATNAFCROMER_" + p + "/")
     #normalize_filenames("/Users/filipilievski/cltl/thesis/data/corpus_NAF_output_141214/corpus_" + p)

     
     
     # evaluate_all(p)
                        
     # sent=get_sentences(p)

     #process_phase_1(p)
     
     store_for_phase_2()
     
     ######### PHASE 2 ###############
     
     #process_phase_2(p)

######################################################################################

                    
#                    # Run semantic modules
#                    #owesom_shared_properties(all_combinations)
##                    owesom_shared_incomings(all_combinations)
##                    owesom_shared_outcomings(all_combinations)
##                    owesom_rel_discovery(all_combinations)
#          
          
        

     
