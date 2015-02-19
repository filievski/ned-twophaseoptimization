#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os
from KafNafParserPy import *
from random import randrange
from py2neo import Graph, Node, Relationship
import itertools
import urllib2
import json
import math
from SPARQLWrapper import SPARQLWrapper, JSON

#graph=Graph()

def extract_resource(dbr):
     return dbr.replace("http://dbpedia.org/resource/", "").replace("http://dbpedia.org/page/", "").strip()
     #.replace(".", "").replace("_", "").replace("-")

def neo_create_relationship(name1, name2, rel_json):
     print name1, name2, rel_json
     graph.cypher.execute("""
        MATCH (a:Thing),(b:Thing)
        WHERE a.name = {n1} AND b.name = {n2}
        CREATE (a)-[r:SEMS { ins : {ins}, outs: {outs}, props: {props}, path: {path} }]->(b)
        RETURN r
     """, {"n1": name1, "n2": name2, "ins": rel_json["ins"], "outs": rel_json["outs"], "props": rel_json["props"], "path": rel_json["path"]})     

def neo_check_for_relation(name1, name2):

     record=graph.cypher.execute("MATCH (a {name:{n1}})-[r]-(b {name:{n2}}) RETURN r", {"n1": name1, "n2":name2})
     try:
          return record[0]
     except:
          return None
     
def neo_check_for_node(name):
    record=graph.cypher.execute("MATCH (a {name:{n1}}) RETURN a", {"n1": name})
    if record and record[0]:
        return record[0]
    else:
        print "ERROR: " + name
        return False
     
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

def get_sentences_with_entity_ids(my_parser):
     ent_sent={}
     for ent in my_parser.get_entities():
          skippy=True
          for er in ent.get_external_references():
               skippy=False
               break
          
          if skippy:
               continue
          else:
               entity_key=ent.get_id()
               for ref in ent.get_references():
                    target_ids = ref.get_span().get_span_ids()
                 
                    if target_ids and len(target_ids):
         
                         token_id=target_ids[0].replace("t", "w")
                         sent=my_parser.get_token(token_id).get_sent()
                         if int(sent)<=6:
                              try:
                                  ent_sent[sent].append(entity_key)
                              except KeyError:
                                   ent_sent[sent]=[entity_key]

     return ent_sent

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
                
                    key = ent.get_id()
                    externals = []
                    for ext_ref in ent.get_external_references():
                         if ext_ref.get_resource() in ["spotlight_v1", "selfixer"]:
                              externals.append(ext_ref.get_reference())
                    allent[key] = externals
     

     return allent

def right_sentence(ent, parser):
    
        for ref in ent.get_references():
            
            target_ids = ref.get_span().get_span_ids()
            
            if target_ids and len(target_ids):
    
                token_id=target_ids[0].replace("t", "w")
                sent=parser.get_token(token_id).get_sent()
                
                return (int(sent)<=6)
               
def generate_all_combis(full_ents):

     ents_of_interest=[]
     for ent in full_ents:
          ents_of_interest.append(ent["links"])
     
     return list(itertools.product(*ents_of_interest))

def retrieve_self_outs_from_dbpedia(p):

     queryA="SELECT count(?ab) as ?cntab { <" + p + "> ?prop1 ?ab }"
     len_p = float(execute_query(queryA, "cntab"))
     
     return len_p

def retrieve_self_props_from_dbpedia(p):

     queryA="SELECT count(?ab) as ?cntab { <" + p + "> ?prop1 ?ab }"
     len_p = float(execute_query(queryA, "cntab"))
     
     return len_p

def retrieve_self_ins_from_dbpedia(p):

     queryA="SELECT count(?ab) as ?cntab { ?ab ?prop1 <" + p + "> }"
     len_p = float(execute_query(queryA, "cntab"))
     
     return len_p

def retrieve_shared_ins_from_dbpedia(pair):

     print "look for shared ins"
     queryAB="SELECT count(?ab) as ?cntab { ?ab ?prop1 <" + pair[0] + "> . ?ab ?prop2 <" + pair[1] + "> }"
     len_ab = float(execute_query(queryAB, "cntab"))
     
     return len_ab

def retrieve_shared_outs_from_dbpedia(pair):

     print "look for shared outs"
     queryAB="SELECT count(?ab) as ?cntab { <" + pair[0] + "> ?prop1 ?ab . <" + pair[1] + "> ?prop2 ?ab }"
     len_ab = float(execute_query(queryAB, "cntab"))
     
     return len_ab

def retrieve_shared_props_from_dbpedia(pair):

     print "look for shared props"
     queryAB="SELECT count(?ab) as ?cntab { <" + pair[0] + "> ?ab ?prop1 . <" + pair[1] + "> ?ab ?prop2  }"
     len_ab = float(execute_query(queryAB, "cntab"))
     
     return len_ab

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

def normalize_score(len_ab, len_a, len_b): # for INS , OUTS and PROPS
     if len_ab==0.0:
          len_ab=1.0
     if len_a==0.0:
          len_a=1.0
     if len_b==0.0:
          len_b=1.0
     sem_rel = 1 - (math.log(max(len_a,len_b))-math.log(len_ab))/(math.log(len_dbpedia)-math.log(min(len_a,len_b)))
     return sem_rel

def find_values_for_mentions_pair(pair):

     pairs=[]
     for ex1 in pair[0]["links"]:
          for ex2 in pair[1]["links"]:
               if [ex2,ex1] not in pairs:
                    pairs.append([ex1, ex2])

     all_mighty_json={"scs":[], "ins":[], "outs":[], "props":[]}

     max_scs=0.0
     max_scs_pair=[]

     for pair in pairs:
          p1 = extract_resource(pair[0])
          p2 = extract_resource(pair[1])
          if p1!=p2:
               data = neo_check_for_relation(p1, p2)
               # SCS
               if data and data["r"]:
                    ab_ins=float(data["r"]["ins"])
                    
                    ab_outs=float(data["r"]["outs"])
                    
                    ab_props=float(data["r"]["props"])
                    
                    len_scs=float(data["r"]["path"])
                    
               else: # If for some reason the relation is not in neo4j
                    print "expensive relation for " + p1 + " and " + p2
                    len_scs=discover_rels_for_pair(pair)
                    ab_ins=retrieve_shared_ins_from_dbpedia(pair)
                    ab_outs=retrieve_shared_outs_from_dbpedia(pair)
                    ab_props=retrieve_shared_props_from_dbpedia(pair)
                    if ab_ins is not None and ab_outs is not None and len_scs is not None and ab_props is not None:
                         neo_create_relationship(extract_resource(pair[0]), extract_resource(pair[1]), {"ins":ab_ins, "outs": ab_outs, "props": ab_props, "path": len_scs})
                    else:
                         print "error with " + str(pair)
     
               # In any case, get the individual ins, outs, props for normalization purposes
               
               p1_rec=neo_check_for_node(p1)
               if p1_rec and p1_rec[0]:
                    p1_node=p1_rec[0]
                    a_ins=float(p1_node["ins"])
                    a_outs=float(p1_node["outs"])
                    a_props=a_outs
               else:
                    print "node from dbpedia"
                    a_ins = retrieve_self_ins_from_dbpedia(pair[0])
                    a_outs = retrieve_self_outs_from_dbpedia(pair[0])
                    a_props = retrieve_self_props_from_dbpedia(pair[0])
               p2_rec=neo_check_for_node(p2)
               if p2_rec and p2_rec[0]:
                    p2_node=p2_rec[0]
                    b_ins=float(p2_node["ins"])
                    b_outs=float(p2_node["outs"])
                    b_props=b_outs
               else:
                    print "node from dbpedia"
                    b_ins = retrieve_self_ins_from_dbpedia(pair[1])
                    b_outs = retrieve_self_outs_from_dbpedia(pair[1])
                    b_props = retrieve_self_props_from_dbpedia(pair[1])
                    
               # Normalize!
               len_ins = normalize_score(ab_ins, a_ins, b_ins)               
               len_outs = normalize_score(ab_outs, a_outs, b_outs)
               len_props = normalize_score(ab_props, a_props, b_props)
          else:
               len_scs = 1.0
               len_ins = 1.0
               len_outs = 1.0
               len_props = 1.0

          if len_scs>0.0:
               all_mighty_json["scs"].append({"pair":[p1,p2], "score":len_scs})
          if len_ins>0.0:
               all_mighty_json["ins"].append({"pair":[p1,p2], "score":len_ins})
          if len_outs>0.0:
               all_mighty_json["outs"].append({"pair":[p1,p2], "score":len_outs})
          if len_props>0.0:
               all_mighty_json["props"].append({"pair":[p1,p2], "score":len_props})
     return all_mighty_json

def sort_best(arr, ak):
     return sorted(arr[ak], key=lambda k: k.get('score', 0), reverse=True)
          
def filter_links(ent_list, scores, k):
     while True:
          did_sth=0
          for ent in ent_list:
               eid=ent["eid"]
               for o_elink in ent["links"]:
                    score=0.0
                    elink=extract_resource(o_elink)
                    print elink
                    for ment_pair in scores:
                         if eid==ment_pair["ents"][0]:
                              found=False
                              for option in ment_pair[k]:
                                   if elink.strip() == option["pair"][0].strip():
                                        found=True
                                        break
                              if not found:
                                   ent["links"].remove(o_elink)
                                   print "Remove " + elink
                                   did_sth+=1
                                   break
                         elif eid==ment_pair["ents"][1]:
                              found=False
                              for option in ment_pair[k]:
                                   if elink.strip() == option["pair"][1].strip():
                                        found=True
                                        break
                              if not found:
                                   ent["links"].remove(o_elink)
                                   print "Remove " + elink
                                   did_sth+=1
                                   break
          for ment_pair in scores:
               id1=ment_pair["ents"][0]
               id2=ment_pair["ents"][1]
               for ent in ent_list:
                    if ent["eid"]==id1:
                         for link_pair in ment_pair[k]:
                              if link_pair["pair"][0] not in ent["links"]:
                                   ment_pair[k].remove(link_pair)
                                   did_sth+=1
                    elif ent["eid"]==id2:
                         for link_pair in ment_pair[k]:
                              if link_pair["pair"][1] not in ent["links"]:
                                   ment_pair[k].remove(link_pair)
                                   did_sth+=1
                                        
          #if did_sth==0:
          break
     return scores, ent_list    
     #return scores, ent_list
     
def filter_options(pos, options, which):
     for option in options:
          if option["pair"][pos]!=which:
               options.remove(option)
     return options
     
def collective_decision(scores, ent_list, k):
     max_score=0.0
     max_combi=[]
     norm=0

     combinations=generate_all_combis(ent_list)
     
     for combi in combinations:               
          combi_score=0.0
          combi_pairs=list(itertools.combinations(combi, 2))
          for cpair in combi_pairs:
               found=False
               cpair=list(cpair)
               cpair[0]=extract_resource(cpair[0])
               cpair[1]=extract_resource(cpair[1])
               for ment_pair in scores:
                    for lookin_full in ment_pair[k]:
                         lookin=lookin_full["pair"]
                         
                         if cpair == lookin or [cpair[1],cpair[0]] == lookin:
                              found=True
                              combi_score+=lookin_full["score"]
                              break
               if found is False:
                    break
          if combi_score>max_score:
               max_score=combi_score
               max_combi=combi
     norm=len(combi_pairs)
     print k, max_score/norm, max_combi

     return max_combi, max_score/norm

#p="stock_market"
#p="airbus"
p="gm_chrysler_ford"
os.chdir("/Users/filipilievski/cltl/thesis/data/mynafs/" + p + "/popularity/")

len_dbpedia = 4233000

fired_sentences = 0

analyzer={}

for input in os.listdir("."):

     print input
     output="/Users/filipilievski/cltl/thesis/data/mynafs/" + p + "/coherence/" + input     
#     if os.path.isfile(output):
#         continue

    # parse the XML
     try:
          my_parser = KafNafParser(input)
     except:
          print "Mission impossible: " + input
          continue
     
     entities = get_all_entities(my_parser)
     #print entities
     
     # Get the entities 
     sents = get_sentences_with_entity_ids(my_parser)
     for sent in sents:
          try:
               analyzer[len(sents[sent])]+=1
          except:
               analyzer[len(sents[sent])]=1
          if len(sents[sent])>1:
               fired_sentences+=1
     #          all_stuff = []
     #          for ent in sents[sent]:
     #               all_stuff.append({"eid":ent, "links":entities[ent]})
     #          
     #          all_pairs=[]
     #          for entity1 in all_stuff:
     #               for entity2 in all_stuff:
     #                    if entity1["eid"]!=entity2["eid"] and [entity2, entity1] not in all_pairs:
     #                         all_pairs.append([entity1, entity2])
     #          all_scores=[]
     #          for pair in all_pairs:
     #               pair_values=find_values_for_mentions_pair(pair)
     #               pair_values["scs"]=sort_best(pair_values, "scs")
     #               pair_values["ins"]=sort_best(pair_values, "ins")
     #               pair_values["outs"]=sort_best(pair_values, "outs")
     #               pair_values["props"]=sort_best(pair_values, "props")
     #               pair_values["ents"]=[pair[0]["eid"], pair[1]["eid"]]
     #               all_scores.append(pair_values)
     #               
     #
     #          
     #          max_scs_combi, max_scs_score=collective_decision(all_scores, all_stuff, "scs")
     #          max_ins_combi, max_ins_score=collective_decision(all_scores, all_stuff, "ins")
     #          max_outs_combi, max_outs_score=collective_decision(all_scores, all_stuff, "outs")
     #          max_prop_combi, max_prop_score=collective_decision(all_scores, all_stuff, "props")
     #
     #          cnt=0
     #          for ekey in sents[sent]:
     #               # SCS
     #               if len(max_scs_combi) and max_scs_score>0.0:
     #                    er = CexternalReference()
     #                    er.set_confidence(str(max_scs_score))
     #                    er.set_resource("scs_coherence")
     #                    er.set_reference(max_scs_combi[cnt])
     #                    my_parser.add_external_reference_to_entity(ekey, er)
     #               # INS
     #               if len(max_ins_combi) and max_ins_score>0.0:
     #                    er = CexternalReference()
     #                    er.set_confidence(str(max_ins_score))
     #                    er.set_resource("ins_coherence")
     #                    er.set_reference(max_ins_combi[cnt])
     #                    my_parser.add_external_reference_to_entity(ekey, er)
     #               # OUTS
     #               if len(max_outs_combi) and max_outs_score>0.0:
     #                    er = CexternalReference()
     #                    er.set_confidence(str(max_outs_score))
     #                    er.set_resource("outs_coherence")
     #                    er.set_reference(max_outs_combi[cnt])
     #                    my_parser.add_external_reference_to_entity(ekey, er)
     #               # PROP
     #               if len(max_prop_combi) and max_prop_score>0.0:
     #                    er = CexternalReference()
     #                    er.set_confidence(str(max_prop_score))
     #                    er.set_resource("prop_coherence")
     #                    er.set_reference(max_prop_combi[cnt])
     #                    my_parser.add_external_reference_to_entity(ekey, er)
     #               cnt+=1
     #
     #my_parser.dump(output)

print "Fired sentences: " + str(fired_sentences)
print analyzer