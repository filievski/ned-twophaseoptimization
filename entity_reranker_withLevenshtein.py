#!/usr/bin/python

## This script takes a naf file with an entities layer as input 
## For each entity disambiguation candidate, it looks up the dbpedia type (class)
## of each candidate and reranks the candidates based on the dbpedia class ranking 
## for the domain at hand. 
## This version loads the dbpedia resource table in memory
## It also only prints the highest ranking candidate 
## V1.001 This version also computes the Levenshtein distance and reranks the candidates accordingly 

## Created by: Marieke van Erp (marieke.van.erp@vu.nl)
## Date: 20 November 2014 

import sys, os
from KafNafParserPy import *
import codecs
import re
import datetime
from random import randrange
from Levenshtein import distance


def right_sentence(ent, parser):
    
        for ref in ent.get_references():
            
            target_ids = ref.get_span().get_span_ids()
            
            if target_ids and len(target_ids):
    
                token_id=target_ids[0].replace("t", "w")
                sent=parser.get_token(token_id).get_sent()
                
                return (int(sent)<=6)

# Resource Types table 
dbpedia_table =  "DBpediaResourceTypeTableOnlyRanked.tsv"

lp = Clp()
lp.set_name('vua-nedtype-reranking-levenshtein')
lp.set_version('1.1')
lp.set_beginTimestamp()

types = {}
with open(dbpedia_table) as f:
	for line in f:
		line = re.sub('\n', '',line)
		fields = line.split('\t')
		types[fields[0]] = fields[1]

p="stock_market"

os.chdir("/Users/filipilievski/cltl/thesis/data/mynafs/" + p + "/fn_filter")

rer_ent=0
rer_links=0
all_entity_nr=0
for input in os.listdir("."):

	output="/Users/filipilievski/cltl/thesis/data/mynafs/" + p + "/ned_reranker/" + input     
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
			all_entity_nr+=1

			print entity.get_id()
			terms = []
			entity_text = "" 
			# Get the span of the references (entities) 
			for reference in entity.get_references():
				for span_obj in reference:
					for item in span_obj.get_span_ids():
						terms.append(item)
			# Also get the text that denote the entities 
			for term in terms:
				entity_text = entity_text + " " + my_parser.get_term(term).get_lemma()
			entity_text = entity_text.rstrip()
			reranked = {}
			# Also get the actual references 
			for external_reference in entity.get_external_references():
				resource_name = external_reference.get_reference().replace('http://dbpedia.org/resource/','').replace('http://dbpedia.org/page/','')
				LD=10
				try: 
					LD = distance(entity_text, resource_name)
				except:
					pass	
				if resource_name in types and external_reference.get_resource() == 'spotlight_v1' and LD < 10:
					score = int(types[resource_name]) - LD
					reranked[external_reference.get_reference()] = score
			if len(reranked) > 0:
				rer_ent+=1
				rer_links+=len(reranked)
				print reranked
				max_key = sorted(reranked.items(), key=lambda t: -t[1])[0][0]
				max_value=float(reranked[max_key])
	#			print reranked[max_key]
				for k in reranked:
					print k
					new_reference = CexternalReference()			
					new_reference.set_resource('vua-type-reranker-v1.1')
					new_reference.set_reference(k)
					new_reference.set_confidence(str(float(reranked[k])/max_value))
					my_parser.add_external_reference_to_entity(entity.get_id(),new_reference)
		
	lp.set_endTimestamp()
	my_parser.add_linguistic_processor('entities', lp)
	
	my_parser.dump(output)
	
print rer_ent, rer_links, all_entity_nr
			

