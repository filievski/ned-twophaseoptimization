#!/usr/bin/python
# -*- coding: utf-8 -*-

# This script reads a CAT XML file and converts it to CoNLL format 
# This script also takes into account the named entities from the CROMER layer that 
# were already included in the CAT files by FBK.

# Note: wids and tids are a quick fix. Double check if indeed wid == tid 


from lxml import etree
import codecs
import sys, os
import csv
import re
import codecs
from KafNafParserPy import * 

def normalize(s):
	return s.strip().replace("http://dbpedia.org/page", "http://dbpedia.org/resource")

def right_sentence(ent, parser):
    
        for ref in ent.get_references():
            
            target_ids = ref.get_span().get_span_ids()
            
            if target_ids and len(target_ids):
    
                token_id=target_ids[0].replace("t", "w")
                sent=parser.get_token(token_id).get_sent()
                
                return (int(sent)<=6)

def sublistExists(listx, sublist):
    for i in range(len(listx)-len(sublist)+1):
        if sublist == listx[i:i+len(sublist)]:
            return True #return position (i) if you wish
    return False #or -1

def get_all_entities_for_upper_bound(parser, gs_entities):
     n=6

     allent={}
     entities = parser.get_entities()
     
	
     for ent in entities:
        for ref in ent.get_references():
            
            target_ids = ref.get_span().get_span_ids()
            
            if target_ids and len(target_ids):
    
                token_id=target_ids[0].replace("t", "w")
                sent=parser.get_token(token_id).get_sent()
                
                if int(sent)<=n:
                
                    word=parser.get_term(target_ids[0]).get_lemma().lower()
                
                    key = "-".join(target_ids).replace("t", "")
		    
		    right_entity="O"
		    for key_id in key.split("-"):
			for gs_key in gs_entities:
				if key_id in gs_key.split("-"):
					right_entity=gs_entities[gs_key]
		    right_entity=normalize(right_entity)
				
		    if right_entity=="O" or right_entity=="":
			max_ref=""
		    else:
			max_ref=""
			for ext_ref in ent.get_external_references():
				if right_entity==normalize(ext_ref.get_reference()):
					max_ref=ext_ref.get_reference()		

                    allent[key] = max_ref

     allent=remove_outer_entities(allent)
    
     #print allent
     #if allent==[]:
     #   print "no  entities for this file"
     return allent
	
def get_reference_for_resource(all_refs, c, forbidden):
	max_ref=""
	max_val=-0.1
	for ref in all_refs:
	    if ref.get_resource() in c and ref.get_reference() not in forbidden:
		#print "yes, found"
		if ref.get_confidence()>max_val:
			#print "yes, bigger"
			max_val=ref.get_confidence()
			max_ref=ref.get_reference()
	return max_ref

def make_combination(ent, forbidden):
	all_decisions={}
	# NWR + SELF
	nwr_self_consider=["spotlight_v1", "selfixer"]
	this_dec=get_reference_for_resource(ent.get_external_references(), nwr_self_consider, forbidden)
	if this_dec.strip()!="":
		s=0.3738
		try:
#		    all_decisions[this_dec]+=1
		    all_decisions[this_dec]+=s
		except:
#		    all_decisions[this_dec]=1
		    all_decisions[this_dec]=s
	# POP
	pop_consider=["popularity-v0.2"]
	this_dec=get_reference_for_resource(ent.get_external_references(), pop_consider, forbidden)
	if this_dec.strip()!="":
		s=0.3901
		try:
#		    all_decisions[this_dec]+=1
		    all_decisions[this_dec]+=s
		except:
#		    all_decisions[this_dec]=1
		    all_decisions[this_dec]=s  
	# RERANKER
	rer_consider=["vua-type-reranker-v1.1"]
	this_dec=get_reference_for_resource(ent.get_external_references(), rer_consider, forbidden)
	if this_dec.strip()!="":
		s=0.3539
		try:
#		    all_decisions[this_dec]+=1
		    all_decisions[this_dec]+=s
		except:
#		    all_decisions[this_dec]=1
		    all_decisions[this_dec]=s
	# SEMANTIC MODS
	for i in ["scs", "prop"]:
	    consider=[i + "_coherence"]
	    this_dec=get_reference_for_resource(ent.get_external_references(), consider, forbidden)
	    if this_dec.strip()!="":
		s=0.0
		if i=="scs":
			s=0.3537
		else:
			s=0.3274
		try:
#		    all_decisions[this_dec]+=1
		    all_decisions[this_dec]+=s
		except:
#		    all_decisions[this_dec]=1
		    all_decisions[this_dec]=s
	sorted_all_decisions=sorted(all_decisions.items(), key=lambda x: x[1])
	try:
		return max(all_decisions.iterkeys(), key=(lambda key: all_decisions[key]))
	except ValueError:
		return ""

def remove_outer_entities(allent):
	c=0
	to_remove=[]
	for ent_key in allent:
	    for other_ent_key in allent:
	       if other_ent_key!=ent_key:
		   if sublistExists(ent_key.split("-"), other_ent_key.split("-")):
		       to_remove.append(ent_key)
       
	new_all_ent = {}
	for this_key in allent:
	   if this_key not in to_remove:
	       new_all_ent[this_key]=allent[this_key]
	       if len(new_all_ent[this_key]) and new_all_ent[this_key]!="None":
			c+=1
	return new_all_ent, c
	
def get_all_entities(parser, which):
     filtertje=[]
     consider=[]
     print which
     nwr_consider=["spotlight_v1", "selfixer"]
     reranks=0
     if which=="nwr":
	n=6
	if module=="nwr":
	   consider=["spotlight_v1"]
	elif module=="self":
	   consider=["spotlight_v1", "selfixer"]
	elif module=="reranker":
	   consider=["vua-type-reranker-v1.1"]
	elif module=="popularity":
	   consider=["popularity-v0.2"]
	elif module=="spotlight":
	   consider=["spotlight_cltl"]
	elif module in ["vn", "fn"]:
	    consider=["spotlight_v1", "selfixer"]
	    filtertje=[module + "_filter_0.2"]
	elif module in ["ins", "outs", "scs", "prop"]:
	    consider=[module + "_coherence"]
	elif module=="filter_combi":
	    filtertje=["vn_filter_0.2", "fn_filter_0.2"]
		
	print "consider", consider
     else:
	n=5
     
     allent={}
     entities = parser.get_entities()
     
     gsl=0
     nwrl=0
     for ent in entities:
        for ref in ent.get_references():
            
            target_ids = ref.get_span().get_span_ids()
            
            if target_ids and len(target_ids):
    
                token_id=target_ids[0].replace("t", "w")
                sent=parser.get_token(token_id).get_sent()
                
                if int(sent)<=n:
                
                    word=parser.get_term(target_ids[0]).get_lemma().lower()
                
                    key = "-".join(target_ids).replace("t", "")
		    max_conf=-0.1
		    max_ref=""
		    max_nwr_conf=-0.1
		    max_nwr_ref=""
		    forbidden=[]
		    ### First, get filters if such exist ! #############
		    if which=="nwr" and filtertje!=[]:
			for ext_ref in ent.get_external_references():
				if ext_ref.get_resource() in filtertje:
					forbidden.append(ext_ref.get_reference())
		    print forbidden, key
		    if (module=="combi" or module=="filter_combi") and which=="nwr":
			#make_combination_with_filters(ent.get_external_references(), forbidden)
			x=make_combination(ent, forbidden)
			allent[key]=x
		    else:
			for ext_ref in ent.get_external_references():
			    if which=="gs":
				    max_ref=ext_ref.get_reference()
			    elif ext_ref.get_resource() in consider:
				    nwrl+=1
				    if float(ext_ref.get_confidence())>=max_conf:
					    if ext_ref.get_reference() not in forbidden:
						    max_conf=float(ext_ref.get_confidence())
						    max_ref=ext_ref.get_reference()
					    else:
						    print ext_ref.get_reference()
			    elif ext_ref.get_resource() in nwr_consider:
				    if float(ext_ref.get_confidence())>=max_nwr_conf:
					    if ext_ref.get_reference() not in forbidden:
						    max_nwr_conf=float(ext_ref.get_confidence())
						    max_nwr_ref=ext_ref.get_reference()
			if which=="nwr":
				if max_ref=="":
					max_ref=max_nwr_ref
				elif max_ref!=max_nwr_ref:
					print max_nwr_ref, max_ref, key
					reranks+=1
			if which=="nwr" and max_ref=="" and module in ["ins", "outs", "scs", "prop"]:
			    consider2=["spotlight_v1", "selfixer"]
			    for ext_ref in ent.get_external_references():
				    if ext_ref.get_resource() in consider2:
					    if float(ext_ref.get_confidence())>=max_conf:
						    max_conf=float(ext_ref.get_confidence())
						    max_ref=ext_ref.get_reference()
						    
    
			allent[key] = max_ref

     if which=="gs":
	allent, gsl=remove_outer_entities(allent)
    
     #print allent
     #if allent==[]:
     #   print "no  entities for this file"
     return allent, gsl, nwrl, reranks


################# Creating the separate files ######################

# ARGV[1] -> which corpus
# ARGV[2] -> what to evaluate (nwr, self, fn, vn, reranker, popularity, scs, ins, outs, props, spotlight)

if len(sys.argv)<3:
	print "Too little arguments! \nUsage: python evaluation.py <corpus> <top_module>"
	sys.exit(0)

corpus = sys.argv[1]
module=sys.argv[2]
src="fn_filter"
if module=="spotlight":
	src="spotlight"
	
num_gs_ent=0
num_nwr_ent=0

gs_links_count=0
nwr_links_count=0

reranks=0

for filename in os.listdir("../data/mynafs/" + corpus + "/" + src + "/"):
	print filename
	if filename.endswith(".naf"):

		# GS
		
		gsfile = open("../data/Gold_standard/CATNAFCROMER_" + corpus + "/" + filename, "r")
		
		gs_out=open("../evaluation/gs/" + filename.split(".")[0] + ".conll", "wb")
		spamwriter2=csv.writer(gs_out, delimiter=' ', quotechar="\"")
		my_parser2 = KafNafParser(gsfile)
		
		e2, gsl, nwrl, rer = get_all_entities(my_parser2, "gs")
		gs_links_count+=gsl
		
		num_gs_ent+=len(e2)
		
		for k in sorted(e2.iterkeys()):
			link=e2[k].encode("utf-8").strip()
			if link=="":
				link="None"
			spamwriter2.writerow([k, link])

		# NWR file
		print filename + " is OK"
		nwrfile = open("../data/mynafs/" + corpus + "/" + src + "/" + filename,"r")
		
		nwr_out=open("../evaluation/nwr/" + filename.split(".")[0] + ".conll", "wb")
		spamwriter=csv.writer(nwr_out, delimiter=' ', quotechar="\"")
		my_parser = KafNafParser(nwrfile)
		
		e, gsl, nwrl, rer_temp=get_all_entities(my_parser, "nwr")
		reranks+=rer_temp
#		e= get_all_entities_for_upper_bound(my_parser, e2)
		nwr_links_count+=nwrl
		
		for k in sorted(e.iterkeys()):
			link=e[k].encode("utf-8").strip()
			if link=="":
				link="None"
			spamwriter.writerow([k, link])
		
		num_nwr_ent+=len(e)
			
		nwrfile.close()
		gsfile.close()

#		break

print reranks