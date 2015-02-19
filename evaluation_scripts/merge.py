#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import csv

################# Merging them into one file #####################

def normalize(s):
    return s.strip().replace("http://dbpedia.org/page", "http://dbpedia.org/resource")

print "############################### Creating separate files done #################################"
print "############################### NOW MERGING FILES INTO ONE.. #################################"

for filename in os.listdir("../evaluation/nwr/"):
	if filename.endswith(".conll"):
		print filename
		nwr=open("../evaluation/nwr/" + filename, "rb")

		nwr_reader = csv.reader(nwr, delimiter=' ', quotechar="\"")
		
		gs=open("../evaluation/gs/" + filename, "rb")
		gs_reader = csv.reader(gs, delimiter=' ', quotechar="\"")
		
		nwr_entities={}
		for row in nwr_reader:
		    nwr_entities[row[0]]=row[1]
		    print "+!"
		gs_entities={}
		for row in gs_reader:
		    gs_entities[row[0]]=row[1]
		    print "<>"
		toprint = []
		    
		inserted_gss=[]
		for nwrkey in sorted(nwr_entities.iterkeys()):
		    nwrs = nwrkey.split("-")
		    found=False
		    for n in nwrs:
		
			for gskey in gs_entities:
			    gss = gskey.split("-")
			    if n in gss:
				toprint.append("e" + str(min(gss[0], nwrs[0])) + " " + nwr_entities[nwrkey] + " " + gs_entities[gskey])
				inserted_gss.append(gss)
				found=True
				break
			if found is True:
			    break
		    if found is False:
			toprint.append("e" + str(nwrs[0]) + " " + nwr_entities[nwrkey] + " O")
			
		for gskey in sorted(gs_entities.iterkeys()):
		    gss = gskey.split("-")
		    found=False
		    for g in gss:
		
			for nwrkey in nwr_entities:
			    nwrs = nwrkey.split("-")
			    if g in nwrs:
				if gss not in inserted_gss:
				    toprint.append("e" + str(min(gss[0], nwrs[0])) + " " + nwr_entities[nwrkey] + " " + gs_entities[gskey])
				    inserted_gss.append(gss)
				found=True
				break
			if found is True:
			    break
		    if found is False and gss not in inserted_gss:
			toprint.append("e" + str(gss[0]) + " O " + gs_entities[gskey])
			inserted_gss.append(gss)
		       
		w=open("../evaluation/merged/" + filename, "w")
		for x in sorted(toprint):
		    w.write(x + "\n")
		
		nwr.close()
		gs.close()
		    
print "############################### Merging files into one: done #################################"
print "############################### NOW EVALUATING... ############################################"

tp=0.0
tpfp=0.0
tpfn=0.0

fc=0

for filename in os.listdir("../evaluation/merged/"):

	f=open("../evaluation/merged/" + filename, "r")
	
	for line in f:
		line=line.strip()
#		print line
		components=line.split(" ")
		nwr_entity=normalize(components[1])
		gs_entity=normalize(components[2])
		if nwr_entity==gs_entity:
			tp+=1
		if nwr_entity!="O":
			tpfp+=1
	#	else:
	#		print "nwr entity is O"
		if gs_entity!="O":
			tpfn+=1
	#	else:
	#		print "gs entity is O"
	fc+=1

recall_ned = tp/tpfn
precision_ned = tp/tpfp
f_ned = 2*recall_ned*precision_ned/(recall_ned+precision_ned)    

print "File count: " + str(fc)
print precision_ned, recall_ned, f_ned