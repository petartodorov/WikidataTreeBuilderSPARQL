#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function,  division
from datetime import datetime,  timedelta
import simplejson as json
import requests

import sys

class WikidataGetEntities(object):

    def __init__(self,  languages=None,  lookupClaims=None):
        self._lookupClaims = lookupClaims   or ["P571", "P275", "P101", "P135", "P348", "P306", "P1482", "P277", "P577", "P366", "P178", "P31", "P271", "P2572", "P3966", "P144","P170","P1324"]
        self._languages = languages         or ["fr", "en"]

        self.session = requests.Session()
        self.queryTemplate="https://www.wikidata.org/w/api.php?action=wbgetentities&ids={0}&format=json"

    def query(self, Q):
        """Query to Wikidata API to get an entity by id."""
        page = self.session.get(self.queryTemplate.format(Q))
        if page.status_code != 200:
            print("ERROR WHILE RETRIEVING "+item+". STATUS:"+str(page.status_code))
            return {}
        return json.loads(page.text)

    def enrichTable(self, inputObj):
        """Send the query from Wikidata and process the result with getProperties"""
        queryResult=self.query(inputObj['id'])
        entities=queryResult.get("entities")
        if len(entities.keys()) != 1:
            return inputObj.update({"ERROR MESSAGE: ASKED FOR 1 ELEMENT, GOT "+str(len(entities.keys()))})
        return self.getProperties(entities[entities.keys()[0]])


    def getProperties(self,  item):
        """Given an entity from Wikidata, return a dict with all desired properties; suitable for import in pandas."""

        toreturn = dict ()
        toreturn["id"] = item.get("id")

        foundClaims=item.get("claims") or []

        # Out of all claims,  filter the interesting ones,  as defined on __init__
        foundRelevantClaims = [claim for claim in foundClaims if claim in self._lookupClaims]

        # And now we loop over all relevant claims
        for claim in foundRelevantClaims:
            toreturn[claim] = list ()
            for i in foundClaims[claim]:
                dataType=i.get("mainsnak", {}).get("datavalue", {}).get("type", "blank")

                # Handle differently different datatypes
                if dataType=="None":
                    continue
                elif dataType=="wikibase-entityid":
                    result=i.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id", "blank")
                elif dataType=="time":
                    result=i.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("time", "blank")
                    result_precision=i.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("precision", "blank")
                    if not claim+"_precision" in toreturn.keys():
                        toreturn[claim+"_precision"]=list ()
                    toreturn[claim+"_precision"].append(result_precision)
                elif dataType=="string":
                    result=i.get("mainsnak", {}).get("datavalue", {}).get("value", "blank")
                else:
                    result="UNHANDLED FORMAT :"+dataType

                # Some claims are qualified, add a new column with the qualifier
                if i.get("qualifiers"):
                    for qualifier in i["qualifiers"]:
                        qck="_".join((claim, qualifier))
                        if not qck in toreturn.keys():
                            toreturn[qck] = list ()
                        for desc_qualfier in i["qualifiers"][qualifier]:
                            dataType=desc_qualfier.get("datatype")
                            # Different claims are qualified differently,  handle each case:
                            if dataType=="time":
                                toreturn[qck].append(desc_qualfier.get("datavalue", {}).get("value", {}).get("time", "blank"))
                                if not qck+"_precision" in toreturn.keys():
                                    toreturn[qck+"_precision"]=list ()
                                toreturn[qck+"_precision"].append(desc_qualfier.get("datavalue", {}).get("value", {}).get("precision", "blank"))
                            elif dataType=="wikibase-item":
                                qid=desc_qualfier.get("datavalue", {}).get("value", {}).get("id", "blank")
                                toreturn[qck].append(qid)
                            elif dataType=="monolingualtext":
                                result=desc_qualfier.get("datavalue", {}).get("value", {}).get("text", "blank")
                                toreturn[qck].append(result)
                            else:
                                toreturn[qck].append("UNHANDLED QUALIFIER "+dataType)
                toreturn[claim].append(result)

        for lang in self._languages:
            toreturn[lang+"Label"]=item.get("labels", {}).get(lang, {}).get("value", "blank")
            toreturn[lang+"Description"]=item.get("descriptions", {}).get(lang, {}).get("value", "blank")
            toreturn[lang+"Aliases"]=[alias.get("value", "blank") for alias in item.get("aliases", {}).get(lang, [])]

        return toreturn

def main():
    """Example run"""
    enrichTable=WikidataGetEntitities()

if __name__=="__main__":
    main()
