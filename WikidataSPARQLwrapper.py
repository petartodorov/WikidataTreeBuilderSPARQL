#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function, division
from datetime import datetime, timedelta
import simplejson as json
import requests

class WikidataTreeBuilder(object):
    def __init__(self):
        #Initilize query template, query prefix, and asking for the result to be in json
        self.queryTemplate="""SELECT ?entityId ?entityLabel WHERE
                {{
                    ?entity wdt:{0} wd:{1}.
                    BIND (replace(str(?entity), str("http://www.wikidata.org/entity/"), "") AS ?entityId)
                    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,fr" }}
                }}"""

        self.queryPrefix="https://query.wikidata.org/bigdata/namespace/wdq/sparql?query="
        self.session=requests.Session()
        self.session.headers.update({'Accept': 'application/sparql-results+json'})
        self._debug=False
        self._keepTraceVisitedNodes=False

    def fromRoot(self,rootId,forbidden):
        # When the class is called, it should be with the "fromRoot" function
        return self.makeTree(rootId,[],forbidden)

    def queryWikidata(self,P,Q):
        #form and execute the wikidata query
        query=self.queryTemplate.format(P,Q)
        if self._debug:
            print(query)
        page=self.session.get(self.queryPrefix+query)
        if page.status_code!=200:
            print("ERROR WHILE RETRIEVING "+P+" OF "+str(Q)+". STATUS:"+str(page.status_code))
            return []
        return json.loads(page.text)

    def makeTree(self,node,visited,forbidden):
        #Recursive function to explore the tree

        flare=dict ()
        flare["name"]=node

        if self._keepTraceVisitedNodes:
            flare["visitedNodes"]=visited

        #Query wikidata for all instanceof (P31) and subclassof (P279)
        queryResult={P:self.queryWikidata(P,node[0]) for P in ["P31","P279"]}

        result = [(item["entityId"]["value"],item["entityLabel"]["value"]) for item in queryResult["P31"]["results"]["bindings"]] + \
        [(item["entityId"]["value"],item["entityLabel"]["value"]) for item in queryResult["P279"]["results"]["bindings"]]

        if len(result)==0:
            return flare

        # recursively call the function for all subclasses and instances
        flare["children"]=[self.makeTree(entry,visited+[node[0]],forbidden) for entry in result if not entry[0] in visited and not entry[0] in forbidden]

        # Optional : this part puts all entries that are not subclasses into a subnode called "singleEntries". For viz.
        newlyStructured=list ()
        singleEntries = list ()
        for index, entry in enumerate(flare["children"]):
            if "children" in entry.keys():
                newlyStructured.append(entry)
            else:
                singleEntries.append(entry)


        newlyStructured.append({"name":["-1","singleEntries"],"children":singleEntries})
        flare["children"]=newlyStructured

        return flare

def main():
    # Example run
    tree=WikidataTreeBuilder()
    print(datetime.now())
    flare=tree.fromRoot(rootId=("Q21198","Computer Science"),forbidden=["Q7889","Q28923"])
    print(datetime.now())
    with open("output.json","w") as f:
        json.dump(flare,f,indent=4)

if __name__=="__main__":
    main()
