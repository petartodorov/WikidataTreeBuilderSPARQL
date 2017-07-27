#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function, division
from datetime import datetime, timedelta
import simplejson as json
import requests
from itertools import product as allpairs
import re
import pandas as pd


class WikidataTreeQuery(object):
    """Class to :
        * Query wikidata for all descendants of a node;
        * Structure the result as an arboresence;
        * Create a 'flare' dictionary object, suitable for writing as a json file
          and visualisation with d3js
        * Create a table with all descendants of a node, their properties as
          extracted from Wikidata, and all the paths to go from the root to the
          given node
    """

    def __init__(self, debug=None, labelsLanguages=None, queryLabels=None, lookupClaims=None, defaultLanguage=None, queryEndpoint=None, propertiesSetMembership=None):
        """
        Initialize query template, query prefix, asking for the result to be in json, and set default values for parameters.

        The following parameters have the following default settings:

        # The endpoint to send the query:
        queryEndpoint = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"

        # Whether we are in verbose mode:
        debug = false

        # The labels we want to get from the base:
        queryLabels = ["rdfs:label", "skos:altLabel", "schema:description"]

        # In which languages we want to get those labels :
        labelsLanguages = ["en", "fr"]

        # A list of the properties of interest, their values will be printed in the output for each entry:
        (References for names: https://www.wikidata.org/wiki/Wikidata:List_of_properties/all_in_one_table)
        lookupClaims = ["P571", "P275", "P101", "P135", "P348", "P306", "P1482", "P277", "P577", "P366", "P178", "P31", "P279", "P2572", "P3966", "P144", "P170", "P1324"]

        # Which properties define set membership. Default are elementOf and subClassOf:
        propertiesSetMembership = ["P31", "P279"]

        # What is the default language for the metadata of the output file:
        defaultLanguage = "en"
        """
        self.columns = list()
        self.nodesInTree = list()

        self.queryTemplateBase = """SELECT DISTINCT ?entity {0} {{?entity wdt:P31*/wdt:P279* wd:{1}. {2} {3}}}"""

        self.prefixURI = "http://www.wikidata.org/entity/"

        self._queryEndpoint = queryEndpoint                     or "https://query.wikidata.org/bigdata/namespace/wdq/sparql"
        self._debug = debug                                     or False
        self._labelsLanguages = labelsLanguages                 or ["en", "fr"]
        self._defaultLanguage = defaultLanguage                 or "en"
        self._lookupClaims = lookupClaims                       or ["P571", "P275", "P101", "P135", "P348", "P306", "P1482", "P277", "P577", "P366", "P178", "P31", "P279", "P2572", "P3966", "P144", "P170", "P1324"]
        self._queryLabels = queryLabels                         or ["rdfs:label", "skos:altLabel", "schema:description"]

        self._propertiesSetMembership = propertiesSetMembership or ["P31", "P279"]

        self.session = requests.Session()
        self.session.headers.update({'Accept': 'application/sparql-results+json'})

        self.getProperties()

        self.visitedNodes = dict()
        self.labels = dict ()
        self.labels["singleEntries"] = "singleEntries"
        self.QIDpattern = re.compile("^Q[0-9]+$")



    def getProperties(self):
        """Function to get human-readable labels of all properties"""
        template = """SELECT ?propertyId ?propertyLabel WHERE
                  {{?property a wikibase:Property.
                      BIND (replace(str(?property), str("{0}"), "") AS ?propertyId)
                      SERVICE wikibase:label {{bd:serviceParam wikibase:language "{1}" .
                  }}
                }}"""
        query = template.format(self.prefixURI, self._defaultLanguage)
        if self._debug:
            print(query)

        response = self.session.post(self._queryEndpoint, data={"query": query})
        if response.status_code != 200:
            raise Exception("QUERY ENDPOINT CONNECTION PROBLEM! STATUS: "+str(response.status_code)+"\nQUERY TEXT:\n"+query)

        results = json.loads(response.text).get("results", {}).get("bindings", [])

        self.property2text = {item["propertyId"]["value"]: re.sub(r"[^\w]", "_", item["propertyLabel"]["value"]) for item in results}
        self.propertiesSetMembership = [p+"_"+self.property2text[p] for p in self._propertiesSetMembership]
        return


    def queryStringProperties(self):
        """Returns the part of the query that asks for different listProperties,
        based on the parameters already set up."""

        template = """OPTIONAL {{?entity wdt:{0} ?{1}.}}"""

        for item in self._lookupClaims:
            self.columns.append("?"+item+"_"+self.property2text[item])

        return " ".join([template.format(item, item+"_"+self.property2text[item]) for item in self._lookupClaims])


    def queryStringDataInLabels(self):
        """Returns the part of the query that asks for different labels of entities. Loop over all label-language pairs"""

        template = """OPTIONAL {{?entity {0} ?{1}_{2} filter (lang(?{1}_{2}) = "{2}").}}"""
        if any(len(item.split(":")) != 2 for item in self._queryLabels):
            raise ValueError("All query labels should have prefixes (prefix:label)!")
        label_language_pairs = list(allpairs(self._queryLabels, self._labelsLanguages))

        for item in label_language_pairs:
            self.columns.append("?{0}_{1}".format(item[0].split(":")[1], item[1]))

        return " ".join([template.format(item[0], item[0].split(":")[1], item[1]) for item in label_language_pairs])


    def buildQuery(self, root):
        """On the queryTemplateBase, build a query, using two other functions
        for subparts of the query string"""

        partLabels = self.queryStringDataInLabels()
        partProperties = self.queryStringProperties()
        columns = " ".join(set(self.columns))

        return self.queryTemplateBase.format(columns, root, partLabels, partProperties)


    def queryWikidata(self, query):
        """Execute the query built with buildQuery, save the result (in self.flatData),
        and save the set of subnodes for each node in order to build the tree
        (in self.subnodesPerNode). Takes as input the output of the buildQuery function"""

        if self._debug:
            print(self._queryEndpoint)
            print("\nText of the query:\n")
            print(query)

        response = self.session.post(self._queryEndpoint, data={"query": query})

        if response.status_code != 200:
            raise Exception("QUERY ENDPOINT CONNECTION PROBLEM! STATUS: "+str(response.status_code)+"\nQUERY TEXT:\n"+query)
        if self._debug:
            print("Query succeeded!")

        try:
            self.flatData = json.loads(response.content).get("results", {}).get("bindings", [])
        except:
            raise Exception("Query deadline is expired! You may try re-running it later or simplifying it.")

        self.subnodesPerNode = dict()

        # Lookup specifically for claims that define set membership.

        for item in self.flatData:
            P_subnodeOf = (item.get(p, {}).get("value") for p in self.propertiesSetMembership)
            subnodeOf = [node.split("/")[-1] for node in P_subnodeOf if node]
            itemId = item["entity"]["value"].split("/")[-1]
            for node in subnodeOf:
                if not node:
                    continue
                if not node in self.subnodesPerNode:
                    self.subnodesPerNode[node] = list()
                self.subnodesPerNode[node].append(itemId)
        for node in self.subnodesPerNode:
            self.subnodesPerNode[node] = list(set(self.subnodesPerNode[node]))


    def getLabels(self, fullListOfNodes):
        """Function to convert a list of nodes to list of human-readable labels.
        Saves the result in the self.labels dictionary"""

        template = """SELECT * WHERE {{?entity rdfs:label ?label filter (lang(?label) = "{0}"). VALUES (?entity) {{{1}}}}}"""
        fullListOfNodes = list(set(fullListOfNodes))
        chunks = [fullListOfNodes[x:x+1000] for x in range(0, len(fullListOfNodes), 1000)]
        result = list()
        for listOfNodes in chunks:
            query = template.format(self._defaultLanguage, "".join(["(wd:{0})".format(node) for node in listOfNodes]))
            if self._debug:
                print(query)
            response = self.session.post(self._queryEndpoint, data = {"query": query})
            if response.status_code != 200:
                raise Exception("QUERY ENDPOINT CONNECTION PROBLEM! STATUS: "+str(response.status_code)+"\nQUERY TEXT:\n"+query)
            result += json.loads(response.text).get("results", {}).get("bindings", [])
        self.labels.update({item["entity"]["value"].split("/")[-1]: item["label"]["value"] for item in result})


    def resultWikidataAggregateRows(self, df):
        """Function to transform the data frame to one-entity-per-line"""
        for column in df.columns:
            if self._debug:
                print("Aggregating results for column "+column+" of the query result table, datetime: "+str(datetime.now()))
            if column == "entity":
                continue
            grouped = df.groupby("entity").apply(lambda x: x[column])
            df[column] = df["entity"].apply(lambda x: tuple(set(grouped[x])))
        df = df.drop_duplicates()
        return df


    def fromRoot(self, root, forbidden=[]):
        """fromRoot("rootQID", ["forbidden node", "another forbidden node"]

        Builds a tree from a given root (specify its Wikidata QID) and returns a
        flare.json suitable to be input for d3js' tree layout"""

        self.queryWikidata(self.buildQuery(root=root))
        return self.makeTree(root, [], forbidden)


    def addLabels(self, flare):
        """Get the labels of all items from query result, then call the
        nestedLabeler to replace the names with human-readable labels"""

        self.getLabels(self.nodesInTree)
        return self.nestedLabeler(flare)

    def makeHR(self, x):
        """Gets a tuple of data and converts labels like Q[0-9]+ to human-readable labels"""
        return tuple([self.labels.get(i, i) if self.QIDpattern.match(i) else i for i in x])

    def getPrettyDF(self):
        """Function to take brute dataframe resulting from Wikidata query and
        render a pretty table, with one-entity-per-line, human-readable labels,
        and for each entity, all paths from the root to this entity"""

        # Simplyify the data
        self.cleanedFlatData = [{item:entity[item]["value"].replace(self.prefixURI,"") if isinstance(entity[item]["value"], str) else entity[item]["value"] for item in entity} for entity in self.flatData]

        # Get only entities in the tree
        self.cleanedFlatData = [entity for entity in self.cleanedFlatData if entity["entity"] in self.nodesInTree]

        # for visited nodes, get the labels
        for entity in self.cleanedFlatData:
            entityId = entity["entity"]
            self.visitedNodes[entityId] = [self.makeHR(entry) for entry in self.visitedNodes[entityId]]
            self.visitedNodes[entityId] = tuple(self.visitedNodes[entityId])

        # Convert the list to data frame
        df = pd.DataFrame(self.cleanedFlatData)
        df = df.reindex_axis(sorted(df.columns, key=lambda x: x.lower()), axis=1)
        df = df.fillna("")

        # Convert it to one-entity-per-line
        df = self.resultWikidataAggregateRows(df)

        # Make tuples of entities human-readable
        for column in df:
            c2list = df[column].tolist()
            c2list_expnd = list(set(reduce(lambda x,y: x+y, c2list)))
            c2list_expnd_Q = filter(lambda x: self.QIDpattern.match(x), c2list_expnd)
            self.getLabels(c2list_expnd_Q)
            if column == "entity":
                continue
            tupleORstring = lambda x: x[0] if len(x) == 1 else x
            df[column] = df[column].apply(lambda x: tupleORstring(self.makeHR(x)))

        # add the visited nodes column to the data frame.
        df["visitedNodes"] = df["entity"].apply(lambda x:self.visitedNodes[x])

        return df

    def nestedLabeler(self, node):
        """Function to explore recursively the flare and convert the labels to
        human-readable content"""

        subnodes = node.get("children", [])
        if len(subnodes) == 0:
            return {"name": self.labels.get(node["name"], node["name"]), "nodeId": node["name"]}
        else:
            return {"name": self.labels.get(node["name"], node["name"]),
                    "nodeId": node["name"],
                    "children": [self.nestedLabeler(subnode) for subnode in subnodes]}


    def makeTree(self, node, visited, forbidden):
        """Recursive function to explore the tree"""
        flare = dict()
        flare["name"] = node

        if not node in self.nodesInTree:
            self.nodesInTree.append(node)

        if not node in self.visitedNodes.keys():
            self.visitedNodes[node] = list()
        self.visitedNodes[node].append(tuple(visited))

        result = self.subnodesPerNode.get(node)

        if not result:
            return flare

        # recursively call the function for all subclasses and instances
        flare["children"] = [self.makeTree(entry, visited+[node], forbidden) for entry in result if not entry in visited and not entry in forbidden]

        # Optional : this part puts all subnodes that do not have subnodes into a subnode called "singleEntries". For viz.
        newlyStructured = list()
        singleEntries = list()
        for index, entry in enumerate(flare["children"]):
            if "children" in entry.keys():
                newlyStructured.append(entry)
            else:
                singleEntries.append(entry)

        newlyStructured.append({"name": "singleEntries", "children": singleEntries})
        flare["children"] = newlyStructured

        return flare


def main():
    """Example run"""
    tree = WikidataTreeQuery()
    print(datetime.now())
    flare = tree.fromRoot(root=("Q21198", "Computer Science"), forbidden=[])
    print(datetime.now())
    with open("output.json", "w") as f:
        json.dump(flare, f, indent=4)


if __name__ == "__main__":
    main()
