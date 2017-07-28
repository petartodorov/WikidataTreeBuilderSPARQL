# The WikidataTreeBuilderSPARQL module

The WikidataTreeBuilderSPARQL.py contains a class to :
* Query wikidata for all descendants of a node;
* Structure the result as an arboresence;
* Create a 'flare' dictionary object, suitable for writing as a json file and visualisation with d3js
* Create a table with all descendants of a node, their properties as extracted from Wikidata, and all the paths to go from the root to the given node

### Installation

Go to an installation folder and simply run :

`git clone https://github.com/petartodorov/WikidataTreeBuilderSPARQL`. You are ready to run the jupyter notebooks provided with the projet!

### Example run notebooks. 

The WikidataSampleRun-Software.ipynb gives an example of how to use the class to get the arborescence of a given node (software, which Wikidata ID is Q7397) and a table will all properties we define as relevant (this list of properties is the lookupClaims parameter of the class __init__). The default list of parameters is suitable to extract information from the 'software' node and its arborescence. If you need to explore the arborescence of another node, you have simply to replace the root="Q7397" in the call of the fromRoot function with the desired root. If you want to get a table with relevant properties, you should research the Wikidata documentation to find out which properties are relevant to your problem, to build their list, and to pass it as input parameter on the class init. 

### Parameters. 

During initialization, the following parameters are set up with the following default settings :

#### The endpoint to send the query:

`queryEndpoint = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"`

#### Whether we are in verbose mode:

`debug = false`

#### The labels we want to get from the base. Those three should be of general interest for any root node:

`queryLabels = ["rdfs:label", "skos:altLabel", "schema:description"]`

#### In which languages we want to get those labels :

`labelsLanguages = ["en", "fr"]`

#### A list of the properties of interest, their values will be printed in the output for each entry:
(References for names: https://www.wikidata.org/wiki/Wikidata:List_of_properties/all_in_one_table)

`lookupClaims = ["P571", "P275", "P101", "P135", "P348", "P306", "P1482", "P277", "P577", "P366", "P178", "P31", "P279", "P2572", "P3966", "P144", "P170", "P1324"]`

#### Which properties define set membership. Default are elementOf and subClassOf:

`propertiesSetMembership = ["P31", "P279"]`

#### What is the default language for the metadata of the output file:

`defaultLanguage = "en"`

### Call.

Depending on your goal, you might not need to run all of these commands. 

In order to get a flare.json file, suitable for visualisation with the d3js' tree layout (https://bl.ocks.org/mbostock/4339083), from a given root, you can type :

`> from WikidataTreeBuilderSPARQL import WikidataTreeQuery`

`> tree = WikidataTreeQuery()`

`> flare = tree.fromRoot("Q7397", forbidden=["Q7889", "Q28923"])`

The `forbidden` parameter tells the recursive exploration function of the tree to not explore the given nodes. 

`> with open("flare.json","wb+") as f: json.dump(flare, f, indent=4)`

Now you have the flare.json file !

If you want to convert the labels to human-readable:

`> flare = tree.addLabels(flare)`

`> with open("flareHR.json","wb+") as f: json.dump(flare, f, indent=4)`

And finally, if you want to get the table, with all the descendant nodes, and all the properties you need, in a `pandas` dataframe, you can use:

`> df = tree.getPrettyDF()`

`> df.to_excel("tableComputerScience.xlsx")`

And voilà!
