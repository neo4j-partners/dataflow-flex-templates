# Neo4j Flex Templates

This project contains FlexTemplates that facilitate loading files within the Google BCloud to the Neo4j graph database

## Big Query to Neo4j

[README-BQ](README-BQ.md)

## Text to Neo4j

[README-TXT](README-TXT.md)

## Known issues

### Inserting relationships after nodes
- There are issues with sequential chaining since the Neo4jIO transform does not return PCollection<Void> which is necessary for Wait.on

### Sorting relationships by target node id. 
- This is not implemented in the Text writer since order by operations do not work well in Beam

### Performance
- Performance could be increased by implementing parallel chunking as shown in "SplunkEventWriter.java"


