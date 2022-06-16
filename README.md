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

### Flow serialization
- When loading Neo4j, opinionated flow-control is required.
- Nodes must obviously be loaded before edges.  Node loads can be highly parallelized but edge loads should be limited to 2 threads.  
- Also, edges should be loaded in order of target edgeIds to minimize locking.
- There are two methods commonly used to serialize (block/unblock) flow in Beam: Wait.on and creating collection group dependencies.  We use the latter since the former is finicky.
- When the job specification requires creating nodes and edges, nodes are bound together with a flattening function.  This blocks additional processing until nodes are written.
- What is flattened are empty PCollection<Row> records, returned by Neo4jRowWriterTransform.
- To unblock, we combine the empty PCollection<Row> records with PCollections used to load relations/edges, creating a copy + 0 rows.
- This works because PCollection is schema agnostic.  We can cast the unblocked PCollection to whatever we want.
- At the moment, the scheme is leaky because the Neo4jIO writer itself returns POutput which is non-blocking.  
- The current best effort is to apply a zero row collapsing transform to the cast data inside Neo4jRowWriterTransform.
