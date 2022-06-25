# Neo4j Flex Templates

This project contains FlexTemplates that facilitate loading files within the Google BCloud to the Neo4j graph database

## Building

### Create jar

```sh
# Build and package the application as an uber-jar file.
mvn clean package
```

This will create an all-in-one, shaded jar in project /target directory.

    target/neo4j-flex-templates-1.0.jar

### Building Container Image
* Set environment variables that will be used in the build process.
* Note that /template is the working directory inside the container image
```sh
export JAVA_HOME=`/usr/libexec/java_home -v 1.11`
export PROJECT=neo4jbusinessdev
export GS_WORKING_DIR=gs://neo4j-sandbox/dataflow-working
export APP_NAME=text-to-neo4j
export JOB_NAME=test-txt-to-neo4j-auradb
export REGION=us-central1
export MACHINE_TYPE=n2-highmem-8

export IMAGE_NAME=text-to-neo4j
export BUCKET_NAME=gs://neo4j-sandbox/flex-templates
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_POM_MODULE=neo4j-flex-templates
export APP_ROOT=/template/${APP_NAME}
export COMMAND_SPEC=${APP_ROOT}/resources/${APP_NAME}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${APP_NAME}-image-spec.json

export PARAM_INPUT_FILE_PATTERN=gs://neo4j-datasets/northwinds/nw_orders_1k_noheader.csv \
export PARAM_JOB_SPEC_URI=gs://neo4j-dataflow/job-specs/testing/text/text-northwind-jobspec.json
export PARAM_NEO4J_CONNECTION_URI=gs://neo4j-dataflow/job-specs/testing/common/auradb-free-connection.json
``` 
* Set gcloud CLI project
```sh
gcloud config set project ${PROJECT}
```
* Build and push image to Google Container Repository from current directory
```sh
mvn clean package \
    -Djib.container.mainClass=com.google.cloud.teleport.v2.neo4j.TextToNeo4j \
    -Dimage=${TARGET_GCR_IMAGE} \
    -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
    -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
    -Dapp-root=${APP_ROOT} \
    -Dcommand-spec=${COMMAND_SPEC} \
    -am -pl :${TEMPLATE_POM_MODULE}
```

### Creating Image Spec

Create file in Cloud Storage with path to container image in Google Container Repository.
```sh
echo '{
  "image": "'${TARGET_GCR_IMAGE}'",
  "metadata": {
    "name": "Text to Neo4j",
    "description": "Export Text data into Neo4j",
    "parameters": [
      {
        "name": "jobSpecUri",
        "label": "Target mapping configuration file",
        "helpText": "Source to target mapping json file",
        "paramType": "TEXT",
        "isOptional": false
      }, 
      {
        "name": "neo4jConnectionUri",
        "label": "Neo4j connection metadata",
        "helpText": "Neo4j connection metadata json file",
        "paramType": "TEXT",
        "isOptional": false
      },  
      {
        "name": "inputFilePattern",
        "label": "Text file",
        "helpText": "GS hosted data file",
        "paramType": "TEXT",
        "isOptional": true
      }   
    ]
  },
  "sdk_info": {
    "language": "JAVA"
  }
}' > image_spec.json
gsutil cp image_spec.json ${TEMPLATE_IMAGE_SPEC}
rm image_spec.json
```

### Testing Template

The template unit tests can be run using:
```sh
mvn test
```

### Executing Template

The template requires the following parameters:
* jobSpecUri: GS hosted job specification file
* neo4jConnectionUri: GS hosted Neo4j configuration file
* inputFilePattern: Job spec source override with GS text file
* readQuery: Job spec source override with query

Template can be executed using the following gcloud command:
```sh
export JOB_NAME="${APP_NAME}-`date +%Y%m%d-%H%M%S`"
gcloud dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=${REGION} \
        --worker-machine-type=${MACHINE_TYPE} \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters inputFilePattern="${PARAM_INPUT_FILE_PATTERN}" \
        --parameters jobSpecUri=${PARAM_JOB_SPEC_URI},neo4jConnectionUri=${PARAM_NEO4J_CONNECTION_URI} \
        --max-workers=1
```

## Other resources

    https://cloud.google.com/sdk/gcloud/reference/dataflow/flex-template/build
    https://cloud.google.com/sdk/gcloud/reference/dataflow/flex-template/run

## Testing

### Big Query to Neo4j
[TEST-BQ](TEST-BQ.md)

### Text to Neo4j
[TEST-TXT](TEST-TEXT.md)

## Known issues

### Insert overloading
- The template parallelizes insertion of nodes, limited only to the number of workers in the cluster.  
- For any one label, insertion parallelism will be limited by parameter, but these will accumulate over many nodes.  This is by design currently.

### Sorting relationships by target node id. 
- This is not implemented in the Text writer since order by operations do not work well in Beam SQL

### Flow serialization
- When loading Neo4j, opinionated flow-control is required. DONE.
- Nodes must obviously be loaded before edges.  Node loads can be highly parallelized but edge loads should be limited to 2 threads. DONE.
- Also, edges should be loaded in order of target edgeIds to minimize locking. DONE if sources support SQL pushdown.
- There are two methods commonly used to serialize (block/unblock) flow in Beam: Wait.on and creating collection group dependencies.  We use the latter since the former is finicky.
- When the job specification requires creating nodes and edges, nodes are bound together with a flattening function.  This blocks additional processing until nodes are written.
- What is flattened are empty PCollection<Row> records, returned by Neo4jRowWriterTransform.
- To unblock, we combine the empty PCollection<Row> records with PCollections used to load relations/edges, creating a copy + 0 rows.
- This works because PCollection is schema agnostic. We can cast the unblocked PCollection to whatever we want.
- At the moment, the scheme is leaky because the Neo4jIO writer itself returns POutput which is non-blocking.  
- The current best effort is to apply a zero row collapsing transform to the cast data inside Neo4jRowWriterTransform.

## Data Type casting

Cypher type/Java type
=========================
String/String
Integer/Long
Float/Double
Boolean/Boolean
Point/org.neo4j.graphdb.spatial.Point
Date/java.time.LocalDate
Time/java.time.OffsetTime
LocalTime/java.time.LocalTime
DateTime/java.time.ZonedDateTime
LocalDateTime/java.time.LocalDateTime
Duration/java.time.temporal.TemporalAmount
Node/org.neo4j.graphdb.Node
Relationship/org.neo4j.graphdb.Relationship
Path/org.neo4j.graphdb.Path

## Running Apache Hop
export JAVA_HOME=`/usr/libexec/java_home -v 8`
cd ~/Documents/hop
./hop-gui.sh

## TODO
__ Update CypherGenerator with cypher-dsl
__ Data driven labels in relationships will fail
__ Incomplete multi-label support





