# Neo4j Flex Templates

This project contains FlexTemplates that facilitate loading files within the Google BCloud to the Neo4j graph database

## Local Integration Test Scripts

You will find local integration tests in <i>/src/test/resources/test-scripts</i>

_Sources from BigQuery_<br>
[TEST-BQ](src/test/resources/test-scripts/TEST-BQ.md)<br/>
_Sources from Excel formatted Text file_<br>
[TEST-TEXT](src/test/resources/test-scripts/TEST-TEXT.md)<br/>
_Sources from Excel formatted Text file in the job file specification_<br>
[TEST-TEXT-INLINE](src/test/resources/test-scripts/TEST-TEXT-INLINE.md)<br/>

In each of these files, you will find links to configuration files such as:

    --jobSpecUri=gs://neo4j-dataflow/job-specs/testing/new/bq-northwind-jobspec.json \
    --neo4jConnectionUri=gs://neo4j-dataflow/job-specs/testing/common/auradb-free-connection.json"

The following test _fail_ because functionality is incomplete:

_Audits to parquet_<br>
[TEST-BQ-AUDIT](src/test/resources/test-scripts/failing/TEST-BQ-AUDIT.md)<br/>
_Sources from parquet_<br>
[TEST-BQ-PARQUET](src/test/resources/test-scripts/failing/TEST-BQ-PARQUET.md)<br/>

## Building
### Create jar

```sh
 export JAVA_HOME=`/usr/libexec/java_home -v 11`
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
export APP_NAME=googlecloud-to-neo4j
export REGION=us-central1
export MACHINE_TYPE=n2-highmem-8
export IMAGE_NAME=googlecloud-to-neo4j
export BUCKET_NAME=gs://neo4j-sandbox/flex-templates
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_POM_MODULE=googlecloud-to-neo4j
export APP_ROOT=/template/${APP_NAME}
export COMMAND_SPEC=${APP_ROOT}/resources/${APP_NAME}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${APP_NAME}-image-spec.json
``` 
* Set gcloud CLI project
```sh
gcloud config set project ${PROJECT}
```
* Build and push image to Google Container Repository from current directory
```sh
mvn clean package \
    -Djib.container.mainClass=com.google.cloud.teleport.v2.neo4j.GcpToNeo4j \
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
    "name": "Google Cloud to Neo4j",
    "description": "BigQuery, Text, and other source import into Neo4j",
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
        "helpText": "Override text file pattern (optional)",
        "paramType": "TEXT",
        "isOptional": true
      } ,  
      {
        "name": "readQuery",
        "label": "Query SQL",
        "helpText": "Override SQL query (optional)",
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

### Executing Template Example

The template requires the following parameters:
* jobSpecUri: GS hosted job specification file
* neo4jConnectionUri: GS hosted Neo4j configuration file
* inputFilePattern: Job spec source override with GS text file
* readQuery: Job spec source override with query

Template can be executed using the following gcloud command:

```sh
export PARAM_INPUT_FILE_PATTERN=gs://neo4j-datasets/northwinds/nw_orders_1k_noheader.csv \
export PARAM_JOB_SPEC_URI=gs://neo4j-dataflow/job-specs/testing/new/text-northwind-jobspec.json
export PARAM_NEO4J_CONNECTION_URI=gs://neo4j-dataflow/job-specs/testing/common/auradb-free-connection.json
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
- When the job specification requires creating nodes and edges, nodes are bound together with a flattening function.  This blocks additional processing until nodes are written.

## Running Apache Hop
export JAVA_HOME=`/usr/libexec/java_home -v 8`
cd ~/Documents/hop
./hop-gui.sh

### Testing Template

The template unit tests can be run using:
```sh
mvn test
```

## TODO
__ Unit tests
__ Update CypherGenerator with cypher-dsl
__ Data driven labels in relationships will fail
__ Incomplete multi-label support
__ Support Parquet as text source
__ Support Parquet as audit





