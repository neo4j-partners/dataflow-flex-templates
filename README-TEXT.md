# Text to Neo4J Dataflow Template

The [TextToNeo4j](src/main/java/com/google/cloud/teleport/v2/neo4j/TextToNeo4j.java) pipeline exports data
from Text using a query into a Neo4j graph database.

NOTE: This template is currently unreleased. If you wish to use it now, you
will need to follow the steps outlined below to add it to and run it from
your own Google Cloud project.

## Getting Started

### Requirements
* Java 11
* Maven
* Text file exists

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
run on Dataflow.

### Creating and running a Flex Template

> <details><summary>
> <i>(Optional)</i> Run the Apache Beam pipeline locally for development.
> <i>(Click to expand)</i>
> </summary>
>
> * Set environment variables that will be used in the build process.
> ```sh
> export JAVA_HOME=`/usr/libexec/java_home -v 11`
> export PROJECT=neo4jbusinessdev
> export GS_WORKING_DIR=gs://dataflow-experiments-gs/dataflow-working
> export APP_NAME=text-to-neo4j
> export JOB_NAME=test-txt-to-neo4j-auradb
> export REGION=us-central1
> export MACHINE_TYPE=n2-highmem-8
> ```
> * Note that to enable_vertical_memory_autoscaling needs Dataflow Prime which requires enabling the "Cloud Autoscaling API"
> * https://cloud.google.com/dataflow/docs/guides/enable-dataflow-prime
    >   --dataflowServiceOptions=enable_prime
    >   --experiments=enable_vertical_memory_autoscaling
> * Additional testing required to determine optimal memory
> ```sh
> mvn compile exec:java \
>   -Dexec.mainClass=com.google.cloud.teleport.v2.neo4j.TextToNeo4j \
>   -Dexec.args="\
>     --runner=DataflowRunner \
>     --project=$PROJECT \
>     --usePublicIps=true \
>     --stagingLocation=$GS_WORKING_DIR/staging/ \
>     --tempLocation=$GS_WORKING_DIR/temp/ \
>     --jobName=$JOB_NAME \
>     --appName=$APP_NAME \
>     --region=$REGION \
>     --workerMachineType=$MACHINE_TYPE \
>     --inputFilePattern=gs://dataflow-experiments-gs/northwind/purchases/nw_orders_1k_noheader.csv \
>     --jobSpecUri=gs://dataflow-experiments-gs/dataflow-job-specs/testing/text/jobSpec.json \
>     --neo4jConnectionUri=gs://dataflow-experiments-gs/dataflow-job-specs/testing/common/neo4jConnection.json"
> ```
> </details>

#### Create jar

```sh
# Build and package the application as an uber-jar file.
mvn clean package
```

This will create an all-in-one, shaded jar in project /target directory.

    target/neo4j-flex-templates-1.0.jar

#### Building Container Image
* Set environment variables that will be used in the build process.
* Note that /template is the working directory inside the container image
```sh
export JAVA_HOME=`/usr/libexec/java_home -v 1.11`
export PROJECT=neo4jbusinessdev
export GS_WORKING_DIR=gs://dataflow-experiments-gs/dataflow-working
export APP_NAME=text-to-neo4j
export JOB_NAME=test-txt-to-neo4j-auradb
export REGION=us-central1
export MACHINE_TYPE=n2-highmem-8

export IMAGE_NAME=text-to-neo4j
export BUCKET_NAME=gs://dataflow-experiments-gs/flex-templates
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_POM_MODULE=neo4j-flex-templates
export APP_ROOT=/template/${APP_NAME}
export COMMAND_SPEC=${APP_ROOT}/resources/${APP_NAME}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${APP_NAME}-image-spec.json

export PARAM_INPUT_FILE_PATTERN=gs://dataflow-experiments-gs/northwind/purchases/nw_orders_1k_noheader.csv \
export PARAM_JOB_SPEC_URI=gs://dataflow-experiments-gs/dataflow-job-specs/testing/text/jobSpec.json
export PARAM_NEO4J_CONNECTION_URI=gs://dataflow-experiments-gs/dataflow-job-specs/testing/common/neo4jConnection.json
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

#### Creating Image Spec

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
* inputFilePattern: Text file from which we load Neo4j

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

