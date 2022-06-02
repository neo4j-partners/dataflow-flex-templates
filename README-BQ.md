# BigQuery to Bigtable Dataflow Template

The [BigQueryToBigtable](src/main/java/com/google/cloud/teleport/v2/neo4j/BigQueryToNeo4j.java) pipeline exports data
from BigQuery using a query into a Cloud Bigtable table.

NOTE: This template is currently unreleased. If you wish to use it now, you
will need to follow the steps outlined below to add it to and run it from
your own Google Cloud project.

## Getting Started

### Requirements
* Java 8
* Maven
* BigQuery table exists

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
run on Dataflow.

### Creating and running a Flex Template

* Set environment variables that will be used in the build process.
```sh
export PROJECT=neo4jbusinessdev
export GS_WORKING_DIR=gs://dataflow-experiments-gs/dataflow-working
export APP_NAME=bigquery-to-neo4j
export JOB_NAME=test-bq-to-neo4j-auradb
export REGION=us-central1
```

> <details><summary>
> <i>(Optional)</i> Run the Apache Beam pipeline locally for development.
> <i>(Click to expand)</i>
> </summary>
>
> ```sh
> # If you omit the --bootstrapServer argument, it connects to localhost.
> # If you are running the Kafka server locally, you can omit --bootstrapServer.
> mvn compile exec:java \
>   -Dexec.mainClass=com.google.cloud.teleport.v2.neo4j.BigQueryToNeo4j \
>   -Dexec.args="\
>     --runner=DataflowRunner \
>     --project=$PROJECT \
>     --stagingLocation=$GS_WORKING_DIR/staging/ \
>     --tempLocation=$GS_WORKING_DIR/temp/ \
>     --jobName=$JOB_NAME \
>     --appName=$APP_NAME \
>     --region=$REGION \
>     --readQuery=\"SELECT customer_id,contact_name,company_name,seller_id,seller_first_name, \
>     seller_last_name,seller_title,product_id,product_name,category_name,supplier_name, \
>     supplier_postal_code, supplier_country,order_id,order_date, shipped_date,required_date, \
>     quantity,unit_price,discount FROM northwind.V_CUSTOMER_ORDERS LIMIT 10\" \
>     --jobSpecUri=gs://dataflow-experiments-gs/dataflow-job-specs/testing/bq/jobSpec.json \
>     --neo4jConnectionUri=gs://dataflow-experiments-gs/dataflow-job-specs/testing/common/neo4jConnection.json"
> ```
>
> </details>

### Notes on parameters
There are challenges when using the project name in the select query.  Use:

    SELECT * FROM <schema>.<table> 

rather than

    SELECT * FROM <project>.<schema>.<table>

First, let's build the container image.

```sh
# Build and package the application as an uber-jar file.
mvn clean package
```

#### Building Container Image
* Set environment variables that will be used in the build process.
```sh

export PROJECT=neo4jbusinessdev
export GS_WORKING_DIR=gs://dataflow-experiments-gs/dataflow-working
export APP_NAME=bigquery-to-neo4j
export JOB_NAME=test-bq-to-neo4j-auradb
export REGION=us-central1

export IMAGE_NAME=bigquery-to-neo4j:latest
export BUCKET_NAME=gs://dataflow-experiments-gs/flex-templates
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=bigquery-to-neo4j
export APP_ROOT=/template/${TEMPLATE_MODULE}
export COMMAND_SPEC=${APP_ROOT}/resources/${TEMPLATE_MODULE}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${TEMPLATE_MODULE}-image-spec.json

export READ_QUERY="select * from northwind.V_CUSTOMER_ORDERS"

gcloud config set project ${PROJECT}
```
* Build and push image to Google Container Repository from the v2 directory
```sh
mvn clean package \
    -Dimage=${TARGET_GCR_IMAGE} \
    -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
    -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
    -Dapp-root=${APP_ROOT} \
    -Dcommand-spec=${COMMAND_SPEC} \
    -am -pl ${TEMPLATE_MODULE}
```

#### Creating Image Spec

Create file in Cloud Storage with path to container image in Google Container Repository.
```sh
echo '{
  "image": "'${TARGET_GCR_IMAGE}'",
  "metadata": {
    "name": "BigQuery to Neo4j",
    "description": "Export BigQuery data into Neo4j",
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
        "name": "readQuery",
        "label": "BigQuery query",
        "helpText": "BigQuery query to export data from.  Include fully qualified table name.",
        "paramType": "TEXT",
        "isOptional": false
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
* readQuery: BigQuery query to export data from
* readIdColumn: BigQuery query unique column ID
* bigtableWriteProjectId: Bigtable project id to write to
* bigtableWriteInstanceId: Bigtable instance id to write to
* bigtableWriteTableId: Bigtable table id to write to
* bigtableWriteColumnFamily: Bigtable table column family id to write to

The template has the following optional parameters:
* bigtableWriteAppProfile: Bigtable app profile to use for the export

Template can be executed using the following gcloud command:
```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=${REGION} \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters ^~^readQuery="${READ_QUERY}" \
        --parameters readIdColumn=${READ_ID_COLUMN},bigtableWriteProjectId=${BIGTABLE_WRITE_PROJECT_ID},bigtableWriteInstanceId=${BIGTABLE_WRITE_INSTANCE_ID},bigtableWriteTableId=${BIGTABLE_WRITE_TABLE_ID},bigtableWriteColumnFamily=${BIGTABLE_WRITE_COLUMN_FAMILY}
```

Note: The `^~^` prefix on readQuery is used to make `~` a delimiter instead of
commas. This allows commas to be used in the query. Read more about [gcloud topic escaping](https://cloud.google.com/sdk/gcloud/reference/topic/escaping).

#### Example query

Here is an example query using a public dataset. It combines a few values into a rowkey with a `#` between each value.

```
export READ_QUERY="SELECT CONCAT(SenderCompID,'#', OrderID) as rowkey, * FROM bigquery-public-data.cymbal_investments.trade_capture_report LIMIT 100"
```
