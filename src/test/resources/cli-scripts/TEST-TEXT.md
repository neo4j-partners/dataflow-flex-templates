#  Integration Test: Text

## Requirements
* Java 11
* Maven
* Text file exists

## Running Flex Template

Run the Apache Beam pipeline locally for development.

 * Set environment variables that will be used in the build process.
 ```sh
export TEMPLATE_GCS_LOCATION="gs://neo4j-dataflow/flex-templates/images/gcp-to-neo4j-image-spec.json"
export REGION=us-central1
 
gcloud dataflow flex-template run "test-text-cli-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location="$TEMPLATE_GCS_LOCATION" \
    --region "$REGION" \
    --parameters jobSpecUri="gs://neo4j-dataflow/job-specs/testing/new/text-northwind-jobspec.json" \
    --parameters neo4jConnectionUri="gs://neo4j-dataflow/job-specs/testing/common/auradb-free-connection.json"
 ```
