#  Integration Test: Inline Text

## Requirements
* Java 11
* Maven
* Text file exists

## Running Flex Template

Run the Apache Beam pipeline locally for development.

 * Set environment variables that will be used in the build process.
 ```sh
 export JAVA_HOME=`/usr/libexec/java_home -v 11`
 export PROJECT=neo4jbusinessdev
 export GS_WORKING_DIR=gs://neo4j-sandbox/dataflow-working
 export APP_NAME=gcpToNeo4j
 export JOB_NAME=test-inline
 export REGION=us-central1
 export MACHINE_TYPE=n2-highmem-8
 ```
 * Note that to enable_vertical_memory_autoscaling needs Dataflow Prime which requires enabling the "Cloud Autoscaling API"
 * https://cloud.google.com/dataflow/docs/guides/enable-dataflow-prime
   --dataflowServiceOptions=enable_prime 
   --experiments=enable_vertical_memory_autoscaling 
 * Additional testing required to determine optimal memory
 ```sh
pushd ../../../
 mvn compile exec:java \
   -Dexec.mainClass=com.google.cloud.teleport.v2.neo4j.GcpToNeo4j \
   -Dexec.cleanupDaemonThreads=false \
   -Dexec.args="\
     --runner=DataflowRunner \
     --project=$PROJECT \
     --usePublicIps=true \
     --stagingLocation=$GS_WORKING_DIR/staging/ \
     --tempLocation=$GS_WORKING_DIR/temp/ \
     --jobName=$JOB_NAME \
     --appName=$APP_NAME \
     --region=$REGION \
     --workerMachineType=$MACHINE_TYPE \
     --maxNumWorkers=2 \
     --jobSpecUri=gs://neo4j-dataflow/job-specs/testing/bigquery/inline-json-serialize-targets-bq-northwind-jobspec.json \
     --neo4jConnectionUri=gs://neo4j-dataflow/job-specs/testing/common/auradb-free-connection.json"
 ```

