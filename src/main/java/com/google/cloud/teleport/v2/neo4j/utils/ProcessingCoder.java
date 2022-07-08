package com.google.cloud.teleport.v2.neo4j.utils;

import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;

/**
 * Utils for streamlining creation of processing records.
 */
public class ProcessingCoder {
   public static Schema getProcessingSchema() {
       return Schema.of(
               Schema.Field.of("JOB", Schema.FieldType.STRING),
               Schema.Field.of("TS", Schema.FieldType.DATETIME),
               Schema.Field.of("DESCRIPTION", Schema.FieldType.STRING),
               Schema.Field.of("TYPE", Schema.FieldType.STRING),
               Schema.Field.of("SUBTYPE", Schema.FieldType.STRING),
               Schema.Field.of("AMOUNT", Schema.FieldType.DOUBLE)
       );
   }
   public static RowCoder getProcessingRowCoder(){
       return  RowCoder.of(getProcessingSchema());
   }
}
