package com.google.cloud.teleport.v2.neo4j.providers;


import com.google.cloud.teleport.v2.neo4j.model.helpers.SourceQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import java.util.List;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Provider interface, implemented for every source.
 */
public interface Provider {
    void configure(OptionsParams optionsParams, JobSpec jobSpecRequest);

    /**
     * Push down capability determine whether groupings and aggregations are executed as SQL queries.
     * When a source does not support push-down, we use SQLTransform in the beam engine.
     * SQL transform is horribly inefficient and does not support ordering.
     */
    boolean supportsSqlPushDown();

    /**
     * Returns validation errors as strings.  This method implements provider specific validations.
     * Core engine validations are shared and executed separately.
     */
    List<String> validateJobSpec();

    /**
     * Queries the source if necessary.  It will be necessary if there are no transforms or the source does not support SQL push-down.
     * For a SQL source with target transformations, this source query will not be made.
     */
    PTransform<PBegin, PCollection<Row>> querySourceBeamRows(SourceQuerySpec sourceQuerySpec);

    /**
     * Queries the source for a particular target.  The TargetQuerySpec includes the source query so that sources that do not support push-down,
     * additional transforms can be done in this transform.
     */
    PTransform<PBegin, PCollection<Row>> queryTargetBeamRows(TargetQuerySpec targetQuerySpec);

    /**
     * Queries the source to extract metadata.  This transform returns zero rows and a valid schema specification.
     */
    PTransform<PBegin, PCollection<Row>> queryMetadata(final Source source);

}
