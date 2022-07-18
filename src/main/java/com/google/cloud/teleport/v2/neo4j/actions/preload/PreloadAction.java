package com.google.cloud.teleport.v2.neo4j.actions.preload;

import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import java.util.List;

/**
 * Interface for running preload Actions.
 * Before the pipeline loads, PCollections are not available.
 */
public interface PreloadAction {
    void configure(Action action, ActionContext context);
    List<String> execute();
}
