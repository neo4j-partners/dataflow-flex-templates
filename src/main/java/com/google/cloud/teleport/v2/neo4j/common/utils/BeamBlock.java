package com.google.cloud.teleport.v2.neo4j.common.utils;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.ArtifactType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility function to queue PCollections, flatten and return empty queue on demand.
 */
public class BeamBlock {
    private static final Logger LOG = LoggerFactory.getLogger(BeamBlock.class);
    protected List<PCollection<Row>> sourceQueue = new ArrayList<>();
    protected List<PCollection<Row>> preloadActionQueue = new ArrayList<>();
    protected List<PCollection<Row>> otherActionQueue = new ArrayList<>();
    protected List<PCollection<Row>> nodeQueue = new ArrayList<>();
    protected List<PCollection<Row>> edgeQueue = new ArrayList<>();
    protected Map<String, PCollection<Row>> namedQueue = new HashMap<>();
    protected Map<String, PCollection<Row>> executionContexts = new HashMap<>();
    PCollection<Row> seedCollection;

    private BeamBlock() {
    }

    public BeamBlock(PCollection<Row> seedCollection) {
        this.seedCollection = seedCollection;
    }

    public void addToQueue(ArtifactType artifactType, boolean preload, String name, PCollection<Row> blockingReturn, PCollection<Row> currentContext) {
        if (artifactType == ArtifactType.action) {
            if (preload) {
                preloadActionQueue.add(blockingReturn);
            } else {
                otherActionQueue.add(blockingReturn);
            }
        } else if (artifactType == ArtifactType.source) {
            sourceQueue.add(blockingReturn);
        } else if (artifactType == ArtifactType.node) {
            nodeQueue.add(blockingReturn);
        } else if (artifactType == ArtifactType.edge) {
            edgeQueue.add(blockingReturn);
        }
        namedQueue.put(artifactType + ":" + name, blockingReturn);
        executionContexts.put(artifactType + ":" + name, currentContext);
    }

    public PCollection<Row> getContextCollection(ArtifactType artifactType, String name) {
        if (executionContexts.containsKey(artifactType + ":" + name)) {
            // execution context has been registered
            return executionContexts.get(artifactType + ":" + name);
        }
        return seedCollection;
    }

    public PCollection<Row> waitOnCollection(ActionExecuteAfter executeAfter, String dependsOn, String queuingDescription) {
        List<PCollection<Row>> allQueues = new ArrayList<>();
        if (executeAfter == ActionExecuteAfter.start) {
            // no dependencies
        } else if (executeAfter == ActionExecuteAfter.preloads) {
            allQueues.addAll(preloadActionQueue);
        } else if (executeAfter == ActionExecuteAfter.sources) {
            allQueues.addAll(sourceQueue);
        } else if (executeAfter == ActionExecuteAfter.nodes) {
            allQueues.addAll(nodeQueue);
            if (allQueues.size() == 0) allQueues.addAll(sourceQueue);
            // end is same as after edges
        } else if (executeAfter == ActionExecuteAfter.edges || executeAfter == ActionExecuteAfter.loads) {
            allQueues.addAll(edgeQueue);
            if (allQueues.size() == 0) allQueues.addAll(nodeQueue);
            if (allQueues.size() == 0) allQueues.addAll(sourceQueue);
        } else if (!StringUtils.isEmpty(dependsOn)) {
            if (executeAfter.toString().equals(ArtifactType.node)) {
                allQueues.add(namedQueue.get(ArtifactType.node + ":" + dependsOn));
            } else if (executeAfter.toString().equals(ArtifactType.edge)) {
                allQueues.add(namedQueue.get(ArtifactType.edge + ":" + dependsOn));
            } else if (executeAfter.toString().equals(ArtifactType.action)) {
                allQueues.add(namedQueue.get(ArtifactType.action + ":" + dependsOn));
            } else if (executeAfter.toString().equals(ArtifactType.source)) {
                allQueues.add(namedQueue.get(ArtifactType.source + ":" + dependsOn));
            }
        }
        if (allQueues.size() == 0) allQueues.add(seedCollection);

        PCollection<Row> combinedQueue = PCollectionList.of(allQueues).apply(" Queueing " + queuingDescription, Flatten.pCollections());
        return combinedQueue;

    }


}