package com.google.cloud.teleport.v2.neo4j.common.utils;

import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
BeamBlock blockingQueue = BeamBlock.create("Serial");
blockingQueue.addEmptyBlockingCollection(emptyReturn);

 */

public class BeamBlock {

    //registry of blocks
    private static Map<String, BeamBlock> blockRegistry = new HashMap<>();

    protected String name;
    protected int sequence;
    protected List<PCollection<Row>> blockingQueue = new ArrayList<>();
    protected List<List<PCollection<Row>>> chainedQueues = new ArrayList<>();

    private BeamBlock() {
    }

    public static BeamBlock create(String name) {
        BeamBlock block = new BeamBlock();
        block.name = name;
        block.addToBlockRegistry();
        return block;
    }

    public static BeamBlock createChained(String name, BeamBlock existingBlock) {
        BeamBlock chained = new BeamBlock();
        chained.chainedQueues.add(existingBlock.blockingQueue);
        chained.name = name;
        chained.addToBlockRegistry();
        return chained;
    }

    public static BeamBlock getByName(String name) {
        if (blockRegistry.get(name) == null) {
            return create(name);
        }
        return blockRegistry.get(name);
    }

    public void addEmptyBlockingCollection(PCollection<Row> collection) {
        blockingQueue.add(collection);
    }

    public PCollection<Row> waitOnCollection(String description) {
        List<PCollection<Row>> allQueues = new ArrayList<>();
        allQueues.addAll(blockingQueue);
        for (List<PCollection<Row>> chainedQueue : chainedQueues) {
            allQueues.addAll(chainedQueue);
        }
        PCollection<Row> combinedQueue = PCollectionList.of(allQueues).apply(description + " Queueing", Flatten.pCollections());
        return combinedQueue;
    }

    private void addToBlockRegistry() {
        BeamBlock beamBlock = blockRegistry.get(name);
        if (beamBlock == null) {
            blockRegistry.put(name, beamBlock);
        }
    }

}