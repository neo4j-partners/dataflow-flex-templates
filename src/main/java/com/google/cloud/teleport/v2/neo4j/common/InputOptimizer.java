package com.google.cloud.teleport.v2.neo4j.common;

import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.PropertyType;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class InputOptimizer {

    private static final Logger LOG = LoggerFactory.getLogger(InputOptimizer.class);

    public static void refactorJobSpec(JobSpecRequest jobSpec){

        //Create or enrich targets from options
        if (jobSpec.targets.size()==0){
            if (jobSpec.options.size()>0) {
                LOG.info("Targets not found, synthesizing from options");
                throw new RuntimeException("Not currently synthesizing targets from options.");
            } else if (jobSpec.getAllFieldNames().size()==0) {
                LOG.info("Targets not found, synthesizing from source.  All properties will be indexed.");
                throw new RuntimeException("Not currently auto-generating targets.");
            }
        }

        // NODES first then relationships
        // This does not actually change execution order, just numbering
        Collections.sort(jobSpec.targets);

        //number and name targets
        int targetNum=0;
        for (Target target:jobSpec.targets){
            targetNum++;
            target.sequence=targetNum;
            if (StringUtils.isEmpty(target.name)){
                target.name = "Target "+targetNum;
            }
        }

    }
}
