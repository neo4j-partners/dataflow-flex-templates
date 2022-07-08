package com.google.cloud.teleport.v2.neo4j.common.model.job;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import java.io.Serializable;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job specification request object.
 */
public class JobSpec implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(JobSpec.class);

    //initialize defaults;
    public Map<String, Source> sources = new HashMap<>();
    public List<Target> targets = new ArrayList<>();
    public Config config = new Config();
    public Map<String, String> options = new HashMap<>();
    public List<Action> actions = new ArrayList<>();


    public List<Target> getActiveTargetsBySource(String sourceName) {
        List<Target> targets = new ArrayList<>();
        for (Target target : this.targets) {
            if (target.active && target.source.equals(sourceName)) {
                targets.add(target);
            }
        }
        return targets;
    }

    public List<Target> getActiveNodeTargetsBySource(String sourceName) {
        List<Target> targets = new ArrayList<>();
        for (Target target : this.targets) {
            if (target.active && target.type == TargetType.node && target.source.equals(sourceName)) {
                targets.add(target);
            }
        }
        return targets;
    }

    public List<Target> getActiveRelationshipTargetsBySource(String sourceName) {
        List<Target> targets = new ArrayList<>();
        for (Target target : this.targets) {
            if (target.active && target.type == TargetType.edge && target.source.equals(sourceName)) {
                targets.add(target);
            }
        }
        return targets;
    }

    public Source getSourceByName(String name) {
        return
                sources.get(name);
    }

    public List<Source> getSourceList() {
        ArrayList<Source> sourceList = new ArrayList<>();
        Iterator<String> sourceKeySet = sources.keySet().iterator();
        while (sourceKeySet.hasNext()) {
            sourceList.add(sources.get(sourceKeySet.next()));
        }
        return sourceList;
    }

    public List<String> getAllFieldNames() {
        ArrayList<String> fieldNameList = new ArrayList<>();
        for (Target target : targets) {
            fieldNameList.addAll(target.fieldNames);
        }
        return fieldNameList;
    }

    public List<Action> getPreloadActions() {
        List<Action> actions = new ArrayList<>();
        for (Action action : this.actions) {
            if (action.executeAfter == ActionExecuteAfter.start) {
                actions.add(action);
            }
        }
        return actions;
    }

    public List<Action> getPostloadActions() {
        List<Action> actions = new ArrayList<>();
        for (Action action : this.actions) {
            if (action.executeAfter != ActionExecuteAfter.start) {
                actions.add(action);
            }
        }
        return actions;
    }

}
