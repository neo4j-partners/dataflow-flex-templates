package com.google.cloud.teleport.v2.neo4j.common.model.job;


import com.google.cloud.teleport.v2.neo4j.common.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.SaveMode;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Target (node/edge) metadata.
 */
public class Target implements Serializable, Comparable {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final Logger LOG = LoggerFactory.getLogger(Target.class);
    public String source = "";
    public String name = "";
    public boolean active = true;
    public TargetType type;
    public boolean autoMap = false;
    public Transform transform = new Transform();
    public List<Mapping> mappings = new ArrayList<>();
    public SaveMode saveMode = SaveMode.append;
    public Map<String, Mapping> mappingByFieldMap = new HashMap();
    public List<String> fieldNames = new ArrayList<>();
    public int sequence = 0;
    public ActionExecuteAfter executeAfter=null;
    public String executeAfterName="";

    public Target() {
    }

    public Mapping getMappingByFieldName(String fieldName) {
        return this.mappingByFieldMap.get(fieldName);
    }

    @Override
    public int compareTo(Object o) {
        if (this.type == ((Target) o).type) {
            return 0;
        } else if (this.type == TargetType.edge && ((Target) o).type == TargetType.node) {
            return 1;
        } else {
            return -1;
        }
    }

}
