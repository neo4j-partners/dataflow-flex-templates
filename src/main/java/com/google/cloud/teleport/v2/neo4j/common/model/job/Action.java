package com.google.cloud.teleport.v2.neo4j.common.model.job;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.ActionType;
import java.io.Serializable;
import java.util.HashMap;

/**
 * Action metadata.
 */
public class Action implements Serializable {

    // TODO: add run-log as input to actions.
    // Extent providers to emit structured run-log.

    public String name = "";
    public ActionType type = ActionType.cypher;
    public HashMap<String, String> options = new HashMap<>();
    public HashMap<String, String> headers = new HashMap<>();

    public ActionExecuteAfter executeAfter = ActionExecuteAfter.edges;
    public String executeAfterName = "";

    /*
    Currently supported actions: source, target, action
     */
    public class DependsOn {

    }

    ////////////////////////
    // supported options
    //
    // actiontype: query
    // note: query cannot return values
    // "source" - name of source
    // "sql" - sql to execute on source
    //
    // actiontype: http_get, http_post
    // "url" - name of source
    // <other keys are passed in payload or query string as key-value>
    // headers map passed as headers
    //
    // actiontype: cypher
    // "cypher" - name of source
    // <other keys are passed in payload or query string as key-value>
    // headers map passed as headers

}
