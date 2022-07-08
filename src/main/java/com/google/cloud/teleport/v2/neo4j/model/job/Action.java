package com.google.cloud.teleport.v2.neo4j.model.job;

import com.google.cloud.teleport.v2.neo4j.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.model.enums.ActionType;
import java.io.Serializable;
import java.util.HashMap;

/**
 * Pre and post-load action model object.
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
    // for GET params are converted to query strings
    // for POST params are converted to entity key-value pair JSON
    // headers map passed as key-values
    //
    // actiontype: cypher
    // "cypher" - name of source
    // <other keys are passed in payload or query string as key-value>
    // headers map passed as headers

}
