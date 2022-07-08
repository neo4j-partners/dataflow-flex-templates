package com.google.cloud.teleport.v2.neo4j.common.model.helpers;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.ActionType;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Action;
import java.util.HashMap;
import java.util.Iterator;
import org.json.JSONArray;
import org.json.JSONObject;

public class ActionMapper {
    public static Action fromJson(final JSONObject actionObj) {
        Action action = new Action();
        action.name = actionObj.getString("name");
        action.type = ActionType.valueOf(actionObj.getString("type"));
        if (actionObj.has("execute_after")) {
            action.executeAfter = ActionExecuteAfter.valueOf(actionObj.getString("execute_after"));
        } else {
            action.executeAfter = ActionExecuteAfter.loads;
        }
        action.executeAfterName = actionObj.has("execute_after_name") ? actionObj.getString("execute_after_name") : "";
        if (actionObj.has("options")) {
            action.options = parseMapObj(actionObj.getJSONArray("options"));
        }
        if (actionObj.has("headers")) {
            action.headers = parseMapObj(actionObj.getJSONArray("headers"));
        }

        return action;
    }

    private static HashMap<String, String> parseMapObj(final JSONArray mapArrayJson) {
        HashMap<String, String> obj = new HashMap<>();
        for (int i = 0; i < mapArrayJson.length(); i++) {
            JSONObject mapObj = mapArrayJson.getJSONObject(i);
            Iterator<String> keys = mapObj.keys();
            while (keys.hasNext()) {
                String key = keys.next();
                obj.put(key, mapObj.opt(key) + "");
            }
        }
        return obj;
    }
}
