package com.google.cloud.teleport.v2.neo4j.common;

import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JobSpecOptimizer {

    public static Validations validateAndOptimize(JobSpecRequest jobSpec){
        boolean errors=false;
        List<String> validationMessages=new ArrayList<>();
        JobSpecRequest optimizedJobSpec = jobSpec;

        //OPTIMIZATION
        //TODO: optimization
        Collections.sort(jobSpec.targets);
        //sort relationships over

        //VALIDATION
        //TODO: validation

        Validations validationResponse=new Validations();
        validationResponse.errors=errors;
        validationResponse.validationMessages=validationMessages;
        return validationResponse;
    }


}
