package com.google.cloud.teleport.v2.neo4j.common;


import com.google.cloud.teleport.v2.neo4j.common.model.*;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.SourceType;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Neo4jUtils {
    private static final Logger LOG = LoggerFactory.getLogger(Neo4jUtils.class);

    final public static String DEFAULT_STAR_QUERY = "SELECT * FROM PCOLLECTION";
    final static String LEGAL_CHARS_REGEX = "[^a-zA-Z0-9_]";
    final static String LEGAL_CHARS_REGEX_SPACE="[^a-zA-Z0-9_ ]";
    final static String ALPHA_CHARS_REGEX="[^a-zA-Z]";
    final public static String CYPHER_DELETE_ALL = "MATCH (n) DETACH DELETE n";


    public static Targets generateDefaultTarget(Source source) throws RuntimeException{
        if (source.sourceType== SourceType.text){
            Targets target=new Targets();

            //TODO: create default target

            return target;
        } else {
            LOG.info("Unhandled source type.");
            throw new RuntimeException("Unhandled source type: " + source.sourceType);
        }
    }

    public static String getTargetSql(Schema sourceSchema, Targets target) {
        StringBuffer sb = new StringBuffer();
        if (target.query != null) {
            List<String> fieldList = new ArrayList<>();
            /////////////////////////////////
            // Grouping transform
            if (target.query != null) {
                Query query = target.query;
                if (query.group || query.aggregations.size() > 0) {
                    for (int i = 0; i < target.mappings.size(); i++) {
                        Mapping mapping = target.mappings.get(i);
                        if (StringUtils.isNotBlank(mapping.field)) {
                            if (sourceSchema.hasField(mapping.field)){
                               fieldList.add(mapping.field);
                            }
                        }
                    }
                    sb.append("SELECT "+StringUtils.join(fieldList,","));
                    if (query.aggregations.size() > 0){
                        for (Aggregation agg:query.aggregations){
                            sb.append(","+agg.expression+" "+agg.field);
                        }
                    }
                    sb.append(" FROM PCOLLECTION");
                    if (StringUtils.isNotBlank(query.where)) {
                        sb.append(" WHERE "+query.where);
                    }
                    sb.append(" GROUP BY "+StringUtils.join(fieldList,","));
                    if (query.limit>-1){
                        sb.append(" LIMIT "+query.limit);
                    }

                }
            }
        }
        if (sb.length() == 0) {
            return DEFAULT_STAR_QUERY;
        } else {
            return sb.toString();
        }
    }

    public static String makeValidNeo4jIdentifier(String proposedIdString) {
        String finalIdString = proposedIdString.replaceAll(LEGAL_CHARS_REGEX, "_").trim();
        if (finalIdString.substring(0, 1).matches(ALPHA_CHARS_REGEX)) {
            finalIdString = "N" + finalIdString;
        }
        return finalIdString;
    }

    public static String makeValidNeo4jRelationshipIdentifier(String proposedIdString) {
        String finalRelationshipIdString=proposedIdString.replaceAll(LEGAL_CHARS_REGEX, "_").toUpperCase().trim();
        return finalRelationshipIdString;
    }

    public static String getGcsPathContents(String gsPath) {
        StringBuilder textBuilder = new StringBuilder();

        try {
            ReadableByteChannel chan = FileSystems.open(FileSystems.matchNewResource(
                    gsPath, false /* is_directory */));
            InputStream inputStream = Channels.newInputStream(chan);
            // Use regular Java utilities to work with the input stream.

            try (Reader reader = new BufferedReader(new InputStreamReader
                    (inputStream, Charset.forName(StandardCharsets.UTF_8.name())))) {
                int c = 0;
                while ((c = reader.read()) != -1) {
                    textBuilder.append((char) c);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return textBuilder.toString();
    }
}
