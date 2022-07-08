package com.google.cloud.teleport.v2.neo4j.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Http client convenience utilities.
 */
public class HttpUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

    public static CloseableHttpResponse getHttpRespoonse(boolean post, String uri, Map<String, String> options, Map<String, String> headers) throws IOException, URISyntaxException {

        CloseableHttpClient httpclient = HttpClients.createDefault();

        if (options.containsKey("url")) {
            options.remove("url");
        }
        List<NameValuePair> paramPairs = getNvPairs(options);
        List<NameValuePair> headerPairs = getNvPairs(headers);

        if (post) {
            HttpPost httpPost = new HttpPost(uri);
            if (paramPairs.size() > 0) {
                httpPost.setEntity(new UrlEncodedFormEntity(paramPairs));
            }
            for (NameValuePair t : headerPairs) {
                httpPost.addHeader(t.getName(), t.getValue());
            }
            return httpclient.execute(httpPost);
        } else {
            URIBuilder builder = new URIBuilder(uri);
            builder.addParameters(paramPairs);
            HttpGet httpGet = new HttpGet(builder.build());
            for (NameValuePair t : headerPairs) {
                httpGet.addHeader(t.getName(), t.getValue());
            }
            return httpclient.execute(httpGet);
        }
    }

    public static String getResponseContent(CloseableHttpResponse httpResponse) throws IOException {
        LOG.info("GET Response Status:: "
                + httpResponse.getStatusLine().getStatusCode());

        BufferedReader reader = new BufferedReader(new InputStreamReader(
                httpResponse.getEntity().getContent()));

        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = reader.readLine()) != null) {
            response.append(inputLine);
        }
        reader.close();
        try {
            httpResponse.close();
        } catch (Exception e) {
            LOG.error("Exception closing connection.");
        }

        return response.toString();
    }


    private static List<NameValuePair> getNvPairs(Map<String, String> options) {
        Iterator optionsIterator = options.keySet().iterator();
        List<NameValuePair> nvps = new ArrayList<NameValuePair>();
        while (optionsIterator.hasNext()) {
            String key = optionsIterator.next() + "";
            String value = options.get(key);
            nvps.add(new BasicNameValuePair(key, value));
        }
        return nvps;
    }

}