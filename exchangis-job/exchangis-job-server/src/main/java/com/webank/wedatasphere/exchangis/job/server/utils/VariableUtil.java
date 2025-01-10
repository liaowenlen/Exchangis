package com.webank.wedatasphere.exchangis.job.server.utils;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.linkis.cs.client.builder.ContextClientFactory;
import org.apache.linkis.cs.client.http.DefaultContextGetAction;
import org.apache.linkis.cs.client.http.HttpContextClient;
import org.apache.linkis.cs.client.service.CSVariableService;
import org.apache.linkis.httpclient.dws.DWSHttpClient;
import org.apache.linkis.httpclient.response.impl.DefaultHttpResult;
import org.eclipse.jetty.util.ajax.JSON;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author LWL
 * @date 2025/01/08
 */
public class VariableUtil {
    private static volatile DWSHttpClient dwsHttpClient;

    public static List<String> extractVariables(String input) {
        List<String> variables = new ArrayList<>();
        Pattern pattern = Pattern.compile("\\$\\{([^}]*)\\}");
        Matcher matcher = pattern.matcher(input);
        while (matcher.find()) {
            variables.add(matcher.group(1));
        }
        return variables;
    }

    public static Map<String, String> getGlobalVariables() throws NoSuchFieldException, IllegalAccessException {
        DefaultHttpResult result = (DefaultHttpResult) getDWSHttpClient().execute(
                new DefaultContextGetAction(
                        "/api/rest_j/v1/variable/listGlobalVariable"
                )
        );
        Map<String, String> paramMap = new HashMap<>(8);
        if (result.getStatusCode() == 200) {
            Map<String, Object> responseBody = (Map<String, Object>) JSON.parse(result.getResponseBody());
            Map<String, Object> data = (Map<String, Object>) responseBody.get("data");
            if (MapUtils.isNotEmpty(data)) {
                Object[] globalVariables = (Object[]) data.get("globalVariables");
                for (Object globalVariable : globalVariables) {
                    paramMap.put(
                            (String) ((Map<String, Object>) globalVariable).get("key"),
                            (String) ((Map<String, Object>) globalVariable).get("value")
                    );
                }
            }
        }
        return paramMap;
    }

    public static Map<String, String> getFlowVariables(String contextIDValueStr, String nodeNameStr) {
        Map<String, String> paramMap = new HashMap<>(8);
        if (StringUtils.isNotBlank(contextIDValueStr)) {
            CSVariableService
                    .getInstance()
                    .getUpstreamVariables(contextIDValueStr, nodeNameStr)
                    .forEach(linkisVariable -> {
                        if (StringUtils.isNotBlank(linkisVariable.getValue())) {
                            paramMap.put(linkisVariable.getKey(), linkisVariable.getValue());
                        }
                    });
        }
        return paramMap;
    }

    private static DWSHttpClient getDWSHttpClient() throws NoSuchFieldException, IllegalAccessException {
        if (null == dwsHttpClient) {
            synchronized (VariableUtil.class) {
                if (null == dwsHttpClient) {
                    Field field = HttpContextClient.class.getDeclaredField("dwsHttpClient");
                    field.setAccessible(true);
                    try {
                        dwsHttpClient = (DWSHttpClient) field.get(ContextClientFactory.getOrCreateContextClient());
                    } finally {
                        field.setAccessible(false);
                    }
                }
            }
        }
        return dwsHttpClient;
    }
}
