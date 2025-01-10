package com.webank.wedatasphere.exchangis.job.server.builder.transform.handlers;

import com.webank.wedatasphere.exchangis.datasource.core.utils.Json;
import com.webank.wedatasphere.exchangis.job.builder.ExchangisJobBuilderContext;
import com.webank.wedatasphere.exchangis.job.domain.SubExchangisJob;
import com.webank.wedatasphere.exchangis.job.domain.params.JobParamDefine;
import com.webank.wedatasphere.exchangis.job.domain.params.JobParamSet;
import com.webank.wedatasphere.exchangis.job.domain.params.JobParams;
import com.webank.wedatasphere.exchangis.job.server.builder.JobParamConstraints;
import com.webank.wedatasphere.exchangis.job.server.utils.SQLCommandUtils;
import org.apache.linkis.common.exception.ErrorException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author LWL
 * @date 2024/07/26
 */
public class KingbaseDataxSubExchangisJobHandler extends AuthEnabledSubExchangisJobHandler {

    /**
     * Database
     */
    private static final JobParamDefine<String> SOURCE_DATABASE = JobParams.define("connection[0].jdbcUrl[0].database", JobParamConstraints.DATABASE);
    private static final JobParamDefine<String> SOURCE_INSTANCE = JobParams.define("connection[0].jdbcUrl[0].instance", JobParamConstraints.SERVICE_NAME);
    private static final JobParamDefine<String> SINK_DATABASE = JobParams.define("connection[0].jdbcUrl.database", JobParamConstraints.DATABASE);
    private static final JobParamDefine<String> SINK_INSTANCE = JobParams.define("connection[0].jdbcUrl.instance", JobParamConstraints.SERVICE_NAME);

    /**
     * Table
     */
    private static final JobParamDefine<String> SINK_TABLE = JobParams.define("connection[0].table[0]", JobParamConstraints.TABLE);

    /**
     * Host
     */
    private static final JobParamDefine<String> SOURCE_HOST = JobParams.define("connection[0].jdbcUrl[0].host", JobParamConstraints.HOST);
    private static final JobParamDefine<String> SINK_HOST = JobParams.define("connection[0].jdbcUrl.host", JobParamConstraints.HOST);

    /**
     * Port
     */
    private static final JobParamDefine<String> SOURCE_PORT = JobParams.define("connection[0].jdbcUrl[0].port", JobParamConstraints.PORT);
    private static final JobParamDefine<String> SINK_PORT = JobParams.define("connection[0].jdbcUrl.port", JobParamConstraints.PORT);

    /**
     * Connect params
     */
    private static final JobParamDefine<Map<String, String>> SOURCE_PARAMS_MAP = JobParams.define("connection[0].jdbcUrl[0].connParams", JobParamConstraints.CONNECT_PARAMS,
            connectParams -> Json.fromJson(connectParams, Map.class), String.class);
    private static final JobParamDefine<Map<String, String>> SINK_PARAMS_MAP = JobParams.define("connection[0].jdbcUrl.connParams", JobParamConstraints.CONNECT_PARAMS,
            connectParams -> Json.fromJson(connectParams, Map.class), String.class);
    /**
     * Where condition
     */
    private static final JobParamDefine<String> WHERE_CONDITION = JobParams.define(JobParamConstraints.WHERE);

    private static final JobParamDefine<String> QUERY_SQL_CUSTOM = JobParams.define("connection[0].querySql[0]", JobParamConstraints.QUERY_SQL);

    /**
     * Query sql
     */
    private static final JobParamDefine<String> QUERY_SQL = JobParams.define("connection[0].querySql[0]", job ->{
        JobParamSet sourceParams = job.getRealmParams(SubExchangisJob.REALM_JOB_CONTENT_SOURCE);
        String where = WHERE_CONDITION.getValue(sourceParams);
        List<String> columns = job.getSourceColumns().stream().map(SubExchangisJob.ColumnDefine::getName).collect(Collectors.toList());
        if (columns.isEmpty()){
            columns.add("*");
        }
        return SQLCommandUtils.contactSql(Collections.singletonList(sourceParams
                .get(JobParamConstraints.TABLE).getValue()), null, columns, null, where);
    }, SubExchangisJob.class);

    /**
     * SQL column
     */
    private static final JobParamDefine<List<String>> SQL_COLUMN = JobParams.define("column", job -> {
        List<String> columns = job.getSinkColumns().stream().map(SubExchangisJob.ColumnDefine::getName).collect(Collectors.toList());
        if (columns.isEmpty()){
            columns.add("*");
        }
        return columns;
    }, SubExchangisJob.class);
    @Override
    public void handleJobSource(SubExchangisJob subExchangisJob, ExchangisJobBuilderContext ctx) throws ErrorException {
        JobParamSet paramSet = subExchangisJob.getRealmParams(SubExchangisJob.REALM_JOB_CONTENT_SOURCE);
        if (Objects.nonNull(paramSet)){
            Arrays.asList(sourceMappings()).forEach(define -> paramSet.addNonNull(define.get(paramSet)));
            if (Objects.nonNull(paramSet.get(QUERY_SQL.getKey()))) {
                paramSet.add(QUERY_SQL.newParam(subExchangisJob));
            }
        }
    }

    @Override
    public void handleJobSink(SubExchangisJob subExchangisJob, ExchangisJobBuilderContext ctx) throws ErrorException {
        JobParamSet paramSet = subExchangisJob.getRealmParams(SubExchangisJob.REALM_JOB_CONTENT_SINK);
        if (Objects.nonNull(paramSet)){
            Arrays.asList(sinkMappings()).forEach(define -> paramSet.addNonNull(define.get(paramSet)));
            paramSet.add(SQL_COLUMN.newParam(subExchangisJob));
        }
    }

    @Override
    public String dataSourceType() {
        return "kingbase";
    }

    @Override
    public boolean acceptEngine(String engineType) {
        return "datax".equalsIgnoreCase(engineType);
    }

    private JobParamDefine<?>[] sourceMappings(){
        return new JobParamDefine[]{USERNAME, PASSWORD, SOURCE_INSTANCE, SOURCE_DATABASE,
                SOURCE_HOST, SOURCE_PORT, SOURCE_PARAMS_MAP, QUERY_SQL_CUSTOM};
    }

    public JobParamDefine<?>[] sinkMappings(){
        return new JobParamDefine[]{USERNAME, PASSWORD, SINK_INSTANCE, SINK_DATABASE, SINK_TABLE,
                SINK_HOST, SINK_PORT, SINK_PARAMS_MAP};
    }
}
