package com.webank.wedatasphere.exchangis.job.server.builder.transform;

import com.baomidou.mybatisplus.core.toolkit.CollectionUtils;
import com.google.common.collect.HashBasedTable;
import com.webank.wedatasphere.exchangis.common.linkis.bml.BmlResource;
import com.webank.wedatasphere.exchangis.datasource.core.domain.DataSourceType;
import com.webank.wedatasphere.exchangis.datasource.core.utils.Json;
import com.webank.wedatasphere.exchangis.datasource.core.vo.ExchangisJobInfoContent;
import com.webank.wedatasphere.exchangis.datasource.core.vo.ExchangisJobParamsContent;
import com.webank.wedatasphere.exchangis.job.builder.ExchangisJobBuilderContext;
import com.webank.wedatasphere.exchangis.job.domain.ExchangisJobInfo;
import com.webank.wedatasphere.exchangis.job.domain.SubExchangisJob;
import com.webank.wedatasphere.exchangis.job.domain.params.JobParams;
import com.webank.wedatasphere.exchangis.job.exception.ExchangisJobException;
import com.webank.wedatasphere.exchangis.job.exception.ExchangisJobExceptionCode;
import com.webank.wedatasphere.exchangis.job.server.builder.AbstractLoggingExchangisJobBuilder;
import com.webank.wedatasphere.exchangis.job.server.builder.JobParamConstraints;
import com.webank.wedatasphere.exchangis.job.server.builder.transform.handlers.SubExchangisJobHandler;
import com.webank.wedatasphere.exchangis.job.server.mapper.JobTransformProcessorDao;
import com.webank.wedatasphere.exchangis.job.server.render.transform.TransformTypes;
import com.webank.wedatasphere.exchangis.job.server.render.transform.processor.TransformProcessor;
import com.webank.wedatasphere.exchangis.job.server.utils.SpringContextHolder;
import com.webank.wedatasphere.exchangis.job.server.utils.VariableUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.linkis.common.exception.ErrorException;
import org.apache.linkis.common.io.Fs;
import org.apache.linkis.common.io.FsPath;
import org.apache.linkis.common.io.MetaData;
import org.apache.linkis.common.io.Record;
import org.apache.linkis.common.io.resultset.ResultSet;
import org.apache.linkis.common.io.resultset.ResultSetReader;
import org.apache.linkis.common.utils.ClassUtils;
import org.apache.linkis.cs.client.service.CSTableService;
import org.apache.linkis.cs.common.entity.metadata.CSColumn;
import org.apache.linkis.cs.common.entity.metadata.CSTable;
import org.apache.linkis.cs.common.entity.source.ContextKeyValue;
import org.apache.linkis.storage.FSFactory;
import org.apache.linkis.storage.resultset.ResultSetFactory;
import org.apache.linkis.storage.resultset.ResultSetReaderFactory;
import org.apache.linkis.storage.resultset.table.TableRecord;
import org.apache.linkis.storage.utils.StorageUtils;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * TransformJob builder
 */
public class GenericExchangisTransformJobBuilder extends AbstractLoggingExchangisJobBuilder<ExchangisJobInfo, TransformExchangisJob> {

    private static final Logger LOG = LoggerFactory.getLogger(GenericExchangisTransformJobBuilder.class);

    /**
     * Handlers
     */
    private static final Map<String, SubExchangisJobHandlerChain> handlerHolders = new ConcurrentHashMap<>();

    /**
     * Transform dao
     */
    private JobTransformProcessorDao transformProcessorDao;

    public synchronized void initHandlers() {
        //Should define wds.linkis.reflect.scan.package in properties
        Set<Class<? extends SubExchangisJobHandler>> jobHandlerSet = ClassUtils.reflections().getSubTypesOf(SubExchangisJobHandler.class);
        List<SubExchangisJobHandler> reflectedHandlers = jobHandlerSet.stream().map(handlerClass -> {
            if (!Modifier.isAbstract(handlerClass.getModifiers()) &&
                    !Modifier.isInterface(handlerClass.getModifiers()) && !handlerClass.equals(SubExchangisJobHandler.class)) {
                try {
                    return handlerClass.newInstance();
                } catch (InstantiationException | IllegalAccessException e) {
                    LOG.warn("Cannot create the instance of handler: [{}], message: [{}]", handlerClass.getCanonicalName(), e.getMessage(), e);
                }
            }
            return null;
        }).filter(handler -> Objects.nonNull(handler) && Objects.nonNull(handler.dataSourceType())).collect(Collectors.toList());
        reflectedHandlers.forEach(reflectedHandler -> handlerHolders.compute(reflectedHandler.dataSourceType(), (type, handlerChain) -> {
           if (Objects.isNull(handlerChain)){
               handlerChain = new SubExchangisJobHandlerChain(type);
           }
           handlerChain.addHandler(reflectedHandler);
           return handlerChain;
        }));
        LOG.trace("Sort the handler chain");
        handlerHolders.values().forEach(SubExchangisJobHandlerChain::sort);
        LOG.trace("Add the default handlerChain to the head");
        //Add the default handlerChain to the head
        Optional.ofNullable(handlerHolders.get(SubExchangisJobHandler.DEFAULT_DATA_SOURCE_TYPE)).ifPresent(defaultHandlerChain ->
                handlerHolders.forEach( (s, handlerChain) -> {if(!Objects.equals(handlerChain, defaultHandlerChain)){
                    handlerChain.addFirstHandler(defaultHandlerChain);
        }}));
    }
    @Override
    public TransformExchangisJob buildJob(ExchangisJobInfo inputJob, TransformExchangisJob expectOut, ExchangisJobBuilderContext ctx) throws ExchangisJobException {
        LOG.trace("Start to build exchangis transform job, name: [{}], id: [{}], engine: [{}], content: [{}]",
                inputJob.getName(), inputJob.getId(), inputJob.getEngineType(), inputJob.getJobContent());
        //First to convert content to "ExchangisJobInfoContent"
        TransformExchangisJob outputJob = new TransformExchangisJob();
        outputJob.setCreateUser(Optional.ofNullable(inputJob.getExecuteUser()).orElse(String.valueOf(ctx.getEnv("USER_NAME"))));
        try {
            if (StringUtils.isNotBlank(inputJob.getJobContent())) {
                //First to convert content to "ExchangisJobInfoContent"
                List<ExchangisJobInfoContent> contents = Json.fromJson(inputJob.getJobContent(), List.class, ExchangisJobInfoContent.class);
                if (Objects.nonNull(contents) ) {
                    LOG.info("To parse content ExchangisJob: id: [{}], name: [{}], expect subJobs: [{}]",
                            inputJob.getId(), inputJob.getName(), contents.size());
                    //Second to new SubExchangisJob instances
                    List<SubExchangisJob> subExchangisJobs = contents.stream().map(job -> {
                                // 解析任务变量
                                {
                                    // read variable
                                    Map<String, String> paramMap = new HashMap<>(8);
                                    try {
                                        // globalVariable
                                        paramMap.putAll(VariableUtil.getGlobalVariables());

                                        // flowVariable
                                        if (StringUtils.isNotBlank(job.getContextID())) {
                                            paramMap.putAll(VariableUtil.getFlowVariables(job.getContextID(), inputJob.getName()));
                                        }

                                        // nodeVariable
                                        String jobParams = inputJob.getJobParams();
                                        if (StringUtils.isNotBlank(jobParams)) {
                                            paramMap.putAll((Map<String, String>) JSON.parse(jobParams));
                                        }
                                    } catch (IllegalAccessException | NoSuchFieldException e) {
                                        throw new RuntimeException(e);
                                    }

                                    // replace variable
                                    if (MapUtils.isNotEmpty(paramMap)) {
                                        ExchangisJobParamsContent params = job.getParams();
                                        List<ExchangisJobParamsContent.ExchangisJobParamsItem> sources = params.getSources();
                                        List<ExchangisJobParamsContent.ExchangisJobParamsItem> sinks = params.getSinks();
                                        if (CollectionUtils.isNotEmpty(sources)) {
                                            for (ExchangisJobParamsContent.ExchangisJobParamsItem source : sources) {
                                                String configValue = String.valueOf(source.getConfigValue());
                                                if (StringUtils.isNotEmpty(configValue)) {
                                                    for (String key : paramMap.keySet()) {
                                                        if (configValue.contains("${" + key + "}")) {
                                                            source.setConfigValue(StringUtils.replace(configValue, "${" + key + "}", paramMap.get(key)));
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        if (CollectionUtils.isNotEmpty(sinks)) {
                                            for (ExchangisJobParamsContent.ExchangisJobParamsItem sink : sinks) {
                                                String configValue = String.valueOf(sink.getConfigValue());
                                                if (StringUtils.isNotEmpty(configValue)) {
                                                    for (String key : paramMap.keySet()) {
                                                        if (configValue.contains("${" + key + "}")) {
                                                            sink.setConfigValue(StringUtils.replace(configValue, "${" + key + "}", paramMap.get(key)));
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    // replaceCsTableVariables
                                    replaceCsTableVariables(job, inputJob.getName());
                                }
                                TransformExchangisJob.TransformSubExchangisJob transformSubJob = new TransformExchangisJob.TransformSubExchangisJob(job);
                                // append querySql
                                {
                                    List<ExchangisJobParamsContent.ExchangisJobParamsItem> sources = job
                                            .getParams()
                                            .getSources()
                                            .stream()
                                            .filter(exchangisJobParamsItem -> exchangisJobParamsItem.getConfigKey().equals(JobParamConstraints.QUERY_SQL))
                                            .collect(Collectors.toList());
                                    if (CollectionUtils.isNotEmpty(sources)) {
                                        ExchangisJobParamsContent.ExchangisJobParamsItem source = sources.get(0);
                                        String configValue = String.valueOf(source.getConfigValue());
                                        if (StringUtils.isNotEmpty(configValue)) {
                                            transformSubJob
                                                    .getRealmParams(SubExchangisJob.REALM_JOB_CONTENT_SOURCE)
                                                    .add(JobParams.newOne(JobParamConstraints.QUERY_SQL, configValue));
                                        }
                                    }
                                }
                                transformSubJob.setId(inputJob.getId());
                                transformSubJob.setCreateUser(outputJob.getCreateUser());
                                setTransformCodeResource(transformSubJob);
                                return transformSubJob;
                            })
                            .collect(Collectors.toList());
                    outputJob.setSubJobSet(subExchangisJobs);
                    outputJob.setId(inputJob.getId());
                    outputJob.setName(inputJob.getName());
                    LOG.info("Invoke job handlers to handle the subJobs, ExchangisJob: id: [{}], name: [{}]", inputJob.getId(), inputJob.getName());
                    //Do handle of the sub jobs
                    for (SubExchangisJob subExchangisJob : subExchangisJobs){
                        if(StringUtils.isBlank(subExchangisJob.getEngineType())){
                            subExchangisJob.setEngineType(inputJob.getEngineType());
                        }
                        String sourceType = subExchangisJob.getSourceType();
                        if (StringUtils.isNotBlank(sourceType)) {
                            if (DataSourceType.DORIS.name().equals(sourceType)) {
                                sourceType = DataSourceType.MYSQL.name;
                            }
                            sourceType = sourceType.toLowerCase();
                        } else {
                            sourceType = "";
                        }
                        SubExchangisJobHandler sourceHandler = handlerHolders.get(sourceType);
                        if(Objects.isNull(sourceHandler)){
                            LOG.warn("Not find source handler for subJob named: [{}], sourceType: [{}], " +
                                            "ExchangisJob: id: [{}], name: [{}], use default instead",
                                    subExchangisJob.getName(), subExchangisJob.getSourceType(), inputJob.getId(), inputJob.getName());
                            sourceHandler = handlerHolders.get(SubExchangisJobHandler.DEFAULT_DATA_SOURCE_TYPE);
                        }
                        String sinkType = subExchangisJob.getSinkType();
                        if (StringUtils.isNotBlank(sinkType)) {
                            if (DataSourceType.DORIS.name().equals(sinkType)) {
                                sinkType = DataSourceType.MYSQL.name;
                            }
                            sinkType = sinkType.toLowerCase();
                        } else {
                            sinkType = "";
                        }
                        SubExchangisJobHandler sinkHandler = handlerHolders.get(sinkType);
                        if(Objects.isNull(sinkHandler)){
                            LOG.warn("Not find sink handler for subJob named: [{}], sinkType: [{}], ExchangisJob: id: [{}], name: [{}], use default instead",
                                    subExchangisJob.getName(), subExchangisJob.getSourceType(), inputJob.getId(), inputJob.getName());
                            sinkHandler = handlerHolders.get(SubExchangisJobHandler.DEFAULT_DATA_SOURCE_TYPE);
                        }
                        LOG.trace("Invoke handles for subJob: [{}], sourceHandler: [{}], sinkHandler: [{}]", subExchangisJob.getName(), sourceHandler, sinkHandler);
                        //TODO Handle the subExchangisJob parallel
                        if (Objects.nonNull(sourceHandler)) {
                            sourceHandler.handleSource(subExchangisJob, ctx);
                        }
                        if (Objects.nonNull(sinkHandler)){
                            sinkHandler.handleSink(subExchangisJob, ctx);
                        }
                    }
                }else{
                    throw new ExchangisJobException(ExchangisJobExceptionCode.BUILDER_TRANSFORM_ERROR.getCode(),
                            "Illegal content string: [" + inputJob.getJobContent() + "] in job, please check", null);
                }
            }else{
                LOG.warn("It looks like an empty job ? id: [{}], name: [{}]", inputJob.getId(), inputJob.getName());
            }
        }catch(Exception e){
            throw new ExchangisJobException(ExchangisJobExceptionCode.BUILDER_TRANSFORM_ERROR.getCode(),
                    "Fail to build transformJob from input job, message: [" + e.getMessage() + "]", e);
        }
        return outputJob;
    }

    private void replaceCsTableVariables(ExchangisJobInfoContent job, String nodeName) {
        try {
            if (StringUtils.isBlank(job.getContextID())) return;

            // 提取变量字符串
            Set<String> variables = new HashSet<>();
            Set<String> tableNames = new HashSet<>();
            ExchangisJobParamsContent params = job.getParams();
            List<ExchangisJobParamsContent.ExchangisJobParamsItem> items = new ArrayList<>();
            List<ExchangisJobParamsContent.ExchangisJobParamsItem> sources = params.getSources();
            List<ExchangisJobParamsContent.ExchangisJobParamsItem> sinks = params.getSinks();
            if (CollectionUtils.isNotEmpty(sources)) items.addAll(sources);
            if (CollectionUtils.isNotEmpty(sinks)) items.addAll(sinks);
            for (ExchangisJobParamsContent.ExchangisJobParamsItem item : items) {
                String configValue = String.valueOf(item.getConfigValue());
                if (StringUtils.isNotEmpty(configValue)) {
                    variables.addAll(VariableUtil.extractVariables(configValue));
                    if (CollectionUtils.isNotEmpty(variables)) {
                        for (String variable : variables) {
                            tableNames.add(variable.split("\\.")[0]);
                        }
                    }
                }
            }

            // 查询暂存表
            HashBasedTable<String, String, String> table = HashBasedTable.create();
            if (CollectionUtils.isNotEmpty(tableNames)) {
                CSTableService csTableService = CSTableService.getInstance();
                List<ContextKeyValue> csTables = csTableService.searchUpstreamTableKeyValue(job.getContextID(), nodeName);
                if (CollectionUtils.isNotEmpty(csTables)) {
                    for (ContextKeyValue contextKeyValue : csTables) {
                        CSTable csTable = (CSTable) (contextKeyValue.getContextValue().getValue());
                        String tableName = csTable.getName();
                        if (!tableNames.contains(tableName)) continue;
                        String location = csTable.getLocation();
                        FsPath resPath = StorageUtils.getFsPath(location);
                        ResultSetFactory resultSetFactory = ResultSetFactory.getInstance();
                        ResultSet<? extends MetaData, ? extends Record> resultSet =
                                resultSetFactory.getResultSetByType(ResultSetFactory.TABLE_TYPE);
                        Fs fs = FSFactory.getFs(resPath);
                        fs.init(null);
                        ResultSetReader reader =
                                ResultSetReaderFactory.getResultSetReader(resultSet, fs.read(resPath));
                        MetaData metaData = reader.getMetaData();
                        int max = 1;
                        int num = 0;
                        while (reader.hasNext()) {
                            num++;
                            if (num > max) return;
                            Record record = reader.getRecord();
                            Object[] row = ((TableRecord) record).row;
                            CSColumn[] columns = csTable.getColumns();
                            for (int i = 0; i < columns.length; i++) {
                                CSColumn column = columns[i];
                                table.put(tableName, column.getName(), String.valueOf(row[i]));
                            }
                        }
                    }
                }
            }

            // 替换变量
            if (!table.isEmpty()) {
                for (ExchangisJobParamsContent.ExchangisJobParamsItem item : items) {
                    String configValue = String.valueOf(item.getConfigValue());
                    if (StringUtils.isNotEmpty(configValue)) {
                        for (String tableName : table.rowKeySet()) {
                            Map<String, String> row = table.row(tableName);
                            for (String columnName : row.keySet()) {
                                String variable = tableName + "." + columnName;
                                if (configValue.contains("${" + variable + "}")) {
                                    item.setConfigValue(
                                            StringUtils.replace(
                                                    configValue,
                                                    "${" + variable + "}",
                                                    row.get(columnName)
                                            )
                                    );
                                    variables.remove(variable);
                                }
                            }
                        }
                    }
                }
            }

            // 验证
            if (CollectionUtils.isNotEmpty(variables)) {
                throw new IllegalArgumentException(
                        String.format(
                                "变量{%s}解析失败，请检查暂存表或暂存字段是否存在",
                                variables.iterator().next()
                        )
                );
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Set the code resource to transform job
     * @param subExchangisJob sub transform job
     */
    private void setTransformCodeResource(TransformExchangisJob.TransformSubExchangisJob subExchangisJob){
        if (subExchangisJob.getTransformType() == TransformTypes.PROCESSOR){
            TransformProcessor processor = getTransformProcessorDao().getProcInfo(
                    Long.valueOf(subExchangisJob.getJobInfoContent().getTransforms().getCodeId()));
            if (Objects.nonNull(processor)){
                // TODO maybe the content of processor doesn't store in bml
                subExchangisJob.addCodeResource(new
                        BmlResource(processor.getCodeBmlResourceId(), processor.getCodeBmlVersion()));
            }
        }
    }

    /**
     * Processor dao
     * @return dao
     */
    private JobTransformProcessorDao getTransformProcessorDao(){
        if (null == transformProcessorDao) {
            this.transformProcessorDao = SpringContextHolder.getBean(JobTransformProcessorDao.class);
        }
        return this.transformProcessorDao;
    }
    /**
     * Chain
     */
    private static class SubExchangisJobHandlerChain implements SubExchangisJobHandler{

        private String dataSourceType;

        private LinkedList<SubExchangisJobHandler> handlers = new LinkedList<>();

        public SubExchangisJobHandlerChain(){}
        public SubExchangisJobHandlerChain(String dataSourceType){
            this.dataSourceType = dataSourceType;
        }
        public void addFirstHandler(SubExchangisJobHandler handler){
            handlers.addFirst(handler);
        }

        public void addHandler(SubExchangisJobHandler handler){
            handlers.add(handler);
        }
        public void sort(){
            handlers.sort(Comparator.comparingInt(SubExchangisJobHandler::order));
        }
        @Override
        public String dataSourceType() {
            return dataSourceType;
        }

        @Override
        public void handleSource(SubExchangisJob subExchangisJob, ExchangisJobBuilderContext ctx) throws ErrorException {
            for(SubExchangisJobHandler handler : handlers){
                if(handler.acceptEngine(subExchangisJob.getEngineType())) {
                    handler.handleSource(subExchangisJob, ctx);
                }
            }
        }

        @Override
        public void handleSink(SubExchangisJob subExchangisJob, ExchangisJobBuilderContext ctx) throws ErrorException {
            for(SubExchangisJobHandler handler : handlers){
                if(handler.acceptEngine(subExchangisJob.getEngineType())) {
                    handler.handleSink(subExchangisJob, ctx);
                }
            }
        }
    }


}
