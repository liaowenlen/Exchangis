package com.webank.wedatasphere.exchangis.job.domain;

import org.apache.commons.lang.StringUtils;
import org.apache.linkis.manager.label.entity.Label;
import org.apache.linkis.manager.label.utils.LabelUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Generic implement
 */
public class GenericExchangisJob implements ExchangisJob {

    private static final Logger LOG = LoggerFactory.getLogger(GenericExchangisJob.class);

    protected Long id;

    protected String name;

    protected String engineType;

    private Map<String, Object> labelHolder = new HashMap<>();

    protected Date createTime;

    protected Date lastUpdateTime;

    protected String createUser;

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Date getCreateTime() {
        return createTime;
    }

    @Override
    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    @Override
    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @Override
    public String getEngineType() {
        return this.engineType;
    }

    @Override
    public void setEngineType(String engineType) {
        this.engineType = engineType;
    }

    @Override
    public Map<String, Object> getJobLabels() {
        return labelHolder;
    }

    @Override
    public void setJobLabels(String labels) {
        if (StringUtils.isNotBlank(labels)){
            try{
                Map<String, Object> labelsMap = LabelUtils.Jackson.fromJson(labels, Map.class);
                labelHolder.putAll(labelsMap);
            } catch (Exception e){
                //Don't throws exception
                LOG.warn("The input labels json \"{}\" is illegal", labels, e);
            }
        }
    }

    @Override
    public void setJobLabels(Map<String, Object> jobLabels) {
        this.labelHolder = jobLabels;
    }

    @Override
    public String getCreateUser() {
        return createUser;
    }

    @Override
    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }
}
