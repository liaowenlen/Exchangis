package com.webank.wedatasphere.exchangis.extension.datasource.doris;

import com.webank.wedatasphere.exchangis.dao.domain.ExchangisJobParamConfig;
import com.webank.wedatasphere.exchangis.datasource.core.domain.Classifier;
import com.webank.wedatasphere.exchangis.datasource.core.domain.DataSourceType;
import com.webank.wedatasphere.exchangis.datasource.core.domain.StructClassifier;
import com.webank.wedatasphere.exchangis.datasource.linkis.ExchangisBatchDataSource;

import java.util.List;

/**
 * @author LWL
 * @date 2024/07/23
 */
public class ExchangisDorisDataSource extends ExchangisBatchDataSource {
    @Override
    public String name() {
        return DataSourceType.DORIS.name;
    }

    @Override
    public String classifier() {
        return Classifier.DORIS.name;
    }

    @Override
    public String structClassifier() {
        return StructClassifier.STRUCTURED.name;
    }

    @Override
    public String description() {
        return "This is Doris DataSource";
    }

    @Override
    public String option() {
        return "Doris数据库";
    }

    @Override
    public String icon() {
        return "icon-doris";
    }

    @Override
    public List<ExchangisJobParamConfig> getDataSourceParamConfigs() {
        return super.getDataSourceParamConfigs(DataSourceType.DORIS.name);
    }
}
