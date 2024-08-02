package com.webank.wedatasphere.exchangis.extension.datasource.kingbase;

import com.webank.wedatasphere.exchangis.dao.domain.ExchangisJobParamConfig;
import com.webank.wedatasphere.exchangis.datasource.core.domain.Classifier;
import com.webank.wedatasphere.exchangis.datasource.core.domain.DataSourceType;
import com.webank.wedatasphere.exchangis.datasource.core.domain.StructClassifier;
import com.webank.wedatasphere.exchangis.datasource.linkis.ExchangisBatchDataSource;

import java.util.List;

/**
 * @author LWL
 * @date 2024/07/22
 */
public class ExchangisKingbaseDataSource extends ExchangisBatchDataSource {
    @Override
    public String name() {
        return DataSourceType.KINGBASE.name;
    }

    @Override
    public String classifier() {
        return Classifier.KINGBASE.name;
    }

    @Override
    public String structClassifier() {
        return StructClassifier.STRUCTURED.name;
    }

    @Override
    public String description() {
        return "This is Kingbase DataSource";
    }

    @Override
    public String option() {
        return "Kingbase数据库";
    }

    @Override
    public String icon() {
        return "icon-kingbase";
    }

    @Override
    public List<ExchangisJobParamConfig> getDataSourceParamConfigs() {
        return super.getDataSourceParamConfigs(DataSourceType.KINGBASE.name);
    }
}
