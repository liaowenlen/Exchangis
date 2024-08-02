package com.webank.wedatasphere.exchangis.datasource.core.domain;

public enum DataSourceType {

    DORIS("DORIS"),

    ELASTICSEARCH("ELASTICSEARCH"),

    HIVE("HIVE"),

    KINGBASE("KINGBASE"),

    MONGODB("MONGODB"),

    MYSQL("MYSQL"),

    SFTP("SFTP"),

    ORACLE("ORACLE");

    public String name;

    DataSourceType(String name) {
        this.name = name;
    }
}
