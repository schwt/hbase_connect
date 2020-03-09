package com.hbaseConnect.core.config;

import com.hbaseConnect.core.hbase.HbaseConfig;

import java.util.Properties;

/**
 * Created by ethan on 2017/8/25.
 */
public class HbaseConfigUtils {

    public static HbaseConfig getUserConfig(Properties properties) {
        return new HbaseConfig(properties.getProperty("hbase.zookeeper.znode.parent"),
            properties.getProperty("hbase.zk.clientPort"), properties.getProperty("hbase.zk.quorum"),
            properties.getProperty("hbase.master"), properties.getProperty("hbase.client.operation.timeout"),
            properties.getProperty("hbase.tableName.user"), properties.getProperty("hbase.columnFamily.user"));
    }

    public static HbaseConfig getDeviceConfig(Properties properties) {
        return new HbaseConfig(properties.getProperty("hbase.zookeeper.znode.parent"),
            properties.getProperty("hbase.zk.clientPort"), properties.getProperty("hbase.zk.quorum"),
            properties.getProperty("hbase.master"), properties.getProperty("hbase.client.operation.timeout"),
            properties.getProperty("hbase.tableName.device"), properties.getProperty("hbase.columnFamily.device"));
    }

}
