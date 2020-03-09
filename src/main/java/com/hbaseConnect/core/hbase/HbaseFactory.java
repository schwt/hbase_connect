package com.hbaseConnect.core.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

/**
 * Created by ethan on 2017/8/28.
 */
public class HbaseFactory {

    public static Hbase creatHbase(HbaseConfig hbaseConfig) throws IOException {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", hbaseConfig.getZKClientPort());
        configuration.set("hbase.zookeeper.quorum", hbaseConfig.getZKquorum());
        configuration.set("hbase.client.operation.timeout", hbaseConfig.getOperationTimeout());
        configuration.set("zookeeper.znode.parent", hbaseConfig.getZnode());
        return new Hbase(configuration);
    }

    public static Configuration creatConfig(HbaseConfig hbaseConfig) throws IOException {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", hbaseConfig.getZKClientPort());
        configuration.set("hbase.zookeeper.quorum", hbaseConfig.getZKquorum());
        configuration.set("hbase.client.operation.timeout", hbaseConfig.getOperationTimeout());

        return configuration;
    }
}
