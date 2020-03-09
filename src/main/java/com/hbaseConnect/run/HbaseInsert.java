package com.hbaseConnect.run;

import java.io.IOException;
import java.util.Properties;
import com.hbaseConnect.core.config.HbaseConfigUtils;
import com.hbaseConnect.core.hbase.Hbase;
import com.hbaseConnect.core.hbase.HbaseConfig;
import com.hbaseConnect.core.hbase.HbaseFactory;
import com.hbaseConnect.core.hbase.HbaseUtils;
import com.hbaseConnect.core.util.PropertiesUtil;
import com.google.common.base.Stopwatch;

/*
 * 单条数据写入。仅用于测试
 */
public class HbaseInsert {

    public static void main(String[] args) throws IOException {

        String envFlag = "dev";
        String tableName = "coupon_reco_test";
        String key = "1671";
        String family = "data";
        String column = "templateNo";
        String value = "5";

        if (args.length >= 6) {
            envFlag = args[0];
            tableName = args[1];
            key = args[2];
            family = args[3];
            column = args[4];
            value = args[5];
        } else {
            System.out.println("Error arguments!");
            return;
        }

        Properties properties = PropertiesUtil.getProperties(envFlag);
        HbaseConfig hbaseConfig = HbaseConfigUtils.getUserConfig(properties);

        Hbase hbase = HbaseFactory.creatHbase(hbaseConfig);

        Stopwatch stopwatch = new Stopwatch().start();

        hbase.addData(tableName, key, family, column, value);
        String retValue = HbaseUtils.getValue(hbase, tableName, key, family, column);

        System.out.println("value=" + retValue);
        System.out.println("stopwatch.elapsedMillis()=" + stopwatch.elapsedMillis() / 1000);

    }

}
