package com.hbaseConnect.run;

import com.hbaseConnect.core.config.HbaseConfigUtils;
import com.hbaseConnect.core.hbase.Hbase;
import com.hbaseConnect.core.hbase.HbaseConfig;
import com.hbaseConnect.core.hbase.HbaseFactory;
import com.hbaseConnect.core.hbase.HbaseUtils;
import com.hbaseConnect.core.util.PropertiesUtil;
import com.google.common.base.Stopwatch;

import java.io.IOException;
import java.util.Properties;

/*
 * hbase单条数据读取。仅用于测试
 */
public class HbaseQueryTest {

    public static void main(String[] args) throws IOException {

        String prodFlag   = "dev";
        String tableName  = "coupon_reco_v0";
        String key        = "2002";
        String familyName = "data";
        String col        = "templateNo";

        if (args.length == 5) {
            prodFlag   = args[0];
            tableName  = args[1];
            key        = args[2];
            familyName = args[3];
            col        = args[4];
        } else {
            System.out.println("Error args! (len=" + args.length + ")");
        }

        Properties properties = PropertiesUtil.getProperties(prodFlag);
        HbaseConfig hbaseConfig = HbaseConfigUtils.getUserConfig(properties);
        Hbase hbase = HbaseFactory.creatHbase(hbaseConfig);

        Stopwatch stopwatch = new Stopwatch().start();

        String value = HbaseUtils.getValue(hbase, tableName, key, familyName, col);

        System.out.println(col + "=" + value);

        System.out.println("stopwatch.elapsedMillis()=" + stopwatch.elapsedMillis() / 1000);

    }

}
