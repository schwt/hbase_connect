package com.hbaseConnect.run;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbaseConnect.core.config.HbaseConfigUtils;
import com.hbaseConnect.core.hbase.Hbase;
import com.hbaseConnect.core.hbase.HbaseConfig;
import com.hbaseConnect.core.hbase.HbaseFactory;
import com.hbaseConnect.core.util.PropertiesUtil;
import com.google.common.base.Stopwatch;

/**
 * @author wyb 从文件数据批量写hbase Map方式批量写太慢，跟单独写速度相同；List<put>方式更快
 */
public class HbaseInsertList {

    /*
     * 解析值映射文件
     */
    private static boolean readValueMap(Map<String, String> map, String file) throws IOException {
        String str = null;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(file)));
        while ((str = bufferedReader.readLine()) != null) {
            String[] arr = str.split(",");
            if (arr.length != 2) {
                continue;
            }
            map.put(arr[0], arr[1]);
        }
        bufferedReader.close();
        System.out.println("# value map: " + map.size());
        return true;
    }

    public static Map<String, Integer> mapInsertType = new HashMap<String, Integer>() {
        private static final long serialVersionUID = 3199026381448886424L;
        {
            this.put("full", 1);
            this.put("value", 2);
            this.put("kind", 3);
        }
    };

    public static void main(String[] args) throws Exception {

        String envFlag = "dev";
        String tableName = "coupon_reco_test";
        String fileData = "data.txt"; // 核心数据文件，userId '\t' value
        String fileValueMap = "valueMap.txt"; // value map文件，提供结果值映射
        String insertType = "full"; // "full", "value", "kind"
        int bufferSize = 1000;

        String family = "data";
        String column1 = "templateNo";
        String column2 = "couponKind";
        String column3 = "number";
        String value2 = "2";
        String value3 = "1";

        if (args.length >= 6) {
            envFlag = args[0];
            tableName = args[1];
            fileData = args[2];
            fileValueMap = args[3];
            insertType = args[4];
            bufferSize = Integer.parseInt(args[5]);
        } else {
            System.out.println("Error arguments!");
            return;
        }

        Integer type = mapInsertType.getOrDefault(insertType, 1);
        if (type == null) {
            System.out.println("Error argument InsertType!");
            return;
        }

        Map<String, String> mapValue = new HashMap<String, String>();
        readValueMap(mapValue, fileValueMap);

        Stopwatch stopwatch = new Stopwatch().start();
        Properties properties = PropertiesUtil.getProperties(envFlag);
        HbaseConfig hbaseConfig = HbaseConfigUtils.getUserConfig(properties);
        Hbase hbase = HbaseFactory.creatHbase(hbaseConfig);

        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(fileData)));
        List<Put> puts = new ArrayList<Put>();
        int cntTotal = 0;
        int cntEffect = 0;
        String str = "";
        while ((str = bufferedReader.readLine()) != null) {
            cntTotal++;
            String[] arr = str.split("\t");
            if (arr.length != 2) {
                continue;
            }
            String key = arr[0];
            String value = arr[1];
            value = mapValue.getOrDefault(value, value);
            if ((!key.isEmpty()) && (!value.isEmpty())) {

                Put put = new Put(Bytes.toBytes(key));
                if (type == 1) { // full
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column1), Bytes.toBytes(value));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column2), Bytes.toBytes(value2));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column3), Bytes.toBytes(value3));
                } else if (type == 2) { // value
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column1), Bytes.toBytes(value));
                } else if (type == 3) { // kind
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column2), Bytes.toBytes(value2));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column3), Bytes.toBytes(value3));
                }
                puts.add(put);

                if (puts.size() >= bufferSize) {
                    hbase.batchAddData(tableName, puts);
                    cntEffect += puts.size();
                    puts.clear();
                }
            }
        }
        if (puts.size() >= 0) {
            hbase.batchAddData(tableName, puts);
            cntEffect += puts.size();
        }

        bufferedReader.close();
        System.out.println("# data readed: " + cntTotal);
        System.out.println("# data insert: " + cntEffect);
        System.out.println("# used time:  " + stopwatch.elapsedMillis() / 1000);
    }
}
