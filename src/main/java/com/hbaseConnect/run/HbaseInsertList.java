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

    public static void main(String[] args) throws Exception {

        String envFlag      = "dev";
        String tableName    = "coupon_reco_test";
        String fileData     = "data.txt"; // 核心数据文件，userId '\t' value
        String fileValueMap = "valueMap.txt"; // value map文件，提供结果值映射
        int bufferSize      = 1000;

        String family     = "data";
        String column1    = "templateNo";
        String column2    = "couponKind";
        String column3    = "number";
        String couponKind = "2";

        if (args.length == 5) {
            envFlag      = args[0];
            tableName    = args[1];
            fileData     = args[2];
            fileValueMap = args[3];
            bufferSize   = Integer.parseInt(args[4]);
        } else {
            System.out.println("Error arguments!");
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
        Map<Integer, Long> mapLineStat = new HashMap<Integer, Long>();
        String str = "";
        while ((str = bufferedReader.readLine()) != null) {
            cntTotal++;
            String[] arr = str.split("\t");
            mapLineStat.put(arr.length, 1+mapLineStat.getOrDefault(arr.length, 0L));
            if (arr.length < 2 || arr.length > 3) {
                continue;
            }
            String userId = arr[0];
            String templateNo = arr[1];
            String number = "1";
            if (arr.length == 3) {
                number = arr[2];
            }
            templateNo = mapValue.getOrDefault(templateNo, templateNo);
            if ((!userId.isEmpty()) && (!templateNo.isEmpty())) {

                Put put = new Put(Bytes.toBytes(userId));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column1), Bytes.toBytes(templateNo));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column2), Bytes.toBytes(couponKind));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column3), Bytes.toBytes(number));
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
        System.out.println("data file col nums:");
        for (Map.Entry<Integer, Long> entry: mapLineStat.entrySet()) {
            System.out.println("   # sep: " + entry.getKey() + ",\t" + entry.getValue());
        }
        System.out.println("# data readed: " + cntTotal);
        System.out.println("# data insert: " + cntEffect);
        System.out.println("# used time:  " + stopwatch.elapsedMillis() / 1000);
    }
}
