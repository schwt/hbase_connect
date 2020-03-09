package com.hbaseConnect.run;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import com.google.common.base.Stopwatch;

/**
 * @author wyb 验证输入数据文件
 */
public class HbaseDataFileCheck {

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

    public static void main(String[] args) throws IOException {

        String envFlag = "dev";
        String tableName = "coupon_reco_test";
        String fileData = "data.txt"; // 核心数据文件，col1: userId, col2(if typeOrVal==-1):value
        String fileValueMap = "valueMap.txt"; // value map文件，提供结果值映射

        Map<String, String> mapValue = new HashMap<String, String>();

        if (args.length >= 4) {
            envFlag = args[0];
            tableName = args[1];
            fileData = args[2];
            fileValueMap = args[3];
        } else {
            System.out.println("Error arguments!");
            return;
        }
        // just flag
        envFlag.concat(tableName);

        Stopwatch stopwatch = new Stopwatch().start();
        readValueMap(mapValue, fileValueMap);

        String str = "";
        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(fileData)));
        int cntTotal = 0;
        int cntEffect = 0;
        while ((str = bufferedReader.readLine()) != null) {
            cntTotal++;
            String[] arr = str.split("\t");
            if (arr.length != 2) {
                System.out.println("real length: " + arr.length);
                continue;
            }
            String key = arr[0];
            String value = arr[1];
            value = mapValue.getOrDefault(value, value);
            if ((!key.isEmpty()) && (!value.isEmpty())) {
                cntEffect++;
            } else {
                System.out.println("@@ key: " + key);
                System.out.println("@@ val: " + value);
            }
        }

        bufferedReader.close();
        System.out.println("# data readed: " + cntTotal);
        System.out.println("# data insert: " + cntEffect);
        System.out.println("# used time:  " + stopwatch.elapsedMillis() / 1000);
    }
}
