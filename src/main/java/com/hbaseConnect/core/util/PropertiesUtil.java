package com.hbaseConnect.core.util;

import java.io.*;
import java.util.*;

/**
 * Created by JunHua.Deng on 2016/12/5.
 */
public class PropertiesUtil {

    public static final String configDir = "./etc/";

    public static Properties getProperties(String prodFlag) {

        String profileName = "";
        if ("test".equals(prodFlag)) {
            profileName = "application-test.properties";
        } else if ("dev".equals(prodFlag)) {
            profileName = "application-dev.properties";
        } else if ("prod".equals(prodFlag)) {
            profileName = "application-prod.properties";
        } else {
            profileName = "application.properties";
        }

        try {
            InputStream inStream = new FileInputStream(configDir + profileName);
            Properties prop = new Properties();
            prop.load(new InputStreamReader(inStream, "utf-8"));
            return prop;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("配置文件加载失败");
        }

    }

    public static Properties loadProperties(String fileName) {
        InputStream inStream = PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName);
        Properties prop = new Properties();
        try {
            prop.load(inStream);
            return prop;
        } catch (IOException e) {
        }
        return null;
    }

    public static void main(String[] args) {
        Properties properties = loadProperties("application-test.properties");
        List<String> topicList = new ArrayList<String>();
        Set<String> propertyNames = properties.stringPropertyNames();
        Iterator<String> it = propertyNames.iterator();
        while (it.hasNext()) {
            String next = it.next();
            if (next.startsWith("kafka.sync.")) {
                topicList.add(next);
            }
        }

        System.out.println(topicList);

    }
}
