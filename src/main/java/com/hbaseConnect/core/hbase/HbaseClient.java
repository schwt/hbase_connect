package com.hbaseConnect.core.hbase;

import com.hbaseConnect.core.config.HbaseConfigUtils;
import com.hbaseConnect.core.util.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by ethan on 17/8/22.
 */
public class HbaseClient {

    private static Logger logger = LoggerFactory.getLogger(HbaseClient.class);

    // 声明静态配置
    private static Configuration conf = HBaseConfiguration.create();
    private static HbaseConfig hbaseConfig = HbaseConfigUtils.getDeviceConfig(PropertiesUtil.getProperties("dev"));

    private static volatile Connection connection;

    static {
        conf.set("hbase.zookeeper.property.clientPort", hbaseConfig.getZKClientPort());
        conf.set("hbase.zookeeper.quorum", hbaseConfig.getZKquorum());
        conf.set("hbase.master", hbaseConfig.getHbaseMaster());
    }

    private static Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            if (connection == null || connection.isClosed()) {
                try {
                    connection = ConnectionFactory.createConnection(conf);
                } catch (IOException e) {
                    logger.error("Hbase 建立链接失败", e);
                }

            }
        }
        return connection;
    }

    /**
     * 创建表
     *
     * @tableName 表名
     *
     * @family 列族列表
     */
    public static void createTable(String tableName, String[] family) throws Exception {
        Admin admin = getConnection().getAdmin();
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        for (int i = 0; i < family.length; i++) {
            desc.addFamily(new HColumnDescriptor(family[i]));
        }
        if (admin.tableExists(TableName.valueOf(tableName))) {
            logger.error("table Exists!");
            System.exit(0);
        } else {
            admin.createTable(desc);
            logger.info("create table Success!");
        }
    }

    /**
     * Add data.
     *
     * @param tableName
     *            the table name
     * @param rowKey
     *            the row key
     * @param familyName
     *            the family name
     * @param columnMapValue
     *            the column map value
     * @throws IOException
     *             the io exception
     */
    public static void addData(String tableName, String rowKey, String familyName, Map<String, String> columnMapValue)
        throws IOException {

        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        for (Map.Entry<String, String> element : columnMapValue.entrySet()) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(element.getKey()),
                Bytes.toBytes(element.getValue()));
        }
        table.put(put);
        table.close();
        logger.info("addData table Success!");
    }

    /**
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param column
     * @param columnValue
     * @throws IOException
     */
    public static void addData(String tableName, String rowKey, String familyName, String column, String columnValue)
        throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column), Bytes.toBytes(columnValue));

        table.put(put);
        table.close();
        logger.info("addData table Success!");
    }

    /**
     * Update table.
     * 
     * 更新表中的某一列
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     *
     * @familyName 列族名
     *
     * @columnName 列名
     *
     * @value 更新后的值
     */
    public void updateTable(String tableName, String rowKey, String familyName, String columnName, String value)
        throws IOException {

        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     * Batch add data.
     *
     * @param tableName
     *            the table name
     * @param familyName
     *            the family name
     * @param messagesMap
     *            the messages map
     * @throws IOException
     *             the io exception
     */
    public static void batchAddData(String tableName, String familyName, Map<String, Map<String, String>> messagesMap)
        throws IOException {

        Table table = getConnection().getTable(TableName.valueOf(tableName));
        for (Map.Entry<String, Map<String, String>> message : messagesMap.entrySet()) {
            String rowKey = message.getKey();
            Map<String, String> elements = message.getValue();
            for (Map.Entry<String, String> element : elements.entrySet()) {
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(element.getKey()),
                    Bytes.toBytes(element.getValue()));
                table.put(put);
            }
        }
        logger.info("batch add data Success!");
    }

    /**
     * Batch add data.
     *
     * @param tableName
     *            the table name
     * @param puts
     *            the puts
     * @throws Exception
     *             the exception
     */
    public void batchAddData(String tableName, List<Put> puts) throws Exception {
        Connection connection = getConnection();
        final BufferedMutator.ExceptionListener exceptionListener = new BufferedMutator.ExceptionListener() {
            // fixxed wyb
            // @Override
            public void onException(RetriesExhaustedWithDetailsException exception, BufferedMutator mutator)
                throws RetriesExhaustedWithDetailsException {
                for (int i = 0; i < exception.getNumExceptions(); i++) {
                    logger.error("Failed to sent put " + exception.getRow(i) + ".");
                }
            }
        };
        BufferedMutatorParams params =
            new BufferedMutatorParams(TableName.valueOf(tableName)).listener(exceptionListener);
        params.writeBufferSize(5 * 1024 * 1024);
        final BufferedMutator mutator = connection.getBufferedMutator(params);
        try {
            mutator.mutate(puts);
            mutator.flush();
        } finally {
            mutator.close();
            closeConnection();
        }
    }

    /*
     * 根据rwokey查询
     *
     * @rowKey rowKey
     *
     * @tableName 表名
     */
    public static Result getResult(String tableName, String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Result result = table.get(get);
        return result;
    }

    /*
     * 遍历查询hbase表
     *
     * @tableName 表名
     */
    public ResultScanner getResultScan(String tableName) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = null;
        Table table = getConnection().getTable(TableName.valueOf(tableName));
        try {
            rs = table.getScanner(scan);
        } finally {
            rs.close();
        }
        return rs;
    }

    /*
     * 遍历查询hbase表
     *
     * @tableName 表名
     */
    public ResultScanner getResultScan(String tableName, String startRowKey, String stopRowKey) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRowKey));
        scan.setStopRow(Bytes.toBytes(stopRowKey));
        ResultScanner rs = null;
        Table table = getConnection().getTable(TableName.valueOf(tableName));
        try {
            rs = table.getScanner(scan);
        } finally {
            rs.close();
        }
        return rs;
    }

    /*
     * 查询表中的某一列
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     */
    public static Result getResultByColumn(String tableName, String rowKey, String familyName, String columnName)
        throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
        Result result = table.get(get);
        return result;
    }

    /*
     * 查询某列数据的多个版本
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     *
     * @familyName 列族名
     *
     * @columnName 列名
     */
    public Result getResultByVersion(String tableName, String rowKey, String familyName, String columnName)
        throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        get.setMaxVersions(5);
        Result result = table.get(get);
        return result;
    }

    /*
     * 删除指定的列
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     *
     * @familyName 列族名
     *
     * @columnName 列名
     */
    public void deleteColumn(String tableName, String rowKey, String falilyName, String columnName) throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.addColumn(Bytes.toBytes(falilyName), Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        logger.info(falilyName + ":" + columnName + "is deleted!");
    }

    /*
     * 删除指定的列
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     */
    public void deleteAllColumn(String tableName, String rowKey) throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteAll);
        logger.info("all columns are deleted!");
    }

    /*
     * 删除表
     *
     * @tableName 表名
     */
    public void deleteTable(String tableName) throws IOException {
        Admin admin = getConnection().getAdmin();
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        logger.info(tableName + "is deleted!");
    }

    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                logger.error("returnConnection error:", e);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", hbaseConfig.getZKClientPort());
        configuration.set("hbase.zookeeper.quorum", hbaseConfig.getZKquorum());
        configuration.set("hbase.master", hbaseConfig.getHbaseMaster());
        configuration.setInt("zookeeper.session.timeout", 100);
        configuration.setInt("hbase.client.operation.timeout", 1);
        configuration.setInt("hbase.client.retries.number", 1);
        configuration.setInt("hhbase.client.pause", 1);
        configuration.setInt("ipc.socket.timeout", 1);
        configuration.setInt("hbase.rpc.timeout", 1000);

        Hbase hbase = HbaseFactory.creatHbase(HbaseConfigUtils.getDeviceConfig(PropertiesUtil.getProperties("dev")));
        String value = HbaseUtils.getValue(hbase, "car_info", "carId_01", "data", "carbon_rate");
        System.out.println("value:" + value);
    }
}
