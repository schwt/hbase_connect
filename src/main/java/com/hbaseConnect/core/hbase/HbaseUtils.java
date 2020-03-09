package com.hbaseConnect.core.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

public class HbaseUtils {

    public static byte[] getBytesValue(Hbase hbase, String tablename, String rowkey, String family, String colname)
        throws IOException {
        Result result = hbase.getResultByColumn(tablename, rowkey, family, colname);
        return result.getValue(Bytes.toBytes(family), Bytes.toBytes(colname));
    }

    public static String getValue(Hbase hbase, String tablename, String rowkey, String family, String colname)
        throws IOException {
        Result result = hbase.getResultByColumn(tablename, rowkey, family, colname);
        byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(colname));
        return Bytes.toString(value);
    }

    public static int getIntValue(Hbase hbase, String tablename, String rowkey, String family, String colname)
        throws IOException {
        Result result = hbase.getResultByColumn(tablename, rowkey, family, colname);
        byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(colname));
        if (null == value) {
            return 0;
        }
        return Bytes.toInt(value);
    }

    public static Long getLongValue(Hbase hbase, String tablename, String rowkey, String family, String colname)
        throws IOException {
        Result result = hbase.getResultByColumn(tablename, rowkey, family, colname);
        byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(colname));
        if (null == value) {
            return 0L;
        }
        return Bytes.toLong(value);
    }

    public static Double getDoubleValue(Hbase hbase, String tablename, String rowkey, String family, String colname)
        throws IOException {
        Result result = hbase.getResultByColumn(tablename, rowkey, family, colname);
        byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(colname));
        if (null == value) {
            return 0d;
        }
        return Bytes.toDouble(value);
    }

}
