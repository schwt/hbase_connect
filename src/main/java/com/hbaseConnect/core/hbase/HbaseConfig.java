package com.hbaseConnect.core.hbase;

import lombok.Data;
import java.io.Serializable;

/**
 * Created by zhouyuefei on 17/6/12.
 */
@Data
public class HbaseConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String zKClientPort;
    private String zKquorum;
    private String hbaseMaster;
    private String tableName;
    private String columnFamily;
    private String operationTimeout;
    private String znode;

    public HbaseConfig(String znode, String zKClientPort, String zKquorum, String hbaseMaster, String timeout,
        String tableName, String columnFamily) {
        this.znode = znode;
        this.zKClientPort = zKClientPort;
        this.zKquorum = zKquorum;
        this.hbaseMaster = hbaseMaster;
        this.operationTimeout = timeout;
        this.tableName = tableName;
        this.columnFamily = columnFamily;
    }

    @Override
    public String toString() {
        return "HbaseConfig{" + "zKClientPort='" + zKClientPort + '\'' + ", zKquorum='" + zKquorum + '\''
            + ", hbaseMaster='" + hbaseMaster + '\'' + ", tableName='" + tableName + '\'' + ", columnFamily='"
            + columnFamily + '\'' + ", operationTimeout='" + operationTimeout + '\'' + '}';
    }
}
