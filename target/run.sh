#!/bin/sh
jar=hbase-connection-1.1.0-SNAPSHOT-jar-with-dependencies.jar

function batch_insert {
  java -cp ${jar} com.hbaseConnect.run.HbaseInsertList \
  prod coupon_reco_v0 'data.txt' 'value_map.txt' 'full' 1000
}

function QueryTest {
  java -cp ${jar} com.hbaseConnect.run.HbaseInsertList \
  prod coupon_reco_v0 '1644' 'data' 'templateNo'
}
