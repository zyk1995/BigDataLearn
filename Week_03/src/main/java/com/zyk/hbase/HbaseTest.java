package com.zyk.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseTest {

    /*
     * docker run -d  -p 2181:2181 -p 16000:16000 -p 16010:16010 -p 16201:16201 -p 16301:16301 -p 8080:8080 -p 8085:8085 -p 9090:9090 -p:9095:9095 -p 16020:16020 --name hbase001 harisekhon/hbase /bin/sh
     */
    public static void main(String[] args) throws IOException {
        // 建立连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:16000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("student");
        String colFamilyInfo = "info";
        String colFamilyScore = "score";

        int rowKey = 1;


        // 插入数据
        Put put = new Put(Bytes.toBytes(rowKey)); // row key
        put.addColumn(Bytes.toBytes(colFamilyInfo), Bytes.toBytes("uid"), Bytes.toBytes(001)); // col1
        put.addColumn(Bytes.toBytes(colFamilyInfo), Bytes.toBytes("name"), Bytes.toBytes("Tom")); // col2
        conn.getTable(tableName).put(put);
        System.out.println("Data insert success");

        // 查看数据
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }

        // 删除数据
        Delete delete = new Delete(Bytes.toBytes(rowKey));      // 指定rowKey
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");

        // 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }


}
