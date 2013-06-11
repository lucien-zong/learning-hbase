package com.intel.hbase.table.replication;

import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.intel.hbase.table.create.Configure;

public class TestReplication {

    private static String tableName = "replication_one";

    public static void main(String[] agrs) {
        HBaseAdmin hba = null;
        HTable table = null;
        try {
            hba = new HBaseAdmin(Configure.getHBaseConfig());
            hba.createTable(Configure.genHTableDescriptor(tableName, (short) 1));
            table = new HTable(Configure.getHBaseConfig(), tableName);
            Put put = new Put(Bytes.toBytes("007"));
            put.add(Bytes.toBytes(Configure.FAMILY_NAME), Bytes.toBytes("key"),
                    Bytes.toBytes("value"));
            table.put(put);
            table.flushCommits();
            hba.disableTable(tableName);

        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (hba != null) {
                try {
                    hba.close();
                } catch (IOException e) {
                }
            }
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                }
            }

        }
    }
}
