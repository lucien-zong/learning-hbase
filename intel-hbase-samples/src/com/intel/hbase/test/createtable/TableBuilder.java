package com.intel.hbase.test.createtable;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.partition.SuffixClusterLocator;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

public class TableBuilder {
    /**
     * @param args
     */
    public static void main(String[] args) {

        if (args.length != 2) {
            printUsage();
            System.exit(1);
        }
        int lengthOfID = Integer.parseInt(args[0]);
        boolean isPartition = args[1].equalsIgnoreCase("true");
        long maxNum = (long) Math.pow(10, lengthOfID);

        Configuration conf = HBaseConfiguration.create();

        byte[] columnFamily = Bytes.toBytes("family");
        byte[] column1 = Bytes.toBytes("long");
        byte[] column2 = Bytes.toBytes("longStr1");
        byte[] column3 = Bytes.toBytes("compositeLongStr");
        byte[] column4 = Bytes.toBytes("longStr2");
        byte[] column5 = Bytes.toBytes("mod10Str");

        String tableName;
        if (isPartition) {
            tableName = "Test_Aggregate_With_Partition";
        } else {
            tableName = "Test_Aggregate";
        }

        try {
            HBaseAdmin hba = new HBaseAdmin(conf);
            if (hba.tableExists(tableName)) {
                hba.disableTable(tableName);
                hba.deleteTable(tableName);
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(
                    columnFamily);
            columnDescriptor.setMaxVersions(1);
            columnDescriptor.setReplication((short) 1);
            columnDescriptor.setBloomFilterType(BloomType.ROW);
            tableDescriptor.addFamily(columnDescriptor);
            if (isPartition) {
                hba.createTable(tableDescriptor, Bytes
                        .toBytes(convertLongToString(0, lengthOfID)), Bytes
                        .toBytes(convertLongToString(maxNum - 1, lengthOfID)),
                        10, new SuffixClusterLocator());
            } else {
                hba.createTable(tableDescriptor, Bytes
                        .toBytes(convertLongToString(0, lengthOfID)), Bytes
                        .toBytes(convertLongToString(maxNum - 1, lengthOfID)),
                        10);
            }
            HTable table = new HTable(conf, tableName);
            table.setAutoFlush(false);
            table.setWriteBufferSize(1024 * 1024);
            for (long i = 0; i < maxNum; i++) {
                Put put = new Put(Bytes.toBytes(getRowKey(isPartition, i,
                        lengthOfID)));
                put.add(columnFamily, column1, Bytes.toBytes(i));
                put.add(columnFamily, column2, Bytes.toBytes("" + i));
                put.add(columnFamily, column3,
                        Bytes.toBytes(i + "," + (i + 100l)));
                put.add(columnFamily, column4, Bytes.toBytes("" + i));
                put.add(columnFamily, column5, Bytes.toBytes("" + (i % 10)));
                table.put(put);
            }
            table.flushCommits();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static String getRowKey(boolean isPartition, long num, int length) {
        if (isPartition) {
            if (Math.random() > 0.5)
                return convertLongToString(num, length) + ",c01";
            else
                return convertLongToString(num, length) + ",c02";
        } else {
            return convertLongToString(num, length) + ",c01";
        }

    }

    private static String convertLongToString(long num, int size) {
        String s = String.valueOf(num);
        int delta = size - s.length();
        while (delta != 0) {
            s = "0" + s;
            delta--;
        }

        return s;
    }

    private static void printUsage() {
        System.out
                .println("Usage: java com.intel.hbase.test.createtable.TableBuilder dataLength true/false");
    }

}
