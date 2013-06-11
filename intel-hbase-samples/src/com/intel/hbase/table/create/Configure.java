package com.intel.hbase.table.create;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;

/**
 * A class to store all the constants for a HBase configuration
 */
public class Configure {
    public static final String FAMILY_NAME = "family";

    public static final Algorithm COMPRESS_TYPE = Algorithm.SNAPPY;
    public static final boolean USE_TTL = false;

    private static Configuration _config = HBaseConfiguration.create();

    public static Configuration getHBaseConfig() throws IOException {
        return _config;
    }

    public static void configHTable(HTableDescriptor ht) {
    }

    public static void configColumnFamily(HColumnDescriptor desc) {
        desc.setMaxVersions(1);
        desc.setBloomFilterType(BloomType.ROW);

        desc.setCompressionType(COMPRESS_TYPE);
    }

    public static HTableDescriptor genHTableDescriptor(String tableName) {
        return genHTableDescriptor(tableName, Short.MIN_VALUE);
    }

    public static HTableDescriptor genHTableDescriptor(String tableName,
            short replica) {
        HTableDescriptor ht = new HTableDescriptor(tableName);
        HColumnDescriptor desc = new HColumnDescriptor(FAMILY_NAME);
        if (replica != Short.MIN_VALUE) {
            desc.setReplication(replica);
        }
        ht.addFamily(desc);
        return ht;
    }

}
