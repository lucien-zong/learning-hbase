package com.intel.hbase.test.fulltextindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.search.IndexAnalyzerNames;
import org.apache.hadoop.hbase.search.IndexMetadata;
import org.apache.hadoop.hbase.search.IndexMetadata.IndexFieldMetadata;
import org.apache.hadoop.hbase.search.IndexMetadata.IndexFieldType;
import org.apache.hadoop.hbase.search.IndexMetadata.IndexedHBaseColumn;
import org.apache.hadoop.hbase.search.IndexSearcher.SearchMode;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTable {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.exit(1);
        }
        String tableName = args[0];
        Configuration conf = HBaseConfiguration.create();

        IndexMetadata indexMetadata = new IndexMetadata();

        indexMetadata.setSearchMode(SearchMode.LastCommit);
        IndexFieldMetadata valueMeta1 = new IndexFieldMetadata();
        valueMeta1.setName(Bytes.toBytes("field1"));
        valueMeta1
                .setAnalyzerMetadataName(IndexAnalyzerNames.COMMA_SEPARATOR_ANALYZER);
        valueMeta1.setIndexed(true);
        valueMeta1.setTokenized(true);
        valueMeta1.setStored(true);
        valueMeta1.setType(IndexFieldType.STRING);

        IndexedHBaseColumn column11 = new IndexedHBaseColumn(
                Bytes.toBytes("values"));
        column11.addQualifierName(Bytes.toBytes("value"));

        valueMeta1.addIndexedHBaseColumn(column11);

        indexMetadata.addFieldMetadata(valueMeta1);

        HTableDescriptor desc = new HTableDescriptor(tableName);
        desc.setIndexMetadata(indexMetadata);
        desc.addFamily(new HColumnDescriptor("values"));
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(new Configuration(conf));
            admin.createTable(desc);
        } finally {
            admin.close();
        }
    }
}
