package com.intel.hbase.test.fulltextindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.search.IndexSearcherClient;
import org.apache.hadoop.hbase.search.IndexSearchResponse;
import org.apache.hadoop.hbase.search.IndexSearchResponse.IndexableRecord;
import org.apache.hadoop.hbase.search.query.expression.MatchQueryExpression;
import org.apache.hadoop.hbase.search.query.expression.QueryExpression;
import org.apache.hadoop.hbase.util.Bytes;

public class Search {
    public static void main(String[] args) throws Throwable {
        if (args.length < 1) {
            System.exit(1);
        }
        String tableName = args[0];
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        QueryExpression q = new MatchQueryExpression("field1", "Im value1");
        IndexSearcherClient client = new IndexSearcherClient(table);
        IndexSearchResponse resp = client.search(q, 0, 50, null);
        IndexableRecord[] records = resp.getRecords();

        if (records != null) {
            for (IndexableRecord record : records) {
                String id = record.getId();
                byte[] rowKey = Bytes.toBytes(id);
                String showRowKey = Bytes.toString(rowKey);
                String value = (String) record.getValues().get("field1");
                StringBuilder sb = new StringBuilder();
                sb.append("rowkey :").append(showRowKey).append("value :")
                        .append(value);
                System.out.println(sb.toString());
            }
        }
    }
}
