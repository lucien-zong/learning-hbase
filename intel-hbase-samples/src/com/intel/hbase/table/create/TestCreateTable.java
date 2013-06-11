package com.intel.hbase.table.create;

import java.io.IOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.partition.ClusterLocator;
import org.apache.hadoop.hbase.partition.ParseClusterLocator;
import org.apache.hadoop.hbase.partition.SuffixClusterLocator;
import org.apache.hadoop.hbase.util.Bytes;

import com.intel.idh.test.util.ResultHTMLGenerater;

public class TestCreateTable {

    private static ResultHTMLGenerater rg = new ResultHTMLGenerater();

    private static HBaseAdmin hba;

    public static void main(String[] args) {
        try {
            boolean isPartitionEnabled = false;
            if (args[0].equals("true")) {
                isPartitionEnabled = true;
            }
            System.out.println("ssssssssss");
            hba = new HBaseAdmin(Configure.getHBaseConfig());
           
            createTable("Test_Table");
            createTableWithSplitKeys("Test_Table_SpilitKey");
            createTableWithStartAndEndKey("Test_Table_StartKey_EndKey_Num");

            if (isPartitionEnabled == true) {
                createPartitionTable("Test_Table_Locator",
                        new SuffixClusterLocator());
                createPartitionTableWithSplitKeys(
                        "Test_Table_Spilit_Key_Locator",
                        new SuffixClusterLocator());
                createPartitionTableWithStartAndEndKey(
                        "Test_Table_StartKey_EndKey_Num_Locator",
                        new SuffixClusterLocator());
            }

            // Async methods
            // Not finished now
            // createPartitionTableAsync("Test_Table_Async_Locator", new
            // SuffixClusterLocator());
            // createPartitionTableAsyncWithSpiltKeys("Test_Table_Async_SplitKeys_Locator",
            // new SuffixClusterLocator());

            try {
                tableExistFamily(hba, "Test_Table");
                if (isPartitionEnabled == true) {
                    tableExistFamily(hba, "Test_Table_Locator");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (isPartitionEnabled == true) {
                createTableFromLocatorString(hba,
                        "Test_Table_LocatorStr_Suffix",
                        "SuffixClusterLocator(':')");
                createTableFromLocatorString(hba,
                        "Test_Table_LocatorStr_Prefix",
                        "PrefixClusterLocator(':')");
                createTableFromLocatorString(hba,
                        "Test_Table_LocatorStr_Substring",
                        "SubstringClusterLocator(0,3)");
                createTableFromLocatorString(hba,
                        "Test_Table_LocatorStr_Composite",
                        "CompositeSubstringClusterLocator(':', 0)");
            }

            rg.setTitle("Test Create Table");
            rg.generateHTMLFile("result.html");

        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (hba != null) {
                try {
                    hba.close();
                } catch (IOException e) {
                }
            }

        }
    }

    public static void tableExistFamily(HBaseAdmin hba, String tableName)
            throws IOException {
        String testColumn1 = "test1";
        String testColumn2 = "test2";
        boolean before = false;
        boolean after = false;
        before = existsFamilyName(hba, tableName, testColumn1);//无 "test1" 列
        boolean getException = false;
        try {
            hba.addColumn(tableName, new HColumnDescriptor(testColumn1));//添加 "test1" 列
        } catch (TableNotDisabledException e) {
            getException = true;
        } finally {
            rg.addCase("add column " + testColumn1, "add column " + testColumn1
                    + " to " + tableName + " get exception", getException);
        }
        after = existsFamilyName(hba, tableName, testColumn1);

        getException = false;
        try {
            hba.deleteColumn(tableName, testColumn1);
        } catch (TableNotDisabledException e) {
            getException = true;
        } finally {
            rg.addCase("delete column " + testColumn1, "delete column "
                    + testColumn1 + " from " + tableName + " get exception",
                    getException);
        }

        after = existsFamilyName(hba, tableName, testColumn1);

        hba.disableTable(tableName);
        rg.addCase("disable table", "disable table " + tableName,
                hba.isTableDisabled(tableName));

        before = existsFamilyName(hba, tableName, testColumn2);
        hba.addColumn(tableName, new HColumnDescriptor(testColumn2));
        after = existsFamilyName(hba, tableName, testColumn2);
        System.out.println(before + " : " + after);
        rg.addCase("add column " + testColumn2, "add column " + testColumn2
                + " to " + tableName, (!before) && after);

        before = after;
        hba.deleteColumn(tableName, testColumn2);
        after = existsFamilyName(hba, tableName, testColumn2);
        System.out.println(before + " : " + after);
        rg.addCase("delete column " + testColumn2, "delete column "
                + testColumn1 + " from " + tableName, before && (!after));

        hba.enableTable(tableName);
        rg.addCase("enable table", "enable table " + tableName,
                hba.isTableAvailable(tableName));
    }

    public static boolean existsFamilyName(HBaseAdmin hba, String tableName,
            String columnName) {
        HTableDescriptor[] list;
        try {
            list = hba.listTables();
            for (int i = 0; i < list.length; i++) {
                if (list[i].getNameAsString().equals(tableName))
                    for (HColumnDescriptor hc : list[i].getColumnFamilies()) {
                        if (hc.getNameAsString().equals(columnName))
                            return true;
                    }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return false;
    }

    public static HTableDescriptor genHTableDescriptor(String tableName) {

        HTableDescriptor ht = new HTableDescriptor(tableName);
        HColumnDescriptor desc = new HColumnDescriptor(Configure.FAMILY_NAME);
        Configure.configColumnFamily(desc);
        ht.addFamily(desc);
        return ht;
    }

    public static void createTableFromLocatorString(HBaseAdmin hba,
            final String tableName, final String locator) {
        boolean result = false;
        try {
            removeTable(tableName);
            ParseClusterLocator parser = new ParseClusterLocator();
            ClusterLocator l = parser.parseClusterLocatorString(Bytes
                    .toBytes(locator));
            hba.createTable(genHTableDescriptor(tableName), l);
            result = hba.tableExists(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rg.addCase("createTableFromLocatorString",
                    "Create Table using parseClusterLocatorString---table name:"
                            + tableName + ", cluster string:" + locator, result);
        }
    }

    public static byte[][] genSplitKeys() {
    	
    	byte[][] b = new byte[2][3];
    	b[0][0] = 'a';
    	b[0][1] = 'b';
    	b[0][2] = 'c';
    	b[1][0] = 'd';
    	b[1][1] = 'e';
    	b[1][2] = 'f';
    	
    	return b;
    	
//        return new byte[0][];
    }

    private static void removeTable(String tableName) throws IOException {
        if (hba.tableExists(tableName)) {
            hba.disableTable(tableName);
            hba.deleteTable(tableName);
        }
    }

    public static void createTable(String tableName) {

        boolean result = false;

        try {
            removeTable(tableName);
            hba.createTable(genHTableDescriptor(tableName));
            result = hba.tableExists(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rg.addCase("CreateTable",
                    "Create Table using createTable(HTableDescriptor desc) ",
                    result);
        }
    }

    public static void createTableWithSplitKeys(String tableName) {

        boolean result = false;
        try {
            removeTable(tableName);
            hba.createTable(genHTableDescriptor(tableName), genSplitKeys());
            result = hba.tableExists(tableName);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rg.addCase("CreateTableWithSplitKeys",
                    "Create Table using createTable(HTableDescriptor desc, byte [][] splitKeys):"
                            + tableName, result);
        }
    }

    public static void createTableWithStartAndEndKey(String tableName) {
        boolean result = false;
        try {
            removeTable(tableName);
            hba.createTable(genHTableDescriptor(tableName),
                    Bytes.toBytes("123"), Bytes.toBytes("456"), 10);
            result = hba.tableExists(tableName);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rg.addCase(
                    "CreateTableWithStartAndEndKey",
                    "Create Table using createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions):"
                            + tableName, result);
        }
    }

    public static void createPartitionTable(String tableName,
            ClusterLocator locator) {
        boolean result = false;
        try {
            removeTable(tableName);
            hba.createTable(genHTableDescriptor(tableName), locator);
            result = hba.tableExists(tableName);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rg.addCase(
                    "createPartionTable",
                    "Create Table using createTable(HTableDescriptor desc, ClusterLocator locator):"
                            + tableName, result);
        }
    }

    public static void createPartitionTableWithSplitKeys(String tableName,
            ClusterLocator locator) {
        boolean result = false;
        try {
            removeTable(tableName);
            hba.createTable(genHTableDescriptor(tableName), genSplitKeys(),
                    locator);
            result = hba.tableExists(tableName);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rg.addCase(
                    "createPartitionTableWithSplitKeys",
                    "Create Table using createTable(HTableDescriptor desc, byte[][] splitKeys, ClusterLocator locator):"
                            + tableName, result);
        }
    }

    public static void createPartitionTableWithStartAndEndKey(String tableName,
            ClusterLocator locator) {
        boolean result = false;
        try {
            removeTable(tableName);
            hba.createTable(genHTableDescriptor(tableName),
                    Bytes.toBytes("123"), Bytes.toBytes("456"), 10, locator);
            result = hba.tableExists(tableName);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rg.addCase(
                    "createPartitionTableWithStartAndEndKey",
                    "Create Table using createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions, ClusterLocator locator):"
                            + tableName, result);
        }
    }

    public static void createPartitionTableAsync(String tableName,
            ClusterLocator locator) {
        // TODO how to check it.
        System.out.println("Not Finished yet");
    }

    public static void createPartitionTableAsyncWithSpiltKeys(String tableName,
            ClusterLocator locator) {
        // TODO how to check it.
        System.out.println("Not Finished yet");
    }

}
