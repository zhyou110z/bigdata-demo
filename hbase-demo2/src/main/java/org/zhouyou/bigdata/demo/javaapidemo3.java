package org.zhouyou.bigdata.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class javaapidemo3 {
    protected  static Connection conn;
    private static final String ZK_QUORUM = "hbase.zookeeper.quorum";
    private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";     
    private static final String HBASE_POS = "192.168.1.52";
    private static final String ZK_POS = "192.168.1.52:2181,192.168.1.52:2182,192.168.1.52:2183";
    private static final String ZK_PORT_VALUE = "2181";
    private static Configuration conf;

    /*** 静态构造，在调用静态方法前运行，  初始化连接对象  * */
    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://"+ HBASE_POS + ":9000/hbase");
        conf.set(ZK_QUORUM, ZK_POS);
        conf.set(ZK_CLIENT_PORT, ZK_PORT_VALUE);        
        //创建连接池
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }   

    }
    public static void main(String[] args) throws IOException {
       // 创建表
        HTable table = new HTable(conf, "fuzuxian");
        byte[] row = Bytes.toBytes("kr001");
        Put put = new Put(row);
        put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zuxianfu"));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("24"));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("number"), Bytes.toBytes(100000));
        table.put(put); 
        table.close();

   //  以下方法的示例
        String tableName = "fuzuxian";
        String rowKey = "fu";
        String columnFamily = "age";
        String column = "xian";
        String value = "haha";
        addRow(tableName , rowKey, columnFamily, column,  value);               
    }

//**************************** 2.  添加一条数据  *******************************

    public static void addRow(String tableName, String rowKey, String columnFamily,
                               String column, String value)   throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));   
        Put put = new Put(Bytes.toBytes(rowKey));  //   通过rowkey创建一个 put 对象    
        //  在 put 对象中设置 列族、列、值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);     //  插入数据，可通过 put(List<Put>) 批量插入      
        table.close();    
        conn.close();
    }

//  ***********************  3.  获取一条数据   *******************************

   public static void getRow(String tableName, String rowKey) throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));  
        Get get = new Get(Bytes.toBytes(rowKey));   //  通过rowkey创建一个 get 对象
        Result result = table.get(get);         //  输出结果
        for (Cell cell : result.rawCells()) {
            System.out.println(
                    "\u884c\u952e:" + new String(CellUtil.cloneRow(cell)) + "\t" +
                    "\u5217\u65cf:" + new String(CellUtil.cloneFamily(cell)) + "\t" + 
                    "\u5217\u540d:" + new String(CellUtil.cloneQualifier(cell)) + "\t" + 
                    "\u503c:" + new String(CellUtil.cloneValue(cell)) + "\t" +
                    "\u65f6\u95f4\u6233:" + cell.getTimestamp());
        }
        table.close();   
        conn.close();
    }

//  ***************************  4. 全表扫描  ****************************

    public static void scanTable(String tableName) throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));           
        Scan scan = new Scan();  //  创建扫描对象
        ResultScanner results = table.getScanner(scan);   //  全表的输出结果
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        "\u884c\u952e:" + new String(CellUtil.cloneRow(cell)) + "\t" +
                        "\u5217\u65cf:" + new String(CellUtil.cloneFamily(cell)) + "\t" + 
                        "\u5217\u540d:" + new String(CellUtil.cloneQualifier(cell)) + "\t" + 
                        "\u503c:" + new String(CellUtil.cloneValue(cell)) + "\t" +
                        "\u65f6\u95f4\u6233:" + cell.getTimestamp());
            }
        }   
        results.close();     
        table.close();
        conn.close();
    }

//**************************** 5. 删除一条数据  *****************************
    public static void delRow(String tableName, String rowKey) throws IOException { 

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));    
        Delete delete = new Delete(Bytes.toBytes(rowKey));  
        table.delete(delete);
        table.close();    
        conn.close();
    }

//************************   6. 删除多条数据  *****************************
    public static void delRows(String tableName, String[] rows) throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));     
        List<Delete> list = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            list.add(delete);
        }
        table.delete(list);
        table.close();   
        conn.close();
    }

// ***************************  7. 删除列族  **************************
    public static void delColumnFamily(String tableName, String columnFamily) 
                                                  throws IOException {

        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();  // 创建数据库管理员  
        hAdmin.deleteColumn(tableName, columnFamily);  
        conn.close();    
    }

// ***************************  8  删除数据库表  **********************

    public static void deleteTable(String tableName) throws IOException {

        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();   // 创建数据库管理员
        if (hAdmin.tableExists(tableName)) { 
            hAdmin.disableTable(tableName);     //  失效表
            hAdmin.deleteTable(tableName);     //  删除表
            System.out.println("删除" + tableName + "表成功");
            conn.close();
        } else {
            System.out.println("需要删除的" + tableName + "表不存在");
            conn.close();
            System.exit(0);
        }
    }

// ***************************  9. 追加插入  ********************************

    // 在原有的value后面追加新的value，  "a" + "bc"  -->  "abc"
    public static void appendData(String tableName, String rowKey, String columnFamily, 
                                  String column, String value)  throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));    
        Append append = new Append(Bytes.toBytes(rowKey));   
        append.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));      
        table.append(append);    
        table.close();  
        conn.close();
    }

// *****************************  10  符合条件后添加数据  *****************************

    public static boolean checkAndPut(String tableName, String rowKey, 
                   String columnFamilyCheck, String columnCheck,  String valueCheck, 
                   String columnFamily, String column, String value) throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName)); 
        Put put = new Put(Bytes.toBytes(rowKey));  
     put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        boolean result = table.checkAndPut(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamilyCheck), 
                         Bytes.toBytes(columnCheck), Bytes.toBytes(valueCheck), put); 
        table.close();   
        conn.close();   
        return result;
    }

// ****************************  11  符合条件后删除数据   ***************************

public static boolean checkAndDelete(String tableName, String rowKey, 
                String columnFamilyCheck, String columnCheck,  String valueCheck, 
                String columnFamily, String column) throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));   
        Delete delete = new Delete(Bytes.toBytes(rowKey));    
        delete.addColumn(Bytes.toBytes(columnFamilyCheck), Bytes.toBytes(columnCheck));
        boolean result = table.checkAndDelete(Bytes.toBytes(rowKey), 
                          Bytes.toBytes(columnFamilyCheck),  Bytes.toBytes(columnCheck),
                          Bytes.toBytes(valueCheck), delete); 
        table.close();   
        conn.close();
        return result;
    }

//***********************  // 11 计数器  ***********************************

 public static long incrementColumnValue(String tableName, String rowKey, 
            String columnFamily, String column, long amount)  throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));  
        long result = table.incrementColumnValue(Bytes.toBytes(rowKey), 
                      Bytes.toBytes(columnFamily), Bytes.toBytes(column), amount);  
        table.close();   
        conn.close();    
        return result;
    }
}