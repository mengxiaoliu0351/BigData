package hbase_exp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtil {
    private static Connection conn;

    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 1. 创建表
    public static void createTable(String tableName, String... cfs) throws IOException {
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            System.out.println("表 " + tableName + " 已存在，跳过创建");
            admin.close();
            return;
        }
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tn);
        for (String cf : cfs) {
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf));
        }
        admin.createTable(builder.build());
        System.out.println("表 " + tableName + " 创建成功");
        admin.close();
    }

    // 2. 添加/更新一条记录（put）
    public static void addRecord(String tableName, String rowKey, String cf, String col, String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(value));
        table.put(put);
        System.out.println("写入成功 rowkey=" + rowKey + " " + cf + ":" + col + "=" + value);
        table.close();
    }

    // 3. 扫描指定列族数据 scan
    public static void scanColumn(String tableName, String cf) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(cf));
        ResultScanner scanner = table.getScanner(scan);
        System.out.println("\n==== scan " + tableName + " cf:" + cf + " ====");
        for (Result res : scanner) {
            for (Cell cell : res.listCells()) {
                String rk = Bytes.toString(CellUtil.cloneRow(cell));
                String c = Bytes.toString(CellUtil.cloneQualifier(cell));
                String v = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.printf("row=%s, col=%s, value=%s%n", rk, c, v);
            }
        }
        scanner.close();
        table.close();
    }

    // 4. 修改数据：和put一样，相同rowkey+列直接覆盖
    public static void modifyData(String tableName, String rowKey, String cf, String col, String newValue) throws IOException {
        addRecord(tableName, rowKey, cf, col, newValue);
        System.out.println("修改完成");
    }

    // 5. 删除整行
    public static void deleteRow(String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete del = new Delete(Bytes.toBytes(rowKey));
        table.delete(del);
        System.out.println("删除 rowkey=" + rowKey + " 成功");
        table.close();
    }

    public static void close() throws IOException {
        if (conn != null) conn.close();
    }

    public static void main(String[] args) throws IOException {
        // 建三张表，列族统一 info
        createTable("student", "info");
        createTable("course", "info");
        createTable("sc", "info");

        // 插入学生数据
        addRecord("student", "2024001", "info", "name", "LiMing");
        addRecord("student", "2024001", "info", "age", "19");
        addRecord("student", "2024002", "info", "name", "WangHong");
        addRecord("student", "2024002", "info", "age", "20");

        // 课程
        addRecord("course", "C001", "info", "cname", "Database");
        addRecord("course", "C001", "info", "credit", "3");
        addRecord("course", "C002", "info", "cname", "Hadoop");
        addRecord("course", "C002", "info", "credit", "4");

        // 选课sc
        addRecord("sc", "2024001_C001", "info", "score", "88");
        addRecord("sc", "2024001_C002", "info", "score", "92");
        addRecord("sc", "2024002_C001", "info", "score", "76");

        // 扫描查看student
        scanColumn("student", "info");

        // 修改：把2024001年龄改成20
        modifyData("student", "2024001", "info", "age", "20");
        scanColumn("student", "info");

        // 删除一行：删除2024002学生
        deleteRow("student", "2024002");
        scanColumn("student", "info");

        close();
    }
}
