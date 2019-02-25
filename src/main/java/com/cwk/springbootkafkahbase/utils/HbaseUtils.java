package com.cwk.springbootkafkahbase.utils;

import com.cwk.springbootkafkahbase.RowKey;
import com.cwk.springbootkafkahbase.bean.Kafka_RealSync_Column;
import com.cwk.springbootkafkahbase.bean.Kafka_RealSync_Metadata;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

@Component
public class HbaseUtils {

    private static Admin admin;

    private static Connection connection;

    public Connection getConnection() {
        return connection;
    }

    @Autowired
    public void setConnection(Connection connection) {
        HbaseUtils.connection = connection;
    }

    public Admin getAdmin() {
        return admin;
    }

    @Autowired
    public void setAdmin(Admin admin) {
        HbaseUtils.admin = admin;
    }

//    //判断表是否存在(过时api)
//    public static boolean tableExist(String tableName) throws Exception {
//
//        //Hbase配置文件
//        HBaseConfiguration configuration = new HBaseConfiguration();
//        //配置zk主机
//        configuration.set("hbase.zookeeper.quorum", "192.168.9.69");
//        configuration.set("hbase.zookeeper.property.clientPort", "2181");
//
//        HBaseAdmin admin = new HBaseAdmin(configuration);
//
//        boolean b = admin.tableExists(tableName);
//        admin.close();
//        return b;
//    }

    private static void close(Connection conn, Admin admin) {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                conn = null;
            }
        }
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                admin = null;
            }
        }

    }

    //判断表是否存在(new api)
    public static boolean tableExist(String tableName) throws Exception {
        boolean b = admin.tableExists(TableName.valueOf(tableName));
        //close(connection, admin);
        return b;
    }

    //创建表
    public static void createTable(String tableName, String... cfs) throws Exception {


        //判断表是否存在
        if (tableExist(tableName)) {
            System.out.println(tableName + "表已经存在");
            throw new RuntimeException();
        }

        //创建表描述器
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //添加列族
        for (String cf : cfs) {
            //创建列
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
            //设置最大版本号
            columnDescriptor.setMaxVersions(5);
            descriptor.addFamily(columnDescriptor);
        }


        admin.createTable(descriptor);

        //close(connection, admin);

        System.out.println(tableName + "表创建成功");

    }

    //删除表
    public static void deleteTable(String tableName) {
        try {
            if (!tableExist(tableName))
                return;
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println(tableName + "表已删除");
        }
    }

    //增加||修改
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {

        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //依据rowKey创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //添加数据
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        //执行操作
        table.put(put);
    }

    public static void putData(String tableName, String rowKey, String cf, Object pojo) throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //依据rowKey创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        Class<?> pojoClass = pojo.getClass();
        Field[] fields = pojoClass.getDeclaredFields();

        for (Field field : fields) {
            String fieldNameLower = field.getName().toLowerCase();
            //添加数据
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(fieldNameLower), Bytes.toBytes(String.valueOf(pojoClass.getMethod("get" + field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1), null).invoke(pojo))));
        }
        //执行操作
        table.put(put);
    }

    //public  static int sum=0;

    public static void putDataBatch(String tableName, String rowKey, String cf, List pojos) throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        ArrayList<Put> puts = new ArrayList<>();
        for (Object pojo : pojos) {
            //依据rowKey创建put对象
            Put put = new Put(Bytes.toBytes(rowKey + "-" + new Random().nextInt()));
            Class<?> pojoClass = pojo.getClass();
            Field[] fields = pojoClass.getDeclaredFields();

            for (Field field : fields) {
                String fieldNameLower = field.getName().toLowerCase();
                //添加数据
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(fieldNameLower), Bytes.toBytes(String.valueOf(pojoClass.getMethod("get" + field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1), null).invoke(pojo))));
            }
            puts.add(put);
        }

        //执行操作
        table.put(puts);
        //System.err.println("@@@@@@@@@@Hbase put datas sum: "+(sum+=5)+" @@@@@@@@@@@@@@@@@@");
    }

    public static void putDataBatch(String tableName, LinkedList<Object> pojos, String... cf) throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        ArrayList<Put> puts = new ArrayList<>();
        for (Object pojo : pojos) {
            //根据batchID生成rowkey
            Put put = new Put(Bytes.toBytes(String.valueOf(pojo.getClass().getDeclaredMethod("getBatchID", null).invoke(pojo, null)) + UUID.randomUUID()));

            //依据rowKey创建put对象
            Class<?> pojoClass = pojo.getClass();

            Field[] fields = pojoClass.getDeclaredFields();
            for (Field field : fields) {
                if (field.getGenericType().getTypeName() == Integer.class.getTypeName() || field.getGenericType().getTypeName() == String.class.getTypeName()) {
                    //String fieldNameLower = field.getName().toLowerCase();
                    //添加数据
                    put.addColumn(Bytes.toBytes(cf[0]), Bytes.toBytes(field.getName()), Bytes.toBytes(String.valueOf(pojoClass.getMethod("get" + field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1), null).invoke(pojo))));
                }
            }

            Method getMetadata = pojoClass.getDeclaredMethod("getMetadata", null);
            Object o = getMetadata.invoke(pojo, null);
            Class<?> metedataClass = o.getClass();
            Field[] metedataClassDeclaredFields = metedataClass.getDeclaredFields();
            for (Field field : metedataClassDeclaredFields) {
                put.addColumn(Bytes.toBytes(cf[1]), Bytes.toBytes(field.getName()), Bytes.toBytes(String.valueOf(metedataClass.getMethod("get" + field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1), null).invoke(o))));
            }

            puts.add(put);
        }

        //执行操作
        table.put(puts);


    }


    public static void putRealSyncDataBatch(String tableName, LinkedList<Object> pojos, String... cf) throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {

        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //用作批量提交
        ArrayList<Put> puts = new ArrayList<>();
        //存儲rowkey字段
        Map<String, Object> rowkeyMap = new HashMap<String, Object>();
        for (Object pojo : pojos) {
            Class<?> pojoClass = pojo.getClass();
            //根据databasename+tableName生成rowkey
            Field[] fields = pojoClass.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
                if (declaredAnnotations.length > 0) {
                    rowkeyMap.put(declaredAnnotations[0].annotationType().getSimpleName(), field.get(pojo));
                }
            }
            /* Object tableName1 = pojoClass.getDeclaredMethod("getTableName", null).invoke(pojo, null);
           Object databasename = pojoClass.getDeclaredMethod("getDatabasename", null).invoke(pojo, null);
           Object owner = pojoClass.getDeclaredMethod("getOwner", null).invoke(pojo, null);
           long ts = (long) (pojoClass.getDeclaredMethod("getTimestamp", null).invoke(pojo, null));
           //获取操作类型
           Object operation_type = pojoClass.getDeclaredMethod("getOperation_type", null).invoke(pojo, null);

            Method getColumnListInfo = pojoClass.getDeclaredMethod("getColumnListInfo", null);*/
            Object tableName1 = rowkeyMap.get(String.valueOf(RowKey.TableName));
            Object databasename = rowkeyMap.get(String.valueOf(RowKey.DataBaseName));
            Object owner = rowkeyMap.get(String.valueOf(RowKey.Owner));
            long ts = (long) rowkeyMap.get(String.valueOf(RowKey.TimeStamp));
            //获取操作类型
            Object operation_type = rowkeyMap.get(String.valueOf(RowKey.Operation_Type));
            Object o = rowkeyMap.get(String.valueOf(RowKey.ColumnList));

            Put put = null;
            byte[] rowKey = null;
            HashMap<String, String> map = new HashMap<String, String>();

            for (Kafka_RealSync_Column column : ((List<Kafka_RealSync_Column>) o)) {
                if (column.getDataColNum() != -1) {
                    if ("ID".equals(column.getColumnName())) {
                        //修改Rowkey
                        rowKey = Bytes.toBytes(column.getColumnValue() + "-" + String.valueOf(databasename == null ? "default" : databasename) + "-" + String.valueOf(owner) + "-" + String.valueOf(tableName1));
                        put = new Put(rowKey);
                       /* f = put.getClass().getDeclaredField("row");
                        f.setAccessible(true);
                        f.set(put, Bytes.toBytes(Bytes.toString((byte[]) f.get(put)) + column.getColumnValue()));*/
                    }
                    if (!map.containsKey(column.getColumnName()))
                        map.put(column.getColumnName(), column.getColumnValue());
                }
            }
            for (Map.Entry<String, String> entry : map.entrySet()) {
                put.addColumn(Bytes.toBytes(cf[0]), Bytes.toBytes(entry.getKey()), ts, Bytes.toBytes(entry.getValue()));
            }


            switch (String.valueOf(operation_type)) {

                case "D":
                    //删除数据
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        HbaseUtils.deleteData(tableName, Bytes.toString(rowKey), cf[0], entry.getKey(), ts);
                    }
                default:
                    puts.add(put);
            }

        }

        //执行操作
        if (puts.size() > 0)
            table.put(puts);
    }

    //删除
    public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));

        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));

        table.delete(delete);

        table.close();

    }

    //删除
    public static void deleteData(String tableName, String rowKey, String cf, String cn, long ts) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));

        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn), ts);

        table.delete(delete);

        table.close();

    }

    //查询数据

    /**
     * 全表扫描
     *
     * @param tableName 表名
     * @param isRaw
     */
    public static void scanAllTable(String tableName, boolean isRaw) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        scan.setRaw(isRaw);
        scan.setMaxVersions();

        ResultScanner results = table.getScanner(scan);

        for (Result result : results) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell))
                        + "\tCF:" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "\tCN:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "\tVALUE:" + Bytes.toString(CellUtil.cloneValue(cell))
                        + "\tTYPE:" + cell.getTypeByte()
                        + "\tTIMESTAMP:" + cell.getTimestamp()
                );
            }
        }


    }

    /**
     * 获取指定行键、列族：列名的数据
     *
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     */
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));

        if (cn == null) {
            get.addFamily(Bytes.toBytes(cf));
        } else {
            get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        }


        get.setMaxVersions();

        Result result = table.get(get);

        CellScanner scanner = result.cellScanner();
        while (scanner.advance()) {
            Cell cell = scanner.current();
            System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell))
                    + "\tCF:" + Bytes.toString(CellUtil.cloneFamily(cell))
                    + "\tCN:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + "\tVALUE:" + Bytes.toString(CellUtil.cloneValue(cell))
                    + "\tTYPE:" + cell.getTypeByte()
                    + "\tTIMESTAMP:" + cell.getTimestamp());
        }


        System.out.println("########################################");
        Cell[] cells = result.rawCells();

        for (Cell cell : cells) {
            System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell))
                    + "\tCF:" + Bytes.toString(CellUtil.cloneFamily(cell))
                    + "\tCN:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + "\tVALUE:" + Bytes.toString(CellUtil.cloneValue(cell))
                    + "\tTYPE:" + cell.getTypeByte()
                    + "\tTIMESTAMP:" + cell.getTimestamp()
            );
        }

    }

}
