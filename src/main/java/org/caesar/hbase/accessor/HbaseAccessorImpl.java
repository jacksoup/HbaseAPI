package org.caesar.hbase.accessor;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.caesar.hbase.annotation.HbaseField;
import org.caesar.hbase.util.CommonUtil;
import org.caesar.hbase.util.Constant;
import org.caesar.hbase.util.HBasesUtil;
import org.caesar.hbase.util.HbaseAnnontationUtil;
import org.caesar.utils.annoatations.AnnoatationsUtil;
import org.caesar.utils.beans.JackBeanUtil;
import org.caesar.utils.common.ConvertsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Created by caesar.zhu on 15-11-5.
 */
public class HbaseAccessorImpl implements IAccessor {
    private static Logger logger = LoggerFactory.getLogger(HbaseAccessorImpl.class);

    /*创建表*/
    @Override
    public boolean createTable(Class clazz) throws IOException {
        String tableNameStr = HbaseAnnontationUtil.getTableName(clazz);
        String family = HbaseAnnontationUtil.getFamilyName(clazz);
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableNameStr));
        if (hasTable(clazz)) {
            logger.warn("该表已存在");
            return false;
        }
        table.addFamily(new HColumnDescriptor(family));
        HBasesUtil.getAdmin().createTable(table);
        logger.info(tableNameStr + "已创建");
        return true;
    }

    /*删除表*/
    @Override
    public boolean deleteTable(Class clazz) throws IOException {
        Admin admin = HBasesUtil.getAdmin();
        String tableNameStr = HbaseAnnontationUtil.getTableName(clazz);
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableNameStr));
        if (hasTable(clazz)) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
            logger.info(tableNameStr + "删除成功");
            return true;
        } else {
            logger.warn(tableNameStr + "表不存在");
            return false;
        }
    }

    /*检测是否有相应的表*/
    @Override
    public boolean hasTable(Class clazz) throws IOException {
        String tableNameStr = HbaseAnnontationUtil.getTableName(clazz);
        Admin admin = HBasesUtil.getAdmin();
        TableName tableName = TableName.valueOf(tableNameStr);
        if (admin.tableExists(tableName)) {
            return true;
        } else {
            return false;
        }
    }

    /*添加数据*/
    @Override
    public boolean put(Object t) throws IOException {
        String tableNameStr = HbaseAnnontationUtil.getTableName(t.getClass());
        Table table = HBasesUtil.getConnecton().getTable(TableName.valueOf(tableNameStr));
        Object keyValue = HbaseAnnontationUtil.getRowKeyValue(t);
//        Put put = new Put(Bytes.toBytes(keyValue));
        Put put = new Put(ConvertsUtil.convertToBytes(keyValue));
        table.put(AddCell(put, t));
        logger.info(t + "已添加");
        return true;
    }

    /*批量添加数据*/
    @Override
    public <T> boolean putBatch(Class<T> clazz, List<T> list) throws IOException {
        String tableNameStr = HbaseAnnontationUtil.getTableName(clazz);
        Table table = HBasesUtil.getConnecton().getTable(TableName.valueOf(tableNameStr));
        List<Row> batch = new LinkedList<>();
//        List<Put> batch = new LinkedList<Put>();
        for (T t : list) {
            Object keyValue = HbaseAnnontationUtil.getRowKeyValue(t);
            Put put = new Put(ConvertsUtil.convertToBytes(keyValue));
            put = AddCell(put, t);
            batch.add(put);
        }
        Object[] results = new Object[list.size()];//用于存放批量操作结果
        try {
            table.batch(batch, results);
        } catch (Exception e) {
            e.printStackTrace();
        }
//        for (int i = 0; i < results.length; i++) {
//            System.out.println("Result[" + i + "]: " + results[i]);
//        }
        return true;
    }

    /*根据rowkey获得具体的model对象*/
    @Override
    public <T> T get(Object rowkey, Class<T> clazz) throws IOException {
        //查询所有列
        List<String> columns = AnnoatationsUtil.getFieldsBySpecAnnoatationWithString(clazz, HbaseField.class);
        return get(rowkey, clazz, columns);
    }

    /*根据rowkey获得指定列，并将结果转换为具体的model对象*/
    @Override
    public <T> T get(Object rowkey, Class<T> clazz, List<String> columns) throws IOException {
        String tableNameStr = HbaseAnnontationUtil.getTableName(clazz);
        String family = HbaseAnnontationUtil.getFamilyName(clazz);
        Table table = HBasesUtil.getConnecton().getTable(TableName.valueOf(tableNameStr));
//        Get get = new Get(byte[] bts);
        Get get = new Get(ConvertsUtil.convertToBytes(rowkey));
        for (String column : columns) {
            get.addColumn(family.getBytes(), column.getBytes());
        }
        Result result = table.get(get);
        if (result.isEmpty()) return null;
        return ResultToModel(result, clazz);
    }

    /*根据起始位置扫描表，并返回model的list集合*/
    @Override
    public <T> List scan(Object startrowkey, Object stoprowkey, Class<T> clazz) throws IOException {
        //扫描行并获取所有列
        return scan(startrowkey, stoprowkey, clazz);
    }

    /*根据起始位置扫描表，获取指定列，并返回model的list集合*/
    @Override
    public <T> List<T> scan(Object startrowkey, Object stoprowkey, Class<T> clazz, List<String> columns) throws IOException {
        String tableNameStr = HbaseAnnontationUtil.getTableName(clazz);
        Table table = HBasesUtil.getConnecton().getTable(TableName.valueOf(tableNameStr));
        String family = HbaseAnnontationUtil.getFamilyName(clazz);
        Scan scan = new Scan();
        for (String column : columns) {
            scan.addColumn(family.getBytes(), column.getBytes());
        }
//        scan.setStartRow(HbaseAnnontationUtil.convertToBytes(startrowkey));//设置开启位置
//        scan.setStopRow(HbaseAnnontationUtil.convertToBytes(stoprowkey));//设置结束位置
        ResultScanner scanner = table.getScanner(scan);
        List<T> list = new LinkedList<T>();
        for (Result result : scanner) {
            T model = ResultToModel(result, clazz);
            list.add(model);
//            System.out.println("find a record");
        }
        return list;
    }

    /*根据rowkey删除指定行*/
    @Override
    public <T> boolean delete(Object rowkey, Class<T> clazz) throws IOException {
        if (!hasRow(rowkey, clazz)) {
            return false;
        }
        String tableNameStr = HbaseAnnontationUtil.getTableName(clazz);
        Table table = HBasesUtil.getConnecton().getTable(TableName.valueOf(tableNameStr));
        Delete delete = new Delete(ConvertsUtil.convertToBytes(rowkey));
        table.delete(delete);
        return true;
    }

    /*根据rowkey（通过设置list对象中的HbaseRowkey值确定）批量删除指定行*/
    @Override
    public <T> boolean deleteBatch(Class<T> clazz, List<T> list) throws IOException {
        String tableNameStr = HbaseAnnontationUtil.getTableName(clazz);
        Table table = HBasesUtil.getConnecton().getTable(TableName.valueOf(tableNameStr));
        List<Row> batch = new LinkedList<Row>();
//        List<Delete> batch = new LinkedList<Delete>();
        for (T t : list) {
            Object keyValue = HbaseAnnontationUtil.getRowKeyValue(t);
            if (!hasRow(keyValue, clazz)) {
                continue;
            }
            Delete delete = new Delete(ConvertsUtil.convertToBytes(keyValue));
            batch.add(delete);
        }
        Object[] results = new Object[list.size()];
        try {
            table.batch(batch, results);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /*根据rowkey判断是否含有某条数据*/
    @Override
    public boolean hasRow(Object rowkey, Class clazz) throws IOException {
        String tableNameStr = HbaseAnnontationUtil.getTableName(clazz);
        Table table = HBasesUtil.getConnecton().getTable(TableName.valueOf(tableNameStr));
        Get get = new Get(ConvertsUtil.convertToBytes(rowkey));
        Result result = table.get(get);
        if (result.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }

    /*修改hbase表的Schema（暂时未用）*/
    public static void modifySchema() throws IOException {

        Admin admin = HBasesUtil.getAdmin();
        TableName tableName = TableName.valueOf("TABLE_NAME");
        if (admin.tableExists(tableName)) {
            logger.warn("Table does not exist.");
            System.exit(-1);
        }

        HTableDescriptor table = new HTableDescriptor(tableName);

        // Update existing table
        HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
        newColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
        newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
        admin.addColumn(tableName, newColumn);

        // Update existing column family
        HColumnDescriptor existingColumn = new HColumnDescriptor("CF_DEFAULT");
        existingColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
        existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
        table.modifyFamily(existingColumn);
        admin.modifyTable(tableName, table);

        // Disable an existing table
        admin.disableTable(tableName);

        // Delete an existing column family
        admin.deleteColumn(tableName, "CF_DEFAULT".getBytes("UTF-8"));

        // Delete a table (Need to be disabled first)
        admin.deleteTable(tableName);

    }

    /*解析model并向put对象添加字段值*/
    private static Put AddCell(Put put, Object model) {
        String family = HbaseAnnontationUtil.getFamilyName(model.getClass());
        /**field字段为基本类型*/
//        List<String> fields = AnnoatationsUtil.getFieldsBySpecAnnoatationWithString(model.getClass(), HbaseField.class);
//        for (String fieldName : fields) {
//            Object value = JackBeanUtil.getProperty(model, fieldName);
//            put.addColumn(family.getBytes(), fieldName.getBytes(), ConvertsUtil.convertToBytes(value));
//        }
        /**field字段为基本类型和集合类型的处理*/
        List<Field> fields = AnnoatationsUtil.getFieldsBySpecAnnoatation(model.getClass(), HbaseField.class);
        for (Field filed : fields) {
            Object value = JackBeanUtil.getProperty(model, filed.getName());
            if(value == null) continue;
            String type = filed.getGenericType().toString();
            //List转换
            if(type.startsWith("java.util.List")){//字段类型为List
                String newValue = "";
                List list = (List)value;
                for(Object o:list){
                    String oo = CommonUtil.filterSpecChar(o.toString());
                    newValue += oo + Constant.COLLECTION_SPLIT_CH;
                }
                if(list.size()==0){
                    newValue = "";
                }else{
                    newValue = newValue.substring(0,newValue.length()-1);
                }
                value = newValue;
            }
            //Set转换
            if(type.startsWith("java.util.Set")){//字段类型为Set
                String newValue = "";
                Set set = (Set)value;
                for(Object o:set){
                    String oo = CommonUtil.filterSpecChar(o.toString());
                    newValue += oo + Constant.COLLECTION_SPLIT_CH;
                }
                if(set.size()==0){
                    newValue = "";
                }else{
                    newValue = newValue.substring(0,newValue.length()-1);
                }
                value = newValue;
            }
            //Map转换
            if(type.startsWith("java.util.Map")){//字段类型为Set
                String newValue = "";
                Map map = (Map)value;
                for(Object key:map.keySet()){
                    String keyStr = CommonUtil.filterSpecChar(key.toString());
                    String valueStr = CommonUtil.filterSpecChar(map.get(key).toString());
                    newValue += keyStr + Constant.MAP_KV_CH + valueStr + Constant.COLLECTION_SPLIT_CH;
                }
                if(map.size()==0){
                    newValue = "";
                }else{
                    newValue = newValue.substring(0,newValue.length()-1);
                }
                value = newValue;
            }
            put.addColumn(family.getBytes(), filed.getName().getBytes(), ConvertsUtil.convertToBytes(value));
        }
        return put;
    }

    /*将Hbase返回的Result对象解析为具体的model对象*/
    private static <T> T ResultToModel(Result result, Class<T> clazz) {
        String family = HbaseAnnontationUtil.getFamilyName(clazz);
        T model = JackBeanUtil.getInstanceByClass(clazz);
        List<Field> fields = AnnoatationsUtil.getFieldsBySpecAnnoatation(model.getClass(), HbaseField.class);
        for (Field field : fields) {
            String fieldName = field.getName();
            byte[] b = result.getValue(family.getBytes(), fieldName.getBytes());
            Object value = null;
            Type type = field.getGenericType();
            if(type.toString().startsWith("java.util.List")) {//字段类型为List
                value = ConvertsUtil.bytesToValue(String.class, b);
                if(value ==null) continue;
                List list = Arrays.asList(value.toString().split(Constant.COLLECTION_SPLIT_CH));
                value = list;
            }else if(type.toString().startsWith("java.util.Set")) {//字段类型为List
                value = ConvertsUtil.bytesToValue(String.class, b);
                if(value ==null) continue;
                Set set = new HashSet();
                for(Object o : value.toString().split(Constant.COLLECTION_SPLIT_CH)){
                    set.add(o);
                }
                value = set;
            }else if(type.toString().startsWith("java.util.Map")) {//字段类型为Map
                value = ConvertsUtil.bytesToValue(String.class, b);
                if(value ==null) continue;
                Map map = new HashMap();
                for(Object o:value.toString().split(Constant.COLLECTION_SPLIT_CH)){
                    Object[] arr = o.toString().split(Constant.MAP_KV_CH);
                    map.put(arr[0],arr[1]);
                }
                value = map;
            }else{
                value = ConvertsUtil.bytesToValue(type, b);
            }
            JackBeanUtil.setProperty(model, fieldName, value);
        }
        return model;
    }

}
