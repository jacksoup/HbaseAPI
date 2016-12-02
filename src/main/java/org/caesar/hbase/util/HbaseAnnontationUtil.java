package org.caesar.hbase.util;

import org.caesar.hbase.annotation.HbaseDocument;
import org.caesar.hbase.annotation.HbaseRowKey;
import org.caesar.utils.annoatations.AnnoatationsUtil;

/**
 * Created by caesar.zhu on 15-11-4.
 */
public class HbaseAnnontationUtil {

    /*获取表名*/
    public static String getTableName(Class<?> clazz){
        return clazz.getAnnotation(HbaseDocument.class).table();
    }

    /*获取列簇名*/
    public static String getFamilyName(Class<?> clazz){
        return clazz.getAnnotation(HbaseDocument.class).family();
    }

    /*获得model的HbaseRowKey字段名*/
    public static String getRowKeyName(Class clazz) {
        return AnnoatationsUtil.getFieldNameByFieldAnnoatation(clazz,HbaseRowKey.class);
    }

    /*获得model的HbaseRowKey的值*/
    public static <T> Object getRowKeyValue(T model){
        return AnnoatationsUtil.getFieldValueByFieldAnnonation(model,HbaseRowKey.class);
    }

}
