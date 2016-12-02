package org.caesar.hbase.accessor;

import java.io.IOException;
import java.util.List;

public interface IAccessor {
    <T>boolean createTable(Class<T> clazz)  throws IOException;
    <T>boolean deleteTable(Class<T> clazz)  throws IOException ;
    <T>boolean hasTable(Class<T> clazz)  throws IOException ;
    <T>boolean put(T t)  throws IOException ;
    <T> boolean putBatch(Class<T> clazz, List<T> list) throws IOException;
    <T> T get(Object rowkey, Class<T> clazz)  throws IOException ;
    <T> T get(Object rowkey, Class<T> clazz, List<String> columns)  throws IOException ;
    <T> List<T> scan(Object startrowkey, Object stoprowkey, Class<T> clazz)  throws IOException ;
    <T> List<T> scan(Object startrowkey, Object stoprowkey, Class<T> clazz, List<String> columns)  throws IOException ;
    <T> boolean delete(Object rowkey, Class<T> clazz)  throws IOException ;
    <T> boolean deleteBatch(Class<T> clazz, List<T> list) throws IOException;
    <T> boolean hasRow(Object rowkey, Class<T> clazz)  throws IOException ;



}
