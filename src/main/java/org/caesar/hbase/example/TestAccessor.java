package org.caesar.hbase.example;

import org.caesar.hbase.accessor.HbaseAccessorImpl;
import org.caesar.hbase.accessor.IAccessor;

import java.io.IOException;
import java.util.*;

/**
 * Created by caesar.zhu on 15-11-5.
 */
public class TestAccessor {

    public static void testHbase() throws IOException {
        //获得访问hbase的accessor对象
        IAccessor accessor = new HbaseAccessorImpl();
        //删除表
        accessor.deleteTable(UserModel.class);
        //创建表
        accessor.createTable(UserModel.class);
        //判断表是否存在
        System.out.println(accessor.hasTable(UserModel.class));
        //new实体类并设置相关属性
        UserModel user = new UserModel();
        user.setUserId(1010001L);
        user.setUserName("caesar.zhu");
        user.setAge(30);
        Map<String,Integer> scoresMap = new HashMap();
        scoresMap.put("history",95);
        scoresMap.put("geography",98);
        scoresMap.put("math",100);
        user.setScoresMap(scoresMap);
        List<String> hobiesList = new ArrayList<>();
        hobiesList.add("basketball");
        hobiesList.add("swimming");
        hobiesList.add("shoot");
        user.setHobiesList(hobiesList);
        Set<Integer> yearsSet = new HashSet<>();
        yearsSet.add(10);
        yearsSet.add(25);
        yearsSet.add(65);
        user.setYears(yearsSet);
        user.setEnd(true);
        //添加记录
        accessor.put(user);
        //根据key进行查询
        UserModel ins = accessor.get(1010001L, UserModel.class);
        System.out.println(ins);
        //根据key判断记录是否存在
        System.out.println(accessor.hasRow(1010001L,UserModel.class));
        //删除指定记录
        System.out.println(accessor.delete(1010001L,UserModel.class));
        //批量删除
        //System.out.println(accessor.deleteBatch(list...,UserModel.class));
        //扫描记录
        //System.out.println(accessor.scan(....));
    }

    public static void main(String[] args) throws IOException {
        testHbase();
    }

}
