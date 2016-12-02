# enhanceHbase ( Java版，增强型Hbase API)
##特点:
*   基于Java，封装了hbase的底层api，提供了基于注解的ORM支持，只需定义实体类对象，即可完成对hbase的各种操作。同时对List、Set、Map等复杂数据类型提供了支持。

##使用说明：
*   1.下载
*   2.修改配置文件参考：
    *  (1).resources/hbase-site.xml:
```Java
   <?xml version="1.0"?>
   <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
   <configuration>
        <property>
             <name>hbase.zookeeper.quorum</name>
            <!--zookeeper 地址-->
            <value>namenode1,namenode2,datanode1,datanode2,datanode3</value>
        </property>
   </configuration>
```
*   3.定义实体类，参考如下：
```Java
   //定义表名和列簇名（目前只支持单个表对应一个列簇）
   @HbaseDocument(table = "my-table-user",family = "col_0")
   public class UserModel {
       @HbaseRowKey
       @HbaseField
       private Long userId;
       @HbaseField
       private String userName;
       @HbaseField
       private Integer age;
       @HbaseField
       private Map<String,Integer> scoresMap;
       @HbaseField
       private List<String> hobiesList;
       @HbaseField
       private Double schooling;
       @HbaseField
       private Set<Integer> years;
       @HbaseField
       private Boolean end;

       getter and setter（省略）
```
*   4.访问hbase：
```Java
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
```
