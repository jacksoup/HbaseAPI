package org.caesar.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by caesar.zhu on 2016/11/24.
 */
public class HBasesUtil {
    private static Configuration config = null;
    private static Connection connection = null;

    static {
        config = HBaseConfiguration.create();
        config.addResource(new Path("hbase-site.xml"));
//        config.addResource(new Path("core-site.xml"));
    }

    /*获得Connection连接*/
    public static Connection getConnecton(){
        if(connection == null || connection.isClosed()){
            try {
                connection = ConnectionFactory.createConnection(config);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static Admin getAdmin(){
        Admin admin = null;
        try {
            admin = getConnecton().getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return admin;
    }


    public static void main(String[] args) throws IOException {
        System.out.println(getAdmin().getClusterStatus());
    }
}
