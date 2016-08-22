package cn.com.conversant.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Copyright (c) 2016 Conversant Solutions. All rights reserved.
 * Created with HbaseTest
 * User:  Lily
 * Date:  2016/8/19
 * Time:  17:15
 * <p/>
 * To change this template use File | Settings | File Templates.
 */
public class HBaseConnectionPoolTest {

    private static final Logger logger = LoggerFactory.getLogger(HBaseConnectionPoolTest.class);

    private HBaseConnectionPool baseConnectionPool;
    private String TABLE_NAME = "Members";
    private static final String CF_DEFAULT = "meta";

    private ExecutorService executorService= Executors.newFixedThreadPool(6);
    @Test
    public void testGetConnection() throws Exception {
        String[] groups={"conversant"};
        baseConnectionPool = new HBaseConnectionPool<Connection>("192.168.1.25", 2181,"conversant",groups);

        Connection connection = (Connection) baseConnectionPool.getConnection();
        logger.info("connection={}",connection);
        System.out.println("test,test");
        testCreateTable(connection);
        baseConnectionPool.returnConnection(connection);

/*
        for(int i=0;i<20;i++){
            final int io=i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    Connection connection = (Connection) baseConnectionPool.getConnection();
                    logger.info("i={},connection={}",io,connection);
                    System.out.println("test,test");
                    baseConnectionPool.returnConnection(connection);
                }
            });
        }*/

       Thread.sleep(5000000);
    }

    @Test
    public void testReturnConnection() throws Exception {

    }


    public void testCreateTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();

        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Compression.Algorithm.NONE));

        System.out.print("Creating table. ");
        createOrOverwrite(admin, table);
        System.out.println(" Done.");


    }

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }



}