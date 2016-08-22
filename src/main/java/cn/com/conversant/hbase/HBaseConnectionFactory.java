package cn.com.conversant.hbase;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Copyright (c) 2016 Conversant Solutions. All rights reserved.
 * Created with HbaseTest
 * User:  Lily
 * Date:  2016/8/18
 * Time:  18:28
 * <p/>
 * To change this template use File | Settings | File Templates.
 */
public class HBaseConnectionFactory<Connection> implements PooledObjectFactory<Connection> {

    private Configuration configuration;
    private User user;

    private ReentrantLock reentrantLock = new ReentrantLock();

    public HBaseConnectionFactory(String zookeeperQuorum, int clientPort, String userName, String[] groups) {
        initConfiguration(zookeeperQuorum, clientPort, userName, groups);


    }

    private void initConfiguration(String zookeeperQuorum, int clientPort, String userName, String[] groups) {

        reentrantLock.lock();
        try {
            if (configuration == null) {
                configuration = new Configuration();
            }
            configuration.set(ConnectionConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
            configuration.set(ConnectionConstants.ZOOKEEPER_CLIENT_PORT, clientPort + "");
            user = User.createUserForTesting(configuration, userName, groups);
        } finally {
            reentrantLock.unlock();
        }

    }


    @Override
    public PooledObject<Connection> makeObject() throws Exception {
        org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(this.configuration, user);
        return new DefaultPooledObject(connection);
    }

    @Override
    public void destroyObject(PooledObject<Connection> pooledObject) throws Exception {
        ClusterConnection connection = (ClusterConnection) pooledObject.getObject();
        try {
            connection.close();
        } catch (Exception e) {

        }

    }

    @Override
    public boolean validateObject(PooledObject<Connection> pooledObject) {
        return true;
    }

    @Override
    public void activateObject(PooledObject<Connection> pooledObject) throws Exception {
        //todo:

    }

    @Override
    public void passivateObject(PooledObject<Connection> pooledObject) throws Exception {
        ////todo:
    }
}
