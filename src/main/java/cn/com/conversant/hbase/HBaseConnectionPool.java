package cn.com.conversant.hbase;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Copyright (c) 2016 Conversant Solutions. All rights reserved.
 * Created with HbaseTest
 * User:  Lily
 * Date:  2016/8/17
 * Time:  18:28
 * <p/>
 * To change this template use File | Settings | File Templates.
 */
public class HBaseConnectionPool<Connection> implements Closeable {

    private GenericObjectPool<Connection> pool;


    private String zookeeperQuorum;
    private int clientPort;

    private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
    private int maxActive = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

    private int minIdle = 1;

    private int initializSize=2;

    private String user = "conversant";
    private String[] groups = {"conversant"};




    private ReentrantLock reentrantLock = new ReentrantLock();

    private GenericObjectPoolConfig genericObjectPoolConfig;

    private static final Logger logger= LoggerFactory.getLogger(HBaseConnectionPool.class);


    public HBaseConnectionPool() {

    }

    /**
     * zookeeperQuorum
     *
     * @param zookeeperQuorum
     * @param clientPort
     * @param genericObjectPoolConfig
     */
    public HBaseConnectionPool(String zookeeperQuorum, int clientPort, String user,String[] groups,GenericObjectPoolConfig genericObjectPoolConfig) {

        this.zookeeperQuorum = zookeeperQuorum;
        this.clientPort = clientPort;
        this.genericObjectPoolConfig = genericObjectPoolConfig;
        this.user=user;
        this.groups=groups;
    }

    public HBaseConnectionPool(String zookeeperQuorum, int clientPort,String user,String[] groups) {
        this(zookeeperQuorum, clientPort, user,groups,new GenericObjectPoolConfig());

    }


    protected GenericObjectPool<Connection> createConnectionPool() {
        reentrantLock.lock();

        try {
            if (pool != null) return pool;

            if (genericObjectPoolConfig == null) {
                genericObjectPoolConfig = new GenericObjectPoolConfig();
            }
            pool = new GenericObjectPool<Connection>(new HBaseConnectionFactory(this.getZookeeperQuorum(),this.getClientPort(),this.getUser(),this.getGroups()),
                       genericObjectPoolConfig);

            pool.setMaxTotal(maxActive);
            pool.setMaxIdle(maxIdle);
            pool.setMinIdle(minIdle);

            this.addConnection(initializSize);
            return pool;
        } finally {
            reentrantLock.unlock();
        }


    }


    public Connection getConnection() {
        try {
            return createConnectionPool().borrowObject();
        } catch (NoSuchElementException nse) {
            throw new HBaseConnectionException("Could not get a resource from the pool", nse);
        } catch (Exception e) {
            throw new HBaseConnectionException("Could not get a resource from the pool", e);
        }
    }

    public void returnConnection(final Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            pool.returnObject(conn);
        } catch (Exception e) {
            throw new HBaseConnectionException("Could not return the resource to the pool", e);
        }
    }

    public String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    public void setZookeeperQuorum(String zookeeperQuorum) {
        this.zookeeperQuorum = zookeeperQuorum;
    }

    public int getClientPort() {
        return clientPort;
    }

    public void setClientPort(int clientPort) {
        this.clientPort = clientPort;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public int getMaxActive() {
        return maxActive;
    }

    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    public int getNumActive() {
        if (poolInactive()) {
            return -1;
        }

        return this.pool.getNumActive();
    }

    public int getNumIdle() {
        if (poolInactive()) {
            return -1;
        }

        return this.pool.getNumIdle();
    }

    public int getNumWaiters() {
        if (poolInactive()) {
            return -1;
        }

        return this.pool.getNumWaiters();
    }

    public long getMeanBorrowWaitTimeMillis() {
        if (poolInactive()) {
            return -1;
        }

        return this.pool.getMeanBorrowWaitTimeMillis();
    }

    public long getMaxBorrowWaitTimeMillis() {
        if (poolInactive()) {
            return -1;
        }

        return this.pool.getMaxBorrowWaitTimeMillis();
    }

    public int getInitializSize() {
        return initializSize;
    }

    public void setInitializSize(int initializSize) {
        this.initializSize = initializSize;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String[] getGroups() {
        return groups;
    }

    public void setGroups(String[] groups) {
        this.groups = groups;
    }

    private boolean poolInactive() {
        return this.pool == null || this.pool.isClosed();
    }


    @Override
    public void close() {
        try {
            pool.close();
        } catch (Exception e) {
            throw new HBaseConnectionException("Could not destroy the pool", e);
        }
    }

    public boolean isClosed() {
        return this.pool.isClosed();
    }

    public void addConnection(int count) {
        try {
            for (int i = 0; i < count; i++) {
                pool.addObject();


            }

            logger.info("initialize Size={},idles={}",count,pool.getMinIdle());
        } catch (Exception e) {
            throw new HBaseConnectionException("Error preloading the connection pool", e);
        }
    }


}
