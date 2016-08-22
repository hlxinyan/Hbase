package cn.com.conversant.hbase;

/**
 * Copyright (c) 2016 Conversant Solutions. All rights reserved.
 * Created with HbaseTest
 * User:  Lily
 * Date:  2016/8/19
 * Time:  15:55
 * <p/>
 * To change this template use File | Settings | File Templates.
 */
public class HBaseConnectionException extends RuntimeException {

    public HBaseConnectionException(String message) {
        super(message);
    }

    public HBaseConnectionException(Throwable e) {
        super(e);
    }

    public HBaseConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
