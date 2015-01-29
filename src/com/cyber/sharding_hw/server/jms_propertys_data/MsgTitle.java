package com.cyber.sharding_hw.server.jms_propertys_data;

import java.io.Serializable;

/**
 * Created by Vadim on 29.01.2015.
 */
public class MsgTitle implements Serializable {
    public static final String STATUS = "STATUS";
    public static final String REBALANCE = "REBALANCE";
    public static final String STOP = "STOP";
    public static final String REBALANCE_REQUIRED = "REBALANCE REQUIRED";
    public static final String REBALANCED = "REBALANCED";
    public static final String STOPPED = "STOPPED";
}
