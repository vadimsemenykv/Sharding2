package com.cyber.sharding_hw.server.jms_propertys_data;

import java.io.Serializable;

/**
 * Created by Vadim on 29.01.2015.
 */
public class Direction implements Serializable {
    public static final String TO_MASTER = "TO MASTER";
    public static final String TO_SHARD = "TO SHARD";
}
