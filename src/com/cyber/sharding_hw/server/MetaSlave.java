package com.cyber.sharding_hw.server;

import java.io.Serializable;

/**
 * Created by Vadim on 27.01.2015.
 */
public class MetaSlave implements Serializable{
    private String ip;
    private int port;

    public MetaSlave(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }
}
