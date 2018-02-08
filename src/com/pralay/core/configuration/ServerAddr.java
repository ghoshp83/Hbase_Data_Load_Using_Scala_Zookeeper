package com.pralay.core.configuration;

import java.util.ArrayList;
import java.util.List;

/**
 *
 *
 */
public class ServerAddr {

    private final String host;
    private final int port;

    public ServerAddr(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public static ServerAddr fromString(String addr) {
        String[] data = addr.split(":");
        if (data.length < 2)
            throw new RuntimeException("Invalid server addr: " + addr);
        int port = 0;
        try {
            port = Integer.parseInt(data[1]);
        } catch (NumberFormatException ex) {
            throw new RuntimeException("invalid port number: " + data[1]);
        }
        return new ServerAddr(data[0], port);
    }

    public static List<ServerAddr> fromMultiString(String addrs) {
        List<ServerAddr> list = new ArrayList<ServerAddr>();
        for (String addr:addrs.split(",")) {
            list.add(fromString(addr));
        }
        return list;
    }

        public static List<ServerAddr> fromMultiString(String addrs, int port) {
                List<ServerAddr> list = new ArrayList<ServerAddr>();
                for (String addr:addrs.split(",")) {
                        list.add(new ServerAddr(addr, port));
                }
                return list;
        }


    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof ServerAddr))
            return false;
        ServerAddr addr = (ServerAddr)obj;
        return (host.equals(addr.getHost()) && port == addr.getPort());
    }

    @Override
    public int hashCode() {
        return (host + ":" + port).hashCode();
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }



}
