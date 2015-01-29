package com.cyber.sharding_hw.server;

import java.util.Scanner;

/**
 * Created by Vadim on 05.01.2015.
 */
public class Server {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("To start Master put \"M\"\n" +
                "To start Slave put \"S\"");
        String str = scanner.nextLine();

        if (str.toLowerCase().equals("m")) {
            System.out.println("Set server ip address:");
            String ip = scanner.nextLine();
            System.out.println("Set server port:");
            int port = Integer.parseInt(scanner.nextLine());
            System.out.println("Set connection pool size:");
            int maxConnections = Integer.parseInt(scanner.nextLine());
            System.out.println("Set JMS IP address in format \"127.0.0.1:7676\":");
            String jmsIp = scanner.nextLine();
            System.out.println("Set JMS Topic name:");
            String topicName = scanner.nextLine();
            System.out.println("Set JMS broker username:");
            String user = scanner.nextLine();
            System.out.println("Set JMS broker password:");
            String pass = scanner.nextLine();

            Master master = new Master(ip, port, maxConnections, jmsIp, topicName, user, pass);
//            Master master = new Master("127.0.0.1", 3366, 10, "127.0.0.1:7676", "TestTopic", "admin", "admin");

            System.out.println("To get Server status put \"status\"\n" +
                    "To add shard put \"add shard\"\n" +
                    "To stop server put \"stop\"\n" +
                    "WARNING!!! For correct work necessary to add at least one shard!!!");

            while (!str.toLowerCase().equals("stop")) {
                str = scanner.nextLine();
                if (str.toLowerCase().equals("add shard")) {
                    System.out.println("Set shard ip address:");
                    String shardIp = scanner.nextLine();
                    System.out.println("Set shard port:");
                    int shardPort = Integer.parseInt(scanner.nextLine());

                    master.addShard(shardIp, shardPort);
//                    master.addShard("127.0.0.1", port);
                }
                if (str.toLowerCase().equals("status")) master.getStatus();
                if (str.toLowerCase().equals("stop")) master.stop();
            }
        }
        if (str.toLowerCase().equals("s")) {
            System.out.println("Set server ip address:");
            String ip = scanner.nextLine();
            System.out.println("Set server port:");
            int port = Integer.parseInt(scanner.nextLine());
            System.out.println("Set connection pool size:");
            int maxConnections = Integer.parseInt(scanner.nextLine());
            System.out.println("Set max shard size:");
            int maxShardSize = Integer.parseInt(scanner.nextLine());
            System.out.println("Set JMS IP address in format \"127.0.0.1:7676\":");
            String jmsIp = scanner.nextLine();
            System.out.println("Set JMS Topic name:");
            String topicName = scanner.nextLine();
            System.out.println("Set JMS broker username:");
            String user = scanner.nextLine();
            System.out.println("Set JMS broker password:");
            String pass = scanner.nextLine();

            Slave slave = new Slave(ip, port, maxConnections, maxShardSize, jmsIp, topicName, user, pass);
//            Slave slave = new Slave("127.0.0.1", port, 10, 5, "127.0.0.1:7676", "TestTopic", "admin", "admin");

            System.out.println("To get Server status put \"status\"\n" +
                    "To stop server put \"stop\"\n");

            while (!str.toLowerCase().equals("stop")) {
                str = scanner.nextLine();
                if (str.toLowerCase().equals("stop")) slave.stop();
                if (str.toLowerCase().equals("status")) System.out.println(slave.getStatus());
            }
        }

        scanner.close();
    }
}
