package com.cyber.sharding_hw.server;

import com.cyber.sharding_hw.request_response.MetaRequest;
import com.cyber.sharding_hw.request_response.MetaResponse;
import com.cyber.sharding_hw.server.jms_propertys_data.Direction;
import com.cyber.sharding_hw.server.jms_propertys_data.MsgTitle;
import com.cyber.sharding_hw.server.jms_propertys_data.PropertyName;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.jms.*;

/**
 * Created by Vadim on 14.01.2015.
 */
public class Master implements JMSMessageProcessor{
    private final String ipMaster;
    private final int portMaster;
    private final int maxConnections;
    private ExecutorService connectionExecutor;
    private CountDownLatch latch;
    private Lock heartBeatLock;
    private ServerSocket serverSocket;
    private List<MetaSlave> metaSlaveList;
    private JMSworker jmsWorker;
    private Thread masterHeartBeat;



    public Master(String ipMaster, int portMaster, int maxConnections, String addressJMSServer,
                  String jmsTopicName, String jmsBrockerUserName, String jmsBrockerPass) {
        this.ipMaster = ipMaster;
        this.portMaster = portMaster;
        this.maxConnections = maxConnections;
        startExecutor();
        this.metaSlaveList = new CopyOnWriteArrayList<MetaSlave>();
        this.heartBeatLock = new ReentrantLock();
        heartBeatLock.lock();

        this.jmsWorker = new JMSworker(addressJMSServer, jmsTopicName, jmsBrockerUserName, jmsBrockerPass, this);
        startMasterHeartBeat();
    }

    private void startMasterHeartBeat() {
        masterHeartBeat = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverSocket = new ServerSocket(portMaster);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                while (!Thread.currentThread().isInterrupted() && !serverSocket.isClosed()) {
//                    heartBeatLock.lock();
                    try {
//                        heartBeatLock.unlock();
                        Socket socket = serverSocket.accept();
                        MasterConnection masterConnection = new MasterConnection(socket);
                        connectionExecutor.submit(masterConnection);
                    } catch (SocketException e) {
                    } catch (IOException e) {
                    }
                }
            }
        });
        masterHeartBeat.start();
    }

    @Override
    public void processJMSMessage(ObjectMessage msg) {
        try {
            String direction = (msg.getStringProperty(PropertyName.DIRECTION));
            if(direction.equals(Direction.TO_MASTER)) {
                String title = msg.getStringProperty(PropertyName.MSG_TITLE);
                switch (title) {
                    case MsgTitle.STATUS:
                        System.out.println(msg.getStringProperty(PropertyName.MSG_DATA));
                        break;
                    case MsgTitle.REBALANCE_REQUIRED:
                        System.out.println(msg.getStringProperty(PropertyName.MSG_DATA));
                        break;
                    case MsgTitle.REBALANCED:
                        latch.countDown();
                        break;
                    case MsgTitle.STOPPED:
                        System.out.println(msg.getStringProperty(PropertyName.MSG_DATA));
                        latch.countDown();
                        break;
                    default:
                        break;
                }
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void getStatus() {
        jmsWorker.sendMessage(Direction.TO_SHARD, MsgTitle.STATUS);
    }

    public boolean addShard(String ip, int port) {
        heartBeatLock.lock();
        shutdownExecutor();
        metaSlaveList.add(new MetaSlave(ip, port));
        latch = new CountDownLatch(metaSlaveList.size());
        rebalance();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        startExecutor();
        heartBeatLock.unlock();
        if (metaSlaveList.size() == 1) heartBeatLock.unlock();
        System.out.println("shard added");
        return true;
    }

    private void rebalance() {
        HashMap<Integer, MetaSlave> map = new HashMap<>();
        for (int i = 0; i < metaSlaveList.size(); i++) {
            map.put(i, metaSlaveList.get(i));
        }
        jmsWorker.sendMessage(Direction.TO_SHARD, MsgTitle.REBALANCE, map);
    }

    public boolean stop() {
        try {
            if (metaSlaveList.size() == 0) heartBeatLock.unlock();
            serverSocket.close();
            shutdownExecutor();

            jmsWorker.sendMessage(Direction.TO_SHARD, MsgTitle.STOP);

            latch = new CountDownLatch(metaSlaveList.size());
            latch.await();
            jmsWorker.stop();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("master stopped");
        return true;
    }

    private void startExecutor() {
        this.connectionExecutor = Executors.newFixedThreadPool(maxConnections);
    }

    private void shutdownExecutor() {
        try {
            connectionExecutor.shutdown();
            connectionExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class MasterConnection implements Runnable {
        private Socket socket;

        private MasterConnection(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream())) {

                MetaRequest metaRequest = (MetaRequest) objectInputStream.readObject();
                int key = metaRequest.getKey();

                int slaveNum = Math.abs(key % metaSlaveList.size());
                MetaSlave sl = metaSlaveList.get(slaveNum);
                MetaResponse metaResponse = new MetaResponse(sl.getIp(), sl.getPort());

                objectOutputStream.writeObject(metaResponse);
                objectOutputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }  catch (ClassCastException e) {
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MasterConnection)) return false;

            MasterConnection that = (MasterConnection) o;

            if (!socket.equals(that.socket)) return false;

            return true;
        }
    }
}
