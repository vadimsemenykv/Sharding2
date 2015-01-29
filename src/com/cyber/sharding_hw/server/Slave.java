package com.cyber.sharding_hw.server;

import com.cyber.sharding_hw.request_response.Command;
import com.cyber.sharding_hw.request_response.Request;
import com.cyber.sharding_hw.request_response.Response;
import com.cyber.sharding_hw.request_response.Status;
import com.cyber.sharding_hw.server.jms_propertys_data.Direction;
import com.cyber.sharding_hw.server.jms_propertys_data.MsgTitle;
import com.cyber.sharding_hw.server.jms_propertys_data.PropertyName;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by Vadim on 13.01.2015.
 */
public class Slave implements JMSMessageProcessor{
    private final String ip;
    private final int port;
    private final int maxConnections;
    private int maxShardSize;
    private ExecutorService connectionExecutor;
    private Semaphore semaphore;
    private ServerSocket serverSocket;
    private JMSworker jmsWorker;
    private Thread slaveHeartBeat;
    private Map<Integer, Object> objectMap;

    public Slave(String hostName, int port, int maxConnections, int maxShardSize, String addressJMSServer,
                 String jmsTopicName, String jmsBrockerUserName, String jmsBrockerPass) {
        this.ip = hostName;
        this.port = port;
        this.maxConnections = maxConnections;
        this.maxShardSize = maxShardSize;
        this.connectionExecutor = Executors.newFixedThreadPool(maxConnections);
        this.semaphore = new Semaphore(maxConnections);
        this.objectMap = new ConcurrentHashMap<>();

        this.jmsWorker = new JMSworker(addressJMSServer, jmsTopicName, jmsBrockerUserName, jmsBrockerPass, this);
        loadDataFromPersistStorage();
        startSlaveHeartBeat();
    }

    private void startSlaveHeartBeat() {
        slaveHeartBeat = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverSocket = new ServerSocket(port);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                while (!Thread.currentThread().isInterrupted() && !serverSocket.isClosed()) {
                    try {
                        Socket socket = serverSocket.accept();
                        SlaveConnection masterConnection = new SlaveConnection(socket);
                        connectionExecutor.submit(masterConnection);
                    } catch (SocketException e) {
                    } catch (IOException e) {
                    }
                }
            }
        });
        slaveHeartBeat.start();
    }

    @Override
    public void processJMSMessage(ObjectMessage msg) {
        try {
            String direction = (msg.getStringProperty(PropertyName.DIRECTION));
            if(direction.equals(Direction.TO_SHARD)) {
                String title = msg.getStringProperty(PropertyName.MSG_TITLE);
                switch (title) {
                    case MsgTitle.STATUS:
                        jmsWorker.sendMessage(Direction.TO_MASTER, MsgTitle.STATUS, getStatus());
                        break;
                    case MsgTitle.REBALANCE:
                        rebalance((HashMap<Integer, MetaSlave>) msg.getObject());
                        break;
                    case MsgTitle.STOP:
                        jmsWorker.sendMessage(Direction.TO_MASTER, MsgTitle.STOPPED, stop());
                        new Thread(new JMSworkerStopper()).start();
                        break;
                    default:
                        break;
                }
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public String getStatus() {
        String keys = objectMap.keySet().toString();
        return "Shard host ip = " + ip + " port = " + port + "\n" +
                " max available connections to shard = " + maxConnections + "\n" +
                " num of objects saved at shard = " + objectMap.size() + "\n" +
                " key of objects saved at shard:\n" + " " + keys;
    }

    private void rebalance(HashMap<Integer, MetaSlave> map) {
        Collection<Future<Response>> futures = new LinkedList<>();
        Collection<SlaveRebalancer> slaveRebalancers = new LinkedList<>();

        for (Map.Entry<Integer, Object> entry : objectMap.entrySet()) {
            int rule = entry.getKey() % map.size();
            MetaSlave metaSlave = map.get(rule);
            if (!(metaSlave.getIp().equals(ip)) && !(metaSlave.getPort() == port)) {
                slaveRebalancers.add(new SlaveRebalancer(metaSlave.getIp(), metaSlave.getPort(), entry.getKey()));
            }
        }

        try {
            ExecutorService balancer = Executors.newFixedThreadPool(maxConnections);
            futures = balancer.invokeAll(slaveRebalancers);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        for (Future<Response> f : futures) {
            try {
                Response response = f.get();
                if (response.getStatus() == Status.CREATED) objectMap.remove(response.getKey());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        jmsWorker.sendMessage(Direction.TO_MASTER, MsgTitle.REBALANCED);
    }

    public String stop() {
        try {
            serverSocket.close();
            connectionExecutor.shutdown();
            connectionExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            saveDataInPersistStorage();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return "slave " + ip + ":" + port + " stopped";
    }

    private void saveDataInPersistStorage() {
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(new File(String.valueOf(port))))) {
            objectOutputStream.writeObject(objectMap);
            objectOutputStream.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadDataFromPersistStorage() {
        File storage = new File(String.valueOf(port));
        if (storage.exists()) {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(new File(String.valueOf(port))))) {
                objectMap = (ConcurrentHashMap) objectInputStream.readObject();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private class SlaveConnection implements Runnable {
        private Socket socket;

        private SlaveConnection(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream())) {

                semaphore.acquire();
                Request request = (Request) objectInputStream.readObject();

                Status status = Status.ABORTED;
                Response response = null;
                switch (request.getCommand()) {
                    case CREATE:
                        status = create(request.getKey(), request.getItem());
                        response = new Response(request.getKey(), request.getCommand(), status);
                        break;
                    case READ:
                        Object obj = read(request.getKey());
                        if (obj != null) status = Status.READ;
                        response = new Response(request.getKey(), obj, status);
                        break;
                    case UPDATE:
                        status = update(request.getKey(), request.getItem());
                        response = new Response(request.getKey(), request.getCommand(), status);
                        break;
                    case DELETE:
                        status = delete(request.getKey());
                        response = new Response(request.getKey(), request.getCommand(), status);
                        break;
                    default:
                        break;
                }

                objectOutputStream.writeObject(response);
                objectOutputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }  catch (ClassCastException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (objectMap.size() >= maxShardSize) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("WARNING!!! ");
                stringBuilder.append("Size of shard ");
                stringBuilder.append(ip);
                stringBuilder.append(":");
                stringBuilder.append(port);
                stringBuilder.append(" is reached his max capacity");

                jmsWorker.sendMessage(Direction.TO_MASTER, MsgTitle.REBALANCE_REQUIRED, stringBuilder.toString());
            }
            semaphore.release();
        }

        private Status create(final int key, Object item) {
            return objectMap.put(key, item) == null ? Status.CREATED : Status.UPDATED;
        }

        private Object read(int key) {
            return objectMap.get(key);
        }

        private Status update(int key, Object item) {
            return objectMap.put(key, item) == null ? Status.CREATED : Status.UPDATED;
        }

        private Status delete(int key) {
            return objectMap.remove(key) == null ? Status.ABORTED : Status.DELETED;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SlaveConnection)) return false;

            SlaveConnection that = (SlaveConnection) o;

            if (!socket.equals(that.socket)) return false;

            return true;
        }
    }

    private class SlaveRebalancer implements Callable<Response> {
        String dstIp;
        int dstPort;
        int key;

        private SlaveRebalancer(String dstIp, int dstPort, int key) {
            this.dstIp = dstIp;
            this.dstPort = dstPort;
            this.key = key;
        }

        @Override
        public Response call() throws Exception {
            try (Socket socketToShard = new Socket(dstIp, dstPort);
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketToShard.getOutputStream());
                 ObjectInputStream objectInputStream = new ObjectInputStream(socketToShard.getInputStream())) {

                Request request = new Request(key, objectMap.get(key), Command.CREATE);
                objectOutputStream.writeObject(request);
                objectOutputStream.flush();

                return (Response) objectInputStream.readObject();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (ClassCastException e) {
            }
            return null;
        }
    }

    private class JMSworkerStopper implements Runnable {
        @Override
        public void run() {
            jmsWorker.stop();
        }
    }
}
