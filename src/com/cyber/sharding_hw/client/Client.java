package com.cyber.sharding_hw.client;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by Vadim on 29.01.2015.
 */
public class Client {
    private String ip;
    private int port;
    private int connectionPoolSize;
    private ClientCRUDhandler clientCRUDhandler;

    public Client(String ip, int port, int connectionPoolSize) {
        this.ip = ip;
        this.port = port;
        this.connectionPoolSize = connectionPoolSize;
        this.clientCRUDhandler = new ClientCRUDhandler(ip, port);
    }

    public boolean createObject(Object o) {
        return clientCRUDhandler.createObj(o);
    }

    public Object readObject(int key) {
        return clientCRUDhandler.readObj(key);
    }

    public boolean updateObject(Object o) {
        return clientCRUDhandler.updateObj(o);
    }

    public boolean deleteObject(int key) {
        return clientCRUDhandler.deleteObj(key);
    }

    public Collection<Boolean> createCollectionOfObjects(Collection<Object> objects) {
        Collection<Boolean> booleans = new LinkedList<>();
        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(connectionPoolSize);

            Collection<CRUDworker<Object, Boolean>> cruDworkers = new LinkedList<>();
            Collection<Future<Boolean>> futures = new LinkedList<>();

            for (Object o : objects) cruDworkers.add(new Creator(o));

            futures = executor.invokeAll(cruDworkers);

            for (Future<Boolean> f : futures) booleans.add(f.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            if (executor != null) executor.shutdown();
        }

        return booleans;
    }

    public Collection<Object> readCollectionOfObjects(Collection<Integer> keys) {
        Collection<Object> objects = new LinkedList<>();
        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(connectionPoolSize);

            Collection<CRUDworker<Integer, Object>> cruDworkers = new LinkedList<>();
            Collection<Future<Object>> futures = new LinkedList<>();

            for (Integer key : keys) cruDworkers.add(new Reader(key));

            futures = executor.invokeAll(cruDworkers);

            for (Future<Object> f : futures) objects.add(f.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            if (executor != null) executor.shutdown();
        }

        return objects;
    }

    public Collection<Boolean> updateCollectionOfObjects(Collection<Object> objects) {
        Collection<Boolean> booleans = new LinkedList<>();
        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(connectionPoolSize);

            Collection<CRUDworker<Object, Boolean>> cruDworkers = new LinkedList<>();
            Collection<Future<Boolean>> futures = new LinkedList<>();

            for (Object o : objects) cruDworkers.add(new Updater(o));

            futures = executor.invokeAll(cruDworkers);

            for (Future<Boolean> f : futures) booleans.add(f.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            if (executor != null) executor.shutdown();
        }

        return booleans;
    }

    public Collection<Boolean> deleteCollectionOfObjects(Collection<Integer> keys) {
        Collection<Boolean> booleans = new LinkedList<>();
        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(connectionPoolSize);

            Collection<CRUDworker<Integer, Boolean>> cruDworkers = new LinkedList<>();
            Collection<Future<Boolean>> futures = new LinkedList<>();

            for (Integer key : keys) cruDworkers.add(new Remover(key));

            futures = executor.invokeAll(cruDworkers);

            for (Future<Boolean> f : futures) booleans.add(f.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            if (executor != null) executor.shutdown();
        }

        return booleans;
    }

    private abstract class CRUDworker<E, V> implements Callable<V> {
        private E processElement;

        protected CRUDworker(E processElement) {
            this.processElement = processElement;
        }

        @Override
        public V call() throws Exception {
            return callMainMethod(processElement);
        }

        protected abstract V callMainMethod(E e);
    }

    private class Creator extends CRUDworker<Object, Boolean> {

        protected Creator(Object processElement) {
            super(processElement);
        }

        @Override
        protected Boolean callMainMethod(Object o) {
            return clientCRUDhandler.createObj(o);
        }
    }

    private class Reader extends CRUDworker<Integer, Object> {

        protected Reader(Integer processElement) {
            super(processElement);
        }

        @Override
        protected Object callMainMethod(Integer i) {
            return clientCRUDhandler.readObj(i);
        }
    }

    private class Updater extends CRUDworker<Object, Boolean> {

        protected Updater(Object processElement) {
            super(processElement);
        }

        @Override
        protected Boolean callMainMethod(Object o) {
            return clientCRUDhandler.updateObj(o);
        }
    }

    private class Remover extends CRUDworker<Integer, Boolean> {

        protected Remover(Integer processElement) {
            super(processElement);
        }

        @Override
        protected Boolean callMainMethod(Integer i) {
            return clientCRUDhandler.deleteObj(i);
        }
    }
}