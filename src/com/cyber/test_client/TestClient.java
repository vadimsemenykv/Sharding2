package com.cyber.test_client;

import com.cyber.sharding_hw.client.Client;
import com.cyber.sharding_hw.client.ClientCRUDhandler;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Scanner;

/**
 * Created by Vadim on 15.01.2015.
 */
public class TestClient {
    public static void main(String[] args) {
//        System.out.println("Start client");
//        Scanner scanner = new Scanner(System.in);
//        System.out.println("Set server ip address:");
//        String ip = scanner.nextLine();
//        scanner.close();

        Client client = new Client("127.0.0.1", 3366, 10);


        Collection<Object> objectCollection = new LinkedList();
        objectCollection.add(new TestObject("Alex", 43));
        objectCollection.add(new TestObject("Sara", 47));
        objectCollection.add(new TestObject("Greg", 21));

        System.out.println("write objects");
        System.out.println((client.createCollectionOfObjects(objectCollection)));


        Collection<Integer> keysCollection = new LinkedList();
        keysCollection.add(new Integer("Alex".hashCode()));
        keysCollection.add(new Integer("Sara".hashCode()));
        keysCollection.add(new Integer("Greg".hashCode()));

        System.out.println("read objects\n");
        Collection<Object> readedObjects = client.readCollectionOfObjects(keysCollection);
        for (Object o : readedObjects) System.out.println(((TestObject) o).name + " - " + ((TestObject) o).age);
    }
}
