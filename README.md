# Sharding2
1. Before starting work you must start JMS server, initialize Broker, add Topic to Broker destinations
2. To start master or slave use Server.class from com/cyber/sharding_hw/server/
3. Automatic rebalancing work only when you add new Shard to master.
4. Before adding new shard necessarily to start this shard.
5. When shard reaches his setted maxCapacity, it will inform master.

To test client use TestClient.class from /com/cyber/test_client/

