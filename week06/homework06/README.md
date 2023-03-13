## Week 6 Homework 

In this homework, there will be two sections, the first session focus on theoretical questions related to Kafka 
and streaming concepts and the second session asks to create a small streaming application using preferred 
programming language (Python or Java).

---

### Question 1: 

**Please select the statements that are correct**

- [] Kafka Node is responsible to store topics
- [X] Zookeeper is removed from Kafka cluster starting from version 4.0
- [X] Retention configuration ensures the messages not get lost over specific period of time.
- [X] Group-Id ensures the messages are distributed to associated consumers

> Answer

- The topics' storage is handled by the Kafka brokers.
- Kafka declares KRaft generally available in Kafka 3.3 release. ZooKeeper would be deprecated in the release after that, and removed in Kafka 4.0.
- Retention can be set either on time or the data amount.
- The Group ID determines which consumers belong to which group. All consumers within a Group Id will read data from a same topic.

---

### Question 2: 

**Please select the Kafka concepts that support reliability and availability**

- [X] Topic Replication
- [X] Topic Partioning
- [] Consumer Group Id
- [X] Ack All

> Answer

- Kafka replicates a topic partition in multiple brokers ensuring reliability and availability.
- Kafka divides a topic into multiple partitionsm asigning each partition to a broker. Whenever a broker failure occurs, only a subset of partitions is affected.
- Consumer Group Id is not related to reliability/availability but rather to scalability.
- Ack All provides highest reliability but it causes slowness in performance.

---

### Question 3: 

**Please select the Kafka concepts that support scaling**  

- [X] Topic Replication
- [X] Topic Partioning
- [X] Consumer Group Id
- [] Ack All

> Answer

- Topic Replication permits continuous operation even when brokers fail, fostering horizontal scaling.
- Topic Partioning allows Kafka handle huge data volumes by distribuiting the load accross multiple brokers where partitions reside.
- Every new instance of a consumer group (Id) can subscribe to one or more topics, and their partitions will be evenly distributed between the instances in that group. That allows parallel processing promoting scalability.
- Ack all is the most reliable configuration, however, it reduces performance and scalability since a confirmation is needed not only from the leader but from all the replicas as well.

---

### Question 4: 

**Please select the attributes that are good candidates for partitioning key. 
Consider cardinality of the field you have selected and scaling aspects of your application**  

- [] payment_type
- [] vendor_id
- [] passenger_count
- [] total_amount
- [X] tpep_pickup_datetime
- [X] tpep_dropoff_datetime

> Answer

Either tpep_pickup_datetime, tpep_dropoff_datetime, or the combination of the two. It would depend on the use case. Both attributes are good candidates and ensure high level of cardinality because of the (highly) uniqueness, since both have a large number of unique values.

On the other hand, the other attributes (payment_type, vendor_id, passenger_count and total_amount) might not ensure high cardinality (e.g. passenger count or payment type have few possibilities) nor be evenly distributed across partitions.

---

### Question 5: 

**Which configurations below should be provided for Kafka Consumer but not needed for Kafka Producer**

- [X] Deserializer Configuration
- [X] Topics Subscription
- [] Bootstrap Server
- [X] Group-Id
- [X] Offset
- [] Cluster Key and Cluster-Secret

> Answer

- Deserializer Configuration: A consumer needs to deserialize a message.
- Topics Subscription: A consumer must subscribe to a topic in order to receive messages.
- Group-Id: Any consumer needs to specify a unique Group-Id.
- Offset: A consumer have to track the offset of the last meessage it receives.

Others:
- Bootstrap Server: Needs to be configured in both ends (producers and consumers).
- Cluster Key and Cluster-Secret: This is transparent for the producers/consumers.

---

### Question 6 (optional):

Please implement a streaming application, for finding out popularity of PUlocationID across green and fhv trip datasets.
Please use the datasets [fhv_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) 
and [green_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green)

PS: If you encounter memory related issue, you can use the smaller portion of these two datasets as well, 
it is not necessary to find exact number in the  question.

Your code should include following
1. Producer that reads csv files and publish rides in corresponding kafka topics (such as rides_green, rides_fhv)
2. Pyspark-streaming-application that reads two kafka topics
   and writes both of them in topic rides_all and apply aggregations to find most popular pickup location.

   
## Submitting the solutions

* Form for submitting: https://forms.gle/rK7268U92mHJBpmW7
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 13 March (Monday), 22:00 CET


## Solution

We will publish the solution here after deadline

---

## References

- https://developer.confluent.io/learn-kafka/architecture/consumer-group-protocol/
-