/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.kafkaplayground;


import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;


/**
 *
 * @author Francisco
 */
public class KafkaTopicCreator {
      public static void create(String topicname, int partitionsno, int replicationno
                                ) throws Exception {
        String zookeeperConnect = "zkserver1:2181,zkserver2:2181";
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;

        String topic = "test-topic";
        int partitions = 2;
        int replication = 3;
        Properties topicConfig = new Properties(); // add per-topic configurations settings here

        // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
        // createTopic() will only seem to work (it will return without error).  The topic will exist in
        // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
        // topic.
        ZkClient zkClient = new ZkClient(
            zookeeperConnect,
            sessionTimeoutMs,
            connectionTimeoutMs,
            ZKStringSerializer$.MODULE$);

        // Security for Kafka was added in Kafka 0.9.0.0
        boolean isSecureKafkaCluster = false;

        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
        if(!AdminUtils.topicExists(zkUtils, topic)){
            AdminUtils.createTopic(zkUtils, topic, partitions, 
                                  replication, topicConfig, RackAwareMode.Enforced$.MODULE$);
        }
        else System.out.println("Topic " + topic + "is already created");
        zkClient.close();
        }  
}
