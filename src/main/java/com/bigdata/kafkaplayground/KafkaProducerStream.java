/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.kafkaplayground;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.StringTokenizer;
import org.apache.kafka.clients.producer.Producer;

/**
 *
 * @author Francisco
 */
public class KafkaProducerStream {
 private static Scanner in;
          public static void main(String[] argv)throws Exception {
             /* if (argv.length != 1) {
                  System.err.println("Please specify 1 parameters ");
                  System.exit(-1);
              } */
              String topicName = "test";
              in = new Scanner(System.in);
              System.out.println("Enter message(type exit to quit)");

              //Configure the Producer
              Properties configProperties = new Properties();
              configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:2181");
              configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
              configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

              Producer producer = new KafkaProducer<String, String>(configProperties);
              
              File file = new File("/Users/Francisco/workspace/realdataset.txt"); // Leo el archivo de lectura de datos.
 
              try (Scanner scanner = new Scanner(file)) {
 
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine(); 
                        ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, line);
                        producer.send(rec);                                                                                
		}

		scanner.close();
	} catch (IOException e) {
		e.printStackTrace();
	}
              in.close();
              producer.close();
          }   
}
