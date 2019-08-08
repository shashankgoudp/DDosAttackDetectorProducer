package com.phData.DDOSAttack;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class Producer {

    KafkaProducer<String, String> kafkaProducer;
    public void initialize(String bootstrapServerAddress) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());

        kafkaProducer = new KafkaProducer<>(props);

    }

    public static void main(String[] args) throws Exception {

        if(args.length !=3){
            System.out.println("Wrong number of arguments passed");
        }
        String topicName = args[0];
        String bootstrapServerAddress = args[1];
        String inputFileName = args[2];
        Producer producer = new Producer();
        producer.initialize(bootstrapServerAddress);
        producer.publishMesssage(inputFileName, topicName);

    }

    public void publishMesssage(String inputFileName, String topicName) throws Exception {
        File file = new File(inputFileName);
        FileInputStream fstream = new FileInputStream(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        String msg = null;

        //Read File Line By Line
        while ((msg = br.readLine()) != null) {
            String key = msg.split(" ")[0];
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,key, msg);
            kafkaProducer.send(record);
        }
    }
}