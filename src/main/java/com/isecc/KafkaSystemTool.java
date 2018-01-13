package com.isecc;

import java.util.Date;
import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

// developer: Evo Eftimov
// Swiss Army Knife tool for debugging Kafka projects and operations
// this first version supports Offset Gap identification and analysis

public class KafkaSystemTool {

    public static void main(String[] args) throws Exception {

        String bootstrapServers = "localhost:9092";
        String topic = "test2";
        int partitionIdx = 1;
        String group = "group-YX";
        int maxRecords = 0;
        Boolean seek = true;
        //long offsetVal = 65220;
        long offsetVal = 2;
        String kerberosLogon = "s";
        String assign = "assign";

        //To Do: more robust handling of input parameters

        if(args.length == 8){

            bootstrapServers = args[0];
            group = args[1];
            topic = args[2].toString();
            partitionIdx = Integer.parseInt(args[3]);
            kerberosLogon = args[4];
            offsetVal = Long.parseLong(args[5]);
            maxRecords = Integer.parseInt(args[6]);
            assign = args[7];

        }
        else{

            System.out.println("Usage: KafkaSystemTool <bootsrap servers> <group name> <topic name> <partition> <kerberos/no> <the required offset> <range of records around the offset> <assign/subscribe>");
            //return ;
        }


        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", group);
        props.put("enable.auto.commit", "false");
        //props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        if (kerberosLogon.equalsIgnoreCase("kerberos")){
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.kerberos.service.name", "kafka");
            System.out.println("Will perform Kerberos Authentication");
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        if (assign.equalsIgnoreCase("assign")){
            TopicPartition partition = new TopicPartition(topic, partitionIdx);
            consumer.assign(Arrays.asList(partition));
            System.out.println("The consumer performed ASSIGN");
        }
        else{
            consumer.subscribe(Arrays.asList(topic));
            System.out.println("The consumer performed SUBSCRIBE");
        }

        System.out.println("Consuming from topic " + topic);

        int i = 0;

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(100);

            if (seek) {
                consumer.seek(new TopicPartition(topic, partitionIdx), offsetVal - maxRecords);
                seek = false;
            }

            for (ConsumerRecord<String, String> record : records) {

                if (i == maxRecords) {
                    System.out.println("==============================================================================================");
                    System.out.printf("provided offset = %d, provided partition = %d\n", offsetVal, partitionIdx);
                }

                System.out.printf("offset = %d, timestamp = %s, partition = %d, key = %s, value = %s\n",
                        record.offset(), new Date(record.timestamp()).toString(), record.partition(), record.key(), record.value());

                if (i == maxRecords)
                    System.out.println("==============================================================================================");

                i++;
                if (i > 2*maxRecords)
                    System.exit(0);

            }


        }
    }
}


