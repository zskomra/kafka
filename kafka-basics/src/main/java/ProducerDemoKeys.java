import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "demo_java";
            String value = "hello world " + i;
            String key = "id_" + i;

            //create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,key,value);

            //send the data - async
            producer.send(producerRecord, new Callback() {
                //execute every time a record is successfully sent or an exception is thrown
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if(e == null){
                        //the record was successfully sent
                        log.info("Received new metadata/ \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Key: " + producerRecord.key() +  "\n" +
                                "Partition " + metadata.partition() +  "\n" +
                                "Offset " + metadata.offset() +  "\n" +
                                "Timestamp " + metadata.timestamp());
                    }
                    else {
                        log.error("Error while producing", e);
                    }
                }
            });
        }

        //flush data - sync
        producer.flush();

        //flush and close the Producer
        producer.close();

    }
}
