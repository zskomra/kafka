import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithShoutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am a Kafka Consumer With Shutdown");

        String bootstrapServers = "localhost:9092";
        String groupId = "my-third-application";
        String topic = "demo_java";

        //create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId );
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        //get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()....");
                consumer.wakeup();

                //join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

        try {
            //subscribe consumer to our topic(s)
            consumer.subscribe(List.of(topic));

            //poll for new data
            while (true){
                ConsumerRecords<String,String> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String,String> record : records) {
                    log.info("Key: " + record.key() + ", Value " + record.value());
                    log.info("Key: " + record.partition() + ", Offsets " + record.offset());
                }

            }

        } catch (WakeupException e) {
            log.info("wake up exceception");
            //we ignore this as this is an expected exception when closing a consumer
        } catch (Exception e) {
            log.error("Unexpected exception");
        } finally {
            consumer.close();
            log.info("The consumer is now gracefully closed");
        }

    }
}
