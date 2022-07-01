import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangeProducer {

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServer = "localhost:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /*
        for older Kafka version <==2.8
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        */

        /*
        linger.ms refers to the time to wait before sending messages out to Kafka. It defaults to 0, which the system interprets as ‘send messages as soon as they are ready to be sent’.
        batch.size refers to the maximum amount of data to be collected before sending the batch.
        Kafka producers will send out the next batch of messages whenever linger.ms or batch.size is met first.
         */
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


        //create producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        //todo
        EventHandler eventHandler = new WikimediaChangeHandler(kafkaProducer,topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        //start the producer in another thread
        eventSource.start();

        // we produce for 10 min and block the program until then
        TimeUnit.MINUTES.sleep(10);
    }
}
