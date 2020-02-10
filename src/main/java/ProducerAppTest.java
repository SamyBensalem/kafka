import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProducerAppTest {

    private int counter=0;

    ProducerAppTest(){

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"client-producer-1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String,String>(properties);

        Random random = new Random();
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(()->{

            String key = String.valueOf((++counter));
            String value = String.valueOf(random.nextDouble()*9999999);
            kafkaProducer.send(new ProducerRecord<>("test4",key,value),(recordMetadata, e) ->{
                System.out.println("Sending message => "+value+"Partition => "+recordMetadata.partition()+" => "+recordMetadata.offset());

            });

        },1000,100, TimeUnit.MICROSECONDS);



    }
    public static void main(String[] args) {
        new ProducerAppTest();
    }
}
