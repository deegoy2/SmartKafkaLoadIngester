import com.walmart.deepak.utils.Helper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.KafkaMetricsUtil;

import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;

import static com.walmart.deepak.utils.Helper.GetPartitions;
import static com.walmart.deepak.utils.Helper.partition;

public class Intelligent {
    public static void main(String[] args) throws Exception{

        KafkaMetricsUtil.initialize("cbb3");
        StringSerializer serializer = new StringSerializer();

        Random random = new Random();
        String topicName = "msg.application";

        Properties props = new Properties();
        props.put("bootstrap.servers", "cbb-kafka-internal-ssl.stg-internal-cdc8.cbb-kafka-internal-ssl.labsitepersonalization.prod.walmart.com:9092");
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 1048576);
        props.put("compression.type", "lz4");
//        props.put("batch.size", 1638400);
        props.put("linger.ms", 1000);
//        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        String _10Kb= Helper._10Kb;
        String _1Kb = Helper._1Kb;

        HashSet<Integer> localPartitions = GetPartitions();

        Long ct=0L;
        int randomSuffix;
        TreeMap<Integer,Integer> hashMap = new TreeMap<>();
        for (Integer i = 0; i <= 1000000000; i++) {
            randomSuffix = random.nextInt(100000);
            String key = "Deepak"+randomSuffix;
            int part = partition(key.getBytes());
            if(!localPartitions.contains(part)){
                continue;
            }

            if(!hashMap.containsKey(part)){
                hashMap.put(part ,0);
            }
            if(hashMap.get(part) <=1000000) {

                if((hashMap.get(part) % 10000) == 0){
                    System.out.println("TotalCount: " + ct + "\tWrote " + hashMap.get(part) +" keys for partition: "+part);
                }
                ct++;
                hashMap.put(part, hashMap.get(part)+1);
                String value =
                        "{\"appName\":\"ero\",\"trentyId\":\""
                        + key
                        + _10Kb
                        + ct
                        + "}]}";
                producer.send(new ProducerRecord<String, String>(topicName, key, value));
                KafkaMetricsUtil.get().incDeadThreadCounter();
            }
        }
        System.out.println("Message sent successfully");
        producer.close();
    }
}


// java -cp cbb3-benchmark.jar Intelligent
//https://confluence.walmart.com/display/CARCB/Benchmark3
//scp d0g00kj@172.30.125.21:/Users/d0g00kj/KafkaStreams/cbb2/benchmark/target/cbb3-benchmark.jar ~/