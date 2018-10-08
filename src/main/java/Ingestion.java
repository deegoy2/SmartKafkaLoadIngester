import com.walmart.deepak.utils.Helper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.KafkaMetricsUtil;
import java.util.*;

import static com.walmart.deepak.utils.Helper.GetPartitions;


public class Ingestion {
    public static void main(String[] args) throws Exception{

        KafkaMetricsUtil.initialize("cbb3");
        StringSerializer serializer = new StringSerializer();
        Random random = new Random();
        String topicName = "msg.application";

        Properties props = new Properties();
        props.put("bootstrap.servers", "cbb-kafka-internal-ssl.stg-internal-cdc8.cbb-kafka-internal-ssl.labsitepersonalization.prod.walmart.com:9092");
        props.put("acks", "all");
        props.put("retries", 100);
        props.put("batch.size", 1048576);
        props.put("compression.type", "lz4");
        props.put("linger.ms", 1000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        String _10Kb= Helper._10Kb;
        HashSet<Integer> localPartitions = GetPartitions();

        TreeMap<Integer,Integer> keyCounts = new TreeMap<>();
        for(int i=0;i<128;i++){
            keyCounts.put(i,0);
        }
        Long ct=1L;
        for (Integer i = 0; i < 130000000; i++) {
            String key = "Deepak"+i;
            int part = Helper.partition(key.getBytes());
            if(keyCounts.get(part) == 250000){
                continue;
            }

            if(!localPartitions.contains(part)){
                continue;
            }
            if((keyCounts.get(part)+1) % 20000 == 0){
                System.out.println("Wrote " + keyCounts.get(part) +" keys for partition: "+part);
            }
            String value =
                    "{\"appName\":\"ero\",\"trentyId\":\""
                            + key
                            + _10Kb
                            + ct
                            + "}]}";
            producer.send(new ProducerRecord<>(topicName, key, value));
            KafkaMetricsUtil.get().incDeadThreadCounter();
            keyCounts.put(part, keyCounts.get(part)+1);
        }
        for(int i=0;i<128;i++){
            System.out.println("Partition:" + i + "\tKeyCount:" + keyCounts.get(i) );
        }
        producer.close();
    }
}
