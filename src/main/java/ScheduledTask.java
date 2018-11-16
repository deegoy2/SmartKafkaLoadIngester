import com.walmart.deepak.utils.Helper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.KafkaMetricsUtil;

import java.util.*;

import static com.walmart.deepak.utils.Helper._10Kb;

/**
 *
 * @author Dhinakaran P.
 */
// Create a class extends with TimerTask
public class ScheduledTask extends TimerTask {

    public Integer sn;
    public TreeMap<Integer,Integer> keyCounts;
    public HashSet<Integer> localPartitions;
    public Integer start;
    public Producer<String, String> producer;
    public Integer step;

    public ScheduledTask(Integer sn, TreeMap<Integer,Integer> keyCounts, HashSet<Integer> localPartitions, Integer start, Integer step){

        //KafkaMetricsUtil.initialize("cbb3");
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

        this.sn= sn;
        this.step= step;
        this.keyCounts = keyCounts;
        this.localPartitions =localPartitions;
        this.start = start;
        this.producer = new KafkaProducer<String, String>(props);
    }

    public void run() {
        for (int i = start; i< start+step; i++) {
            int prefix = i;
            prefix = prefix%13000000;
            if(prefix==0){
                sn++;
            }
            String key = "Deepak"+prefix;
            int part = Helper.partition(key.getBytes());

            if(!localPartitions.contains(part)){
                continue;
            }
            if((keyCounts.get(part)) % 10000 == 0){
                System.out.println("Wrote " + keyCounts.get(part) +" keys for partition: "+part);
            }
            String value =
                    "{\"appName\":\"ero\",\"trentyId\":\""
                            + key
                            + _10Kb
                            + sn
                            + "}]}";
            producer.send(new ProducerRecord<>("msg.application", key, value));
            KafkaMetricsUtil.get().incDeadThreadCounter();
            keyCounts.put(part, keyCounts.get(part)+1);
        }
        start = start + step;
    }
}