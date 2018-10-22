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

    public ScheduledTask(Integer sn, TreeMap<Integer,Integer> keyCounts, Integer start, Integer step){
        KafkaMetricsUtil.initialize("cbb3");
        this.sn= sn;
        this.step= step;
        this.keyCounts = keyCounts;
        this.start = start;
        this.producer = Helper.getProducer();
        this.localPartitions = Helper.GetPartitions(producer, "msg.application");
    }

    public void run() {

        for (int i = start; i< start+step; i++) {
            int prefix = i;
            prefix = prefix%66000000;
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