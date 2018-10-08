import com.walmart.deepak.utils.Helper;

import java.util.HashSet;
import java.util.Timer;
import java.util.TreeMap;

/**
 *
 * @author Dhinakaran P.
 */

//Main class
public class SchedulerMain {
    public static void main(String args[]) throws InterruptedException {
        TreeMap<Integer,Integer> keyCounts = new TreeMap<>();
        for(int i=0;i<128;i++){
            keyCounts.put(i,0);
        }
        HashSet<Integer> localPartitions = Helper.GetPartitions();
        Integer start=0;

        Timer time = new Timer(); // Instantiate Timer Object
        int sn =Integer.parseInt(args[0]);
        int step = Integer.parseInt(args[1]);
        ScheduledTask st = new ScheduledTask(sn,keyCounts,localPartitions,start ,step)  ; // Instantiate SheduledTask class
        time.schedule(st, 0, 1000); // Create Repetitively task for every 1 secs
    }
}