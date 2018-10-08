import com.walmart.deepak.utils.Helper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

public class KeyGen {
    public static void main(String[] args) throws Exception{


        TreeMap<Integer,Integer> keyCounts = new TreeMap<>();
        ArrayList<File> files  = new ArrayList<>();
        ArrayList<FileWriter> fileWriters  = new ArrayList<>();
        for(int i=0;i<128;i++){
            File file = new File(args[0]+ "/partition"+i);
            FileWriter writer = new FileWriter(file);
            files.add(file);
            fileWriters.add(writer);
            keyCounts.put(i,0);
        }

        int ct=0;
        for (Integer i = 0; i < 66000000; i++) {
            String key = "Deepak"+i;
            int part = Helper.partition(key.getBytes());
            if(keyCounts.get(part) == 500000){
                continue;
            }
            keyCounts.put(part, keyCounts.get(part)+1);
            fileWriters.get(part).write(key+"\n");
            ct++;
        }
        for(int i=0;i<128;i++){
            fileWriters.get(i).flush();
            fileWriters.get(i).close();
            System.out.println("Partition:" + i + "\tKeyCount:" + keyCounts.get(i) );
        }
    }
}
