package com.walmart.deepak.utils;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.requests.MetadataResponse;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Helper {


    public static String _10Kb= "\",\"trentyType\":\"wmt\",\"payload\":[{\"key\":\"shoppingListV1\",\"value\":{\"pangaeaId\":\"f06dcde986b239bae044001517f43a86\",\"eroData\":[{\"baseItemId\":\"53373993\",\"offerId\":\"46CA83BC450E435681245353CB3B47B0\",\"score\":2.298819,\"usSellerId\":\"0\",\"usItemId\":\"53373993\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-07-04\",\"totalQuantity\":9,\"totalOrders\":9,\"productId\":\"4VB6VR5KKFZR\"},{\"baseItemId\":\"10295171\",\"offerId\":\"68173A3D23004545A5D59B0793C360E3\",\"score\":2.263448,\"usSellerId\":\"0\",\"usItemId\":\"10295171\",\"lastQuantity\":2,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-29\",\"totalQuantity\":11,\"totalOrders\":7,\"productId\":\"7K175ODCWP8L\"},{\"baseItemId\":\"10295277\",\"offerId\":\"08CA47699D3D4012B5A029200EAF85B9\",\"score\":2.213622,\"usSellerId\":\"0\",\"usItemId\":\"10295277\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-01\",\"totalQuantity\":9,\"totalOrders\":4,\"productId\":\"2VRZNMUHY6LK\"},{\"baseItemId\":\"12166390\",\"offerId\":\"0B3AFCB14C2C4308BA85FF749DE488FF\",\"score\":2.202758,\"usSellerId\":\"0\",\"usItemId\":\"12166390\",\"lastQuantity\":2,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-07-04\",\"totalQuantity\":5,\"totalOrders\":4,\"productId\":\"2GQYBYB5CKLX\"},{\"baseItemId\":\"17018118\",\"offerId\":\"51C21E0890DB4E54B09045A45AFA2610\",\"score\":2.15366,\"usSellerId\":\"0\",\"usItemId\":\"17018118\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-03-30\",\"totalQuantity\":3,\"totalOrders\":3,\"productId\":\"5C4M2XRU6P1S\"},{\"baseItemId\":\"11027754\",\"offerId\":\"C6CD5BC3E19742C4AB6D5F29CD8421EA\",\"score\":2.086486,\"usSellerId\":\"0\",\"usItemId\":\"11027754\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-07-04\",\"totalQuantity\":2,\"totalOrders\":2,\"productId\":\"0S0B8CHHQNVK\"},{\"baseItemId\":\"42425073\",\"offerId\":\"F49A0C1AC6314648AD40B665D33D05F4\",\"score\":2.07064,\"usSellerId\":\"0\",\"usItemId\":\"42425073\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-07-04\",\"totalQuantity\":2,\"totalOrders\":2,\"productId\":\"782U6M9TFUW5\"},{\"baseItemId\":\"655597420\",\"offerId\":\"45197A673DAB4DD2AC62CB21060A1888\",\"score\":2.059633,\"usSellerId\":\"0\",\"usItemId\":\"655597420\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-07-04\",\"totalQuantity\":1,\"totalOrders\":1,\"productId\":\"3EXVEYLEY2K1\"},{\"baseItemId\":\"11045784\",\"offerId\":\"CBBE9B5A0B21456080E746E119B25B23\",\"score\":2.057446,\"usSellerId\":\"0\",\"usItemId\":\"11045784\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-29\",\"totalQuantity\":3,\"totalOrders\":3,\"productId\":\"5D6U0YMRKKCN\"},{\"baseItemId\":\"16940474\",\"offerId\":\"EBB804B9247040DCAE135FE1B2724608\",\"score\":2.05723,\"usSellerId\":\"0\",\"usItemId\":\"16940474\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-29\",\"totalQuantity\":1,\"totalOrders\":1,\"productId\":\"2FKEUGXALHEH\"},{\"baseItemId\":\"37792362\",\"offerId\":\"18CFA5EEFD6B433380683D13D8660B2C\",\"score\":2.055543,\"usSellerId\":\"0\",\"usItemId\":\"37792362\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-29\",\"totalQuantity\":1,\"totalOrders\":1,\"productId\":\"4N5Z1JYLJVHP\"},{\"baseItemId\":\"14150016\",\"offerId\":\"E131D0A0868B4A57B8EB1E06F270B5C8\",\"score\":2.055201,\"usSellerId\":\"0\",\"usItemId\":\"14150016\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-29\",\"totalQuantity\":1,\"totalOrders\":1,\"productId\":\"53BBM8TXA6ED\"},{\"baseItemId\":\"144368067\",\"offerId\":\"5098CC70814048408B5B46F773AE8C7E\",\"score\":2.054365,\"usSellerId\":\"0\",\"usItemId\":\"144368067\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-29\",\"totalQuantity\":1,\"totalOrders\":1,\"productId\":\"33DIOZQXC279\"},{\"baseItemId\":\"646946712\",\"offerId\":\"FC0859A72FA649E89F2C3832F299AC4E\",\"score\":2.048535,\"usSellerId\":\"0\",\"usItemId\":\"646946712\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-29\",\"totalQuantity\":1,\"totalOrders\":1,\"productId\":\"2C92U0OZBCPV\"},{\"baseItemId\":\"25822599\",\"offerId\":\"A1E40BE9EC004A6AAB01192F7310414E\",\"score\":2.040602,\"usSellerId\":\"0\",\"usItemId\":\"25822599\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-03-30\",\"totalQuantity\":3,\"totalOrders\":3,\"productId\":\"4G1NC746DPUT\"},{\"baseItemId\":\"37792362\",\"offerId\":\"CDA9CD347B3D4B7FB752E516D072198F\",\"score\":2.036639,\"usSellerId\":\"0\",\"usItemId\":\"19416508\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-05-06\",\"totalQuantity\":2,\"totalOrders\":2,\"productId\":\"52BM9VUWJ4ZM\"},{\"baseItemId\":\"29236006\",\"offerId\":\"2A7ED57CC7AD43E49004647F4DAABA72\",\"score\":2.035323,\"usSellerId\":\"0\",\"usItemId\":\"29236006\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-01\",\"totalQuantity\":2,\"totalOrders\":2,\"productId\":\"1OS6N8U8IE08\"},{\"baseItemId\":\"20854176\",\"offerId\":\"A8F1C733EF8043A8B6E546C7C5EEEB1F\",\"score\":2.032824,\"usSellerId\":\"0\",\"usItemId\":\"20854176\",\"lastQuantity\":2,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-03\",\"totalQuantity\":2,\"totalOrders\":1,\"productId\":\"4663VGR38UIG\"},{\"baseItemId\":\"10307999\",\"offerId\":\"C9700F752D624079AFA7B150ABB9F0E0\",\"score\":2.032824,\"usSellerId\":\"0\",\"usItemId\":\"10307999\",\"lastQuantity\":2,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-03\",\"totalQuantity\":2,\"totalOrders\":1,\"productId\":\"5K849DU8NQ1Y\"},{\"baseItemId\":\"10308006\",\"offerId\":\"CFD6ABFBD44645C294C6CE56BAFA8CF4\",\"score\":2.032824,\"usSellerId\":\"0\",\"usItemId\":\"10308006\",\"lastQuantity\":2,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-03\",\"totalQuantity\":2,\"totalOrders\":1,\"productId\":\"6EWSUJCLH4EA\"},{\"baseItemId\":\"10321395\",\"offerId\":\"3CF7E39E25674AF9AFEE906185CA4D12\",\"score\":2.032824,\"usSellerId\":\"0\",\"usItemId\":\"10321395\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-03\",\"totalQuantity\":1,\"totalOrders\":1,\"productId\":\"4S3IH43D2BW9\"},{\"baseItemId\":\"10291701\",\"offerId\":\"E66C274CE9D546CEB7F37653576B01A4\",\"score\":2.032824,\"usSellerId\":\"0\",\"usItemId\":\"10291701\",\"lastQuantity\":5,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-03\",\"totalQuantity\":5,\"totalOrders\":1,\"productId\":\"17SHRJL6HN8H\"},{\"baseItemId\":\"31954395\",\"offerId\":\"808B99F8C8764AA9B4D484E4C0D4059E\",\"score\":2.0299,\"usSellerId\":\"0\",\"usItemId\":\"31954395\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-01\",\"totalQuantity\":9,\"totalOrders\":8,\"productId\":\"2MGNQTTXO13C\"},{\"baseItemId\":\"19757671\",\"offerId\":\"8809291C8A1948D9A77FFE8B8FD8C184\",\"score\":2.025384,\"usSellerId\":\"0\",\"usItemId\":\"19757671\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-05-06\",\"totalQuantity\":1,\"totalOrders\":1,\"productId\":\"3DJYCB5JBC2Q\"},{\"baseItemId\":\"21014200\",\"offerId\":\"7B4A4F2D0EFF4F1AB6BBC7B3B0A16747\",\"score\":2.024034,\"usSellerId\":\"0\",\"usItemId\":\"21014200\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-29\",\"totalQuantity\":3,\"totalOrders\":3,\"productId\":\"3YNEF8IDYY8K\"},{\"baseItemId\":\"49585273\",\"offerId\":\"EE74ECDEBCE94E12939AA4909CCD2EC5\",\"score\":2.019205,\"usSellerId\":\"0\",\"usItemId\":\"49585273\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-05-11\",\"totalQuantity\":1,\"totalOrders\":1,\"productId\":\"5OH3JQ31GG03\"},{\"baseItemId\":\"17324971\",\"offerId\":\"1B16109F64AD4B7AABDCCC1E16C51CFE\",\"score\":2.017895,\"usSellerId\":\"0\",\"usItemId\":\"17324971\",\"lastQuantity\":2,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-05-01\",\"totalQuantity\":3,\"totalOrders\":2,\"productId\":\"627ZKKSKY891\"},{\"baseItemId\":\"115990855\",\"offerId\":\"80230B216541447B83D308075A71D18C\",\"score\":2.014917,\"usSellerId\":\"0\",\"usItemId\":\"115990855\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-03-30\",\"totalQuantity\":2,\"totalOrders\":2,\"productId\":\"2A3H7UL7YX0P\"},{\"baseItemId\":\"20880815\",\"offerId\":\"28A9064CF3904990AAF624F511A4A274\",\"score\":2.01485,\"usSellerId\":\"0\",\"usItemId\":\"20880815\",\"lastQuantity\":4,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-01\",\"totalQuantity\":6,\"totalOrders\":2,\"productId\":\"472VNSWPODMU\"},{\"baseItemId\":\"21096717\",\"offerId\":\"5DB49143835E46978F224BB4FCC5F6B0\",\"score\":2.01455,\"usSellerId\":\"0\",\"usItemId\":\"21096717\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-07-04\",\"totalQuantity\":5,\"totalOrders\":3,\"productId\":\"3TNROTND8WZA\"},{\"baseItemId\":\"50423046\",\"offerId\":\"D279611EDD48418484EC471173E8C84E\",\"score\":2.010736,\"usSellerId\":\"0\",\"usItemId\":\"50423046\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-01\",\"totalQuantity\":1,\"totalOrders\":1,\"productId\":\"3ZWNDFF2N87B\"},{\"baseItemId\":\"37792362\",\"offerId\":\"FE839F9DDFF14861A8430555E90A7298\",\"score\":2.008912,\"usSellerId\":\"0\",\"usItemId\":\"37792361\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-01\",\"totalQuantity\":1,\"totalOrders\":1,\"productId\":\"3OU55ZBVF5P5\"},{\"baseItemId\":\"183836991\",\"offerId\":\"B6E93A56D0A44FDC8DF133F3814A583F\",\"score\":2.004558,\"usSellerId\":\"0\",\"usItemId\":\"183836991\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-02-11\",\"totalQuantity\":2,\"totalOrders\":2,\"productId\":\"3BUV5T12W949\"},{\"baseItemId\":\"36073275\",\"offerId\":\"685CA92B23454225AAF7EF35DE7312AA\",\"score\":2.004081,\"usSellerId\":\"0\",\"usItemId\":\"36073275\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-02-01\",\"totalQuantity\":2,\"totalOrders\":2,\"productId\":\"5B0JUFKC4BH2\"}]},\"operation\":\"WRITE\",\"sequenceNum\":";
    public static String _1Kb = "\",\"trentyType\":\"wmt\",\"payload\":[{\"key\":\"shoppingListV1\",\"value\":{\"pangaeaId\":\"f06dcde986b239bae044001517f43a86\",\"eroData\":[{\"baseItemId\":\"53373993\",\"offerId\":\"46CA83BC450E435681245353CB3B47B0\",\"score\":2.298819,\"usSellerId\":\"0\",\"usItemId\":\"53373993\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-07-04\",\"totalQuantity\":9,\"totalOrders\":9,\"productId\":\"4VB6VR5KKFZR\"},{\"baseItemId\":\"10295171\",\"offerId\":\"68173A3D23004545A5D59B0793C360E3\",\"score\":2.263448,\"usSellerId\":\"0\",\"usItemId\":\"10295171\",\"lastQuantity\":2,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-29\",\"totalQuantity\":11,\"totalOrders\":7,\"productId\":\"7K175ODCWP8L\"},{\"baseItemId\":\"10295277\",\"offerId\":\"08CA47699D3D4012B5A029200EAF85B9\",\"score\":2.213622,\"usSellerId\":\"0\",\"usItemId\":\"10295277\",\"lastQuantity\":1,\"lastSource\":\"Walmart.com\",\"lastOrderDate\":\"2018-06-01\",\"totalQuantity\":9,\"totalOrders\":4,\"productId\":\"2VRZNMUHY6LK\"}]},\"operation\":\"WRITE\",\"sequenceNum\":";

    public static Producer<String, String> getProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "cbb-kafka-internal-ssl.stg-internal-cdc8.cbb-kafka-internal-ssl.labsitepersonalization.prod.walmart.com:9092");
        //props.put("acks", "all");
        props.put("retries", 100);
        props.put("batch.size", 1048576);
        props.put("compression.type", "lz4");
        props.put("linger.ms", 1000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>(props);
    }

    public static HashSet<Integer> GetPartitions(Producer<String, String> producer, String topic){
        String brokerId= "";
        //List<String> configs = readFileInList("/Users/d0g00kj/KafkaStreams/cbb2/temp/pubsub.properties");
        List<String> configs = readFileInList("/etc/kafka/pubsub.properties");

        for(String config: configs){
            if(config.startsWith("broker.id=")){
                brokerId = config.substring(10);
            }
        }

        List<PartitionInfo> partitionInfos = producer.partitionsFor(topic);
        HashSet<Integer> partitions = new HashSet<>();
        for(PartitionInfo partitionInfo : partitionInfos) {
            if(brokerId.equals(partitionInfo.leader().idString())) {
                partitions.add(partitionInfo.partition());
            }
        }
        return partitions;
    }

    public static HashSet<Integer> GetPartitions(){

        String brokerId= "";
        //List<String> configs = readFileInList("/Users/d0g00kj/KafkaStreams/cbb2/temp/pubsub.properties");
        List<String> configs = readFileInList("/etc/kafka/pubsub.properties");

        for(String config: configs){
            if(config.startsWith("broker.id=")){
                brokerId = config.substring(10);
            }
        }

        HashSet<Integer> partitions = new HashSet<>();

        HashMap<Integer,Integer> partitiontoBroker = new HashMap<>();
        String[] leaders = new String[]{"1917822898","1960176959","1984042953","432766029","502926999","502926999","816856590","502926999","816856590","1152186262","1188870872","1396671918","1396671918","1576636018","1576636018","1917822898","1576636018","1712850041","1917822898","1960176959","1984042953","502926999","502926999","816856590","871376180","502926999","1152186262","1188870872","1396671918","1396671918","1576636018","1576636018","1396671918","1917822898","1576636018","1712850041","1917822898","1960176959","1984042953","502926999","816856590","831309242","871376180","502926999","1188870872","1396671918","1396671918","1576636018","1576636018","1917822898","1396671918","1528954321","1576636018","1712850041","1917822898","1960176959","502926999","816856590","2039220615","871376180","1152186262","1188870872","1396671918","1396671918","1576636018","1576636018","1917822898","1917822898","1396671918","1528954321","1576636018","1712850041","1917822898","1960176959","1984042953","831309242","2039220615","1152186262","1188870872","1396671918","1396671918","1576636018","1576636018","1917822898","1917822898","1960176959","1396671918","1528954321","1576636018","1712850041","1917822898","1960176959","1984042953","871376180","1152186262","1188870872","1396671918","1396671918","1576636018","1576636018","1917822898","1917822898","1960176959","1188870872","1396671918","1528954321","1576636018","1712850041","1917822898","1960176959","1984042953","1152186262","1188870872","1396671918","1396671918","1576636018","1576636018","1917822898","1917822898","1960176959","1984042953","1188870872","1396671918","1528954321","1576636018","1712850041","1917822898","1960176959"};


        for(int i=0;i<128;i++){
            if(leaders[i].equals(brokerId)){
                partitions.add(i);
            }
        }
        return partitions;
    }

    public static List<String> readFileInList(String fileName)
    {
        List<String> lines = Collections.emptyList();
        try{
            lines = Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

    public static int partition(byte[] keyBytes) {
        int numPartitions = 128;
        int index = keyBytes.length <= 32 ? 0 : keyBytes.length - 32;
        return org.apache.kafka.common.utils.Utils.toPositive(org.apache.kafka.common.utils.Utils.murmur2(keyBytes, index)) % numPartitions;
    }

}
