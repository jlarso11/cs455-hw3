package cs455.hadoop.utils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.*;

public class MapSorts {

    public static Map<Text,IntWritable> sortByValues(Map<Text,IntWritable> map, int sortDirection){

        List<Map.Entry<Text,IntWritable>> entries = new LinkedList<>(map.entrySet());

        Collections.sort(entries, (o1, o2) -> sortDirection*o1.getValue().compareTo(o2.getValue()));

        Map<Text,IntWritable> sortedMap = new LinkedHashMap<>();

        for(Map.Entry<Text,IntWritable> entry: entries){
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

}
