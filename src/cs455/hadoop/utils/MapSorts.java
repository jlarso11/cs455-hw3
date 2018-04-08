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

    public static int getMinimum(Map<Text,IntWritable> map) {
        int min = Integer.MAX_VALUE;

        for (Map.Entry<Text,IntWritable> entry : map.entrySet())
        {
            if(min > entry.getValue().get()) {
                min = entry.getValue().get();
            }
        }
        return min;
    }

    public static int getMaximum(Map<Text,IntWritable> map) {
        int max = Integer.MIN_VALUE;

        for (Map.Entry<Text,IntWritable> entry : map.entrySet())
        {
            if(max < entry.getValue().get()) {
                max = entry.getValue().get();
            }
        }
        return max;
    }

    public static List<Integer> getIndexOfLargest( int[] array ) {
        int[] tempArray = array.clone();
        Arrays.sort(tempArray );
        int max = tempArray[tempArray .length - 1];

        List<Integer> allTheHighestValues = new LinkedList<>();
        for(int i = 0; i < array.length; i++) {
            if(array[i] == max) {
                allTheHighestValues.add(i);
            }
        }
        return allTheHighestValues; // position of the first largest found
    }

    public static List<Integer> getIndexOfLowest( int[] array ) {
        int[] tempArray = array.clone();
        Arrays.sort(tempArray );
        int max = tempArray[0];

        List<Integer> allTheLowestValues = new LinkedList<>();
        for(int i = 0; i < array.length; i++) {
            if(array[i] == max) {
                allTheLowestValues.add(i);
            }
        }
        return allTheLowestValues; // position of the first largest found
    }

}
