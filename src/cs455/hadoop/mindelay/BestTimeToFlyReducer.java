package cs455.hadoop.mindelay;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class BestTimeToFlyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static Map<Text, IntWritable> countMap = new HashMap<>();

    public static Map<Text,IntWritable> sortByValues(Map<Text,IntWritable> map, int sortDirection){

        List<Map.Entry<Text,IntWritable>> entries = new LinkedList<>(map.entrySet());

        Collections.sort(entries, (o1, o2) -> sortDirection*o1.getValue().compareTo(o2.getValue()));

        Map<Text,IntWritable> sortedMap = new LinkedHashMap<>();

        for(Map.Entry<Text,IntWritable> entry: entries){
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalNumber = 0;
        int count = 0;

        for(IntWritable val : values){
            count += val.get();
            totalNumber++;
        }

        IntWritable average = new IntWritable(count/totalNumber);

        countMap.put(new Text(key), average);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<Text, IntWritable> sortedMap = sortByValues(countMap, 1);

        for(Map.Entry<Text, IntWritable> entry : sortedMap.entrySet()) {
            context.write(entry.getKey(), entry.getValue());
        }
    }
}
