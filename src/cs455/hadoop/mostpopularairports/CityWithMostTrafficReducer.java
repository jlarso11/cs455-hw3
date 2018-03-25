package cs455.hadoop.mostpopularairports;

import cs455.hadoop.utils.MapSorts;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class CityWithMostTrafficReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static Map<Text, IntWritable> countMap = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        for(IntWritable val : values){
            count += val.get();
        }

        IntWritable totalCount = new IntWritable(count);

        countMap.put(new Text(key), totalCount);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<Text, IntWritable> sortedMap = MapSorts.sortByValues(countMap, -1);

        for(Map.Entry<Text, IntWritable> entry : sortedMap.entrySet()) {
            context.write(entry.getKey(), entry.getValue());
        }
    }
}
