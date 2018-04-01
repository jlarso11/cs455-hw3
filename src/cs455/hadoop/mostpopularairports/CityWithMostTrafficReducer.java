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

    private static Map<String, Map<Text, IntWritable>> countMap = new HashMap<>();
    private static Map<Text, IntWritable> overallCount = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int count = 0;

        for(IntWritable val : values){
            count += val.get();
        }

        IntWritable totalCount = new IntWritable(count);

        String[] keySplit = key.toString().split(",");
        if(keySplit.length > 1) {
            String year = keySplit[0];

            if(!countMap.containsKey(year)) {
                Map<Text, IntWritable> localCountMap = new HashMap<>();
                countMap.put(year, localCountMap);
            }

            countMap.get(year).put(new Text(keySplit[1]), totalCount);

            if(overallCount.containsKey(new Text(keySplit[1]))) {
                count += overallCount.get(new Text(keySplit[1])).get();
            }

            overallCount.put(new Text(keySplit[1]), new IntWritable(count));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        context.write(new Text("Overall Results"), new IntWritable(-1));
        Map<Text, IntWritable> overallSortedMap = MapSorts.sortByValues(overallCount, -1);
        int overallCounter = 0;
        for(Map.Entry<Text, IntWritable> yearEntry : overallSortedMap.entrySet()) {
            if(overallCounter == 10) {
                break;
            }
            context.write(yearEntry.getKey(), yearEntry.getValue());
            overallCounter++;
        }

        for(Map.Entry<String, Map<Text, IntWritable>> entry : countMap.entrySet()) {
            Map<Text, IntWritable> sortedMap = MapSorts.sortByValues(entry.getValue(), -1);

            context.write(new Text(entry.getKey()), new IntWritable(-1));
            int counter = 0;
            for(Map.Entry<Text, IntWritable> yearEntry : sortedMap.entrySet()) {
                if(counter == 10) {
                    break;
                }
                context.write(yearEntry.getKey(), yearEntry.getValue());
                counter++;
            }
        }
    }
}
