package cs455.hadoop.cityweatherdelay;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class TotalCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        // calculate the total count
        for(IntWritable val : values){
            count += val.get();
        }
        context.write(key, new IntWritable(count));
    }

//    public Set<Text> getKeysByValue(IntWritable value) {
//        Set<Text> keys = new HashSet<Text>();
//        for (Map.Entry<Text, IntWritable> entry : countMap.entrySet()) {
//            if (value.equals(entry.getValue())) {
//                keys.add(entry.getKey());
//            }
//        }
//        return keys;
//    }
}
