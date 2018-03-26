package cs455.hadoop.utils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class AverageCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalNumber = 0;
        int count = 0;
        // calculate the total count
        for(IntWritable val : values){
            count += val.get();
            totalNumber++;
        }
        context.write(key, new IntWritable(count/totalNumber));
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
