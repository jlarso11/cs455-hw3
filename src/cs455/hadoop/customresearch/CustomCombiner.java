package cs455.hadoop.customresearch;

import cs455.hadoop.utils.TypeCheckUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CustomCombiner extends Reducer<Text, Text, Text, Text> {

    private static Map<Text, IntWritable> overallCount = new HashMap<>();
    private static Map<Text, IntWritable> overallDelayCount = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int counter = 0;
        int totalDelay = 0;
        for(Text value : values){
            String[] fileSplit = value.toString().split("-");
            if("f2".equalsIgnoreCase(fileSplit[0])) {
                context.write(new Text(key), new Text(value));
            }
            else if ("f3".equalsIgnoreCase(fileSplit[0])) {
                context.write(new Text(key), new Text(value));

            }
            else if ("f1".equalsIgnoreCase(fileSplit[0]) && fileSplit.length == 2 && TypeCheckUtil.isInteger(fileSplit[1])) {
                counter++;
                totalDelay += Integer.parseInt(fileSplit[1]);
            }
        }

        if(counter > 0 ) {
            overallCount.put(new Text(key), new IntWritable(counter));
            overallDelayCount.put(new Text(key), new IntWritable(totalDelay));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(Map.Entry<Text, IntWritable> entry : overallCount.entrySet()) {
            if(overallDelayCount.containsKey(entry.getKey())) {
                String[] keySplit = entry.getKey().toString().split(",");
                if(keySplit.length == 2) {
                    Text carrierCode = new Text(keySplit[0]);
                    Text airport = new Text(keySplit[1]);
                    IntWritable totalCount = entry.getValue();
                    IntWritable totalDelaySum = overallDelayCount.get(entry.getKey());

                    context.write(carrierCode, new Text("F1-" + totalDelaySum.toString() + "," + airport + "," + totalCount.toString()));

                }

            }
        }
    }

}
