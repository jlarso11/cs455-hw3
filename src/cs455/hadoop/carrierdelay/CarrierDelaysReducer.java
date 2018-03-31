package cs455.hadoop.carrierdelay;

import cs455.hadoop.utils.MapSorts;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class CarrierDelaysReducer extends Reducer<Text, Text, Text, Text> {

    MultipleOutputs<Text, IntWritable> mos;

    private static Map<Text, IntWritable> totalCountMap = new HashMap<>();
    private static Map<Text, IntWritable> totalMinuteMap = new HashMap<>();
    private static Map<Text, IntWritable> totalAverageMap = new HashMap<>();

    @Override
    public void setup(Context context) {
        mos = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        int totalMinutes = 0;
        String carrierName = "";

        for(Text value : values){
            String[] fileSplit = value.toString().split("-");
            if("f2".equalsIgnoreCase(fileSplit[0])) {
                carrierName = fileSplit[1];
            } else {
                total++;
                totalMinutes += Integer.parseInt(fileSplit[1]);
            }
        }

        if(!"".equals(carrierName)) {
            Text carrierKey = new Text(carrierName);
            IntWritable totalCount = new IntWritable(total);
            IntWritable totalMinutesCount = new IntWritable(totalMinutes);

            totalCountMap.put(carrierKey, totalCount);
            totalMinuteMap.put(carrierKey, totalMinutesCount);

            int average = totalMinutes/1;
            if(total > 0) {
                average = totalMinutes/total;
            }

            totalAverageMap.put(carrierKey, new IntWritable(average));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<Text, IntWritable> sortedCountMap = MapSorts.sortByValues(totalCountMap, -1);
        Map<Text, IntWritable> sortedMinuteCount = MapSorts.sortByValues(totalMinuteMap, -1);
        Map<Text, IntWritable> sortedAverageCount = MapSorts.sortByValues(totalAverageMap, -1);

        for(Map.Entry<Text, IntWritable> entry : sortedCountMap.entrySet()) {
            mos.write("totalDelays", entry.getKey(), entry.getValue());
        }

        for(Map.Entry<Text, IntWritable> entry : sortedMinuteCount.entrySet()) {
            mos.write("totalDelayMinutes", entry.getKey(), entry.getValue());
        }

        for(Map.Entry<Text, IntWritable> entry : sortedAverageCount.entrySet()) {
            mos.write("averageDelay", entry.getKey(), entry.getValue());
        }

        mos.close();
    }
}
