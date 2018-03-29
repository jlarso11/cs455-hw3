package cs455.hadoop.mindelay;

import cs455.hadoop.utils.MapSorts;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class BestTimeToFlyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static Map<Text, IntWritable> dayCountMap = new HashMap<>();
    private static Map<Text, IntWritable> hourCountMap = new HashMap<>();
    private static Map<Text, IntWritable> monthCountMap = new HashMap<>();
    MultipleOutputs<Text, IntWritable> mos;

    @Override
    public void setup(Context context) {
        mos = new MultipleOutputs(context);
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

        String[] keySplit = key.toString().split("-");
        if("hour".equalsIgnoreCase(keySplit[0])) {
            hourCountMap.put(new Text(keySplit[1]), average);
        } else if("day".equalsIgnoreCase(keySplit[0])) {
            dayCountMap.put(new Text(keySplit[1]), average);
        } else if("month".equalsIgnoreCase(keySplit[0])) {
            monthCountMap.put(new Text(keySplit[1]), average);
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<Text, IntWritable> sortedDayAverage = MapSorts.sortByValues(dayCountMap, 1);

        for(Map.Entry<Text, IntWritable> entry : sortedDayAverage.entrySet()) {
            mos.write("bestDay", entry.getKey(), entry.getValue());
        }


        Map<Text, IntWritable> sortedMonthAverage = MapSorts.sortByValues(monthCountMap, 1);

        for(Map.Entry<Text, IntWritable> entry : sortedMonthAverage.entrySet()) {
            mos.write("bestMonth", entry.getKey(), entry.getValue());
        }

        Map<Text, IntWritable> sortedHourAverage = MapSorts.sortByValues(hourCountMap, 1);

        for(Map.Entry<Text, IntWritable> entry : sortedHourAverage.entrySet()) {
            mos.write("bestHour", entry.getKey(), entry.getValue());
        }
    }
}
