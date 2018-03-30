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

        int minValue = MapSorts.getMinimum(dayCountMap);
        int maxValue = MapSorts.getMaximum(dayCountMap);

        for(Map.Entry<Text, IntWritable> entry : sortedDayAverage.entrySet()) {
            if(entry.getValue().get() == minValue) {
                mos.write("bestDay", entry.getKey(), entry.getValue());
            } else if(entry.getValue().get() == maxValue) {
                mos.write("worstDay", entry.getKey(), entry.getValue());
            }
        }

        Map<Text, IntWritable> sortedMonthAverage = MapSorts.sortByValues(monthCountMap, 1);

        minValue = MapSorts.getMinimum(monthCountMap);
        maxValue = MapSorts.getMaximum(monthCountMap);

        for(Map.Entry<Text, IntWritable> entry : sortedMonthAverage.entrySet()) {
            if(entry.getValue().get() == minValue) {
                mos.write("bestMonth", entry.getKey(), entry.getValue());
            } else if(entry.getValue().get() == maxValue) {
                mos.write("worstMonth", entry.getKey(), entry.getValue());
            }

        }

        Map<Text, IntWritable> sortedHourAverage = MapSorts.sortByValues(hourCountMap, 1);

        minValue = MapSorts.getMinimum(hourCountMap);
        maxValue = MapSorts.getMaximum(hourCountMap);

        for(Map.Entry<Text, IntWritable> entry : sortedHourAverage.entrySet()) {
            if(entry.getValue().get() == minValue) {
                mos.write("bestHour", entry.getKey(), entry.getValue());
            } else if(entry.getValue().get() == maxValue) {
                mos.write("worstHour", entry.getKey(), entry.getValue());
            }
        }

        mos.close();
    }
}
