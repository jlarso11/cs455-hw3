package cs455.hadoop.olderplanes;

import cs455.hadoop.utils.TypeCheckUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class OldPlaneDelaysReducer extends Reducer<Text, Text, Text, LongWritable> {

    private Map<Text, Integer> countMap = new HashMap<>();
    private Map<Text, Integer> airplanes = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) {

        for(Text value : values) {
            String[] fileSplit = value.toString().split("-");
            if("f2".equalsIgnoreCase(fileSplit[0]) && TypeCheckUtil.isInteger(fileSplit[1])) {
                int year = Integer.parseInt(fileSplit[1]);
                airplanes.put(new Text(key), year);
            } else {
                String[] valueSplit = fileSplit[1].split(",");
                if(valueSplit.length > 1 && TypeCheckUtil.isInteger(valueSplit[0]) && TypeCheckUtil.isInteger(valueSplit[1])) {
                    int yearOfFlight = Integer.parseInt(valueSplit[0]);
                    boolean hadDelay = Integer.parseInt(valueSplit[1]) > 0;

                    Text countMapKey;
                    if(hadDelay) {
                        countMapKey = new Text(yearOfFlight + ",delay," + key);
                    } else {
                        countMapKey = new Text(yearOfFlight + ",ontime," + key);
                    }

                    int totalFlightsToAdd = 1;
                    if(countMap.containsKey(countMapKey)) {
                        totalFlightsToAdd += countMap.get(countMapKey);
                    }
                    countMap.put(countMapKey, totalFlightsToAdd);
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        long delayedFlightsOver20Years = 0;
        long totalFlightsOver20Years = 0;
        long totalFlightsUnder20Years = 0;
        long delayedFlightsUnder20Years  =0;

        for(Map.Entry<Text, Integer> entry : countMap.entrySet()) {
            String key = entry.getKey().toString();
            String[] splitUpKey = key.split(",");
            if(splitUpKey.length == 3 && TypeCheckUtil.isInteger(splitUpKey[0]) && airplanes.containsKey(new Text(splitUpKey[2]))) {
                Text airplane = new Text(splitUpKey[2]);
                int yearOfFlights = Integer.parseInt(splitUpKey[0]);
                if(Math.abs(yearOfFlights - airplanes.get(airplane)) > 20) {
                    totalFlightsOver20Years += entry.getValue();
                    if("delay".equalsIgnoreCase(splitUpKey[1])) {
                        delayedFlightsOver20Years += entry.getValue();
                    }
                } else {
                    totalFlightsUnder20Years += entry.getValue();
                    if("delay".equalsIgnoreCase(splitUpKey[1])) {
                        delayedFlightsUnder20Years += entry.getValue();
                    }
                }
            }
        }

        context.write(new Text("Total Delayed flights on old planes"), new LongWritable(delayedFlightsOver20Years));
        context.write(new Text("Total flights on old planes"), new LongWritable(totalFlightsOver20Years));

        if(totalFlightsOver20Years > 0) {
            long average = (long)((double) delayedFlightsOver20Years /totalFlightsOver20Years * 100);
            context.write(new Text("Percent of delayed flights on old planes"), new LongWritable(average));
        }


        context.write(new Text("Total Delayed flights on newer planes"), new LongWritable(delayedFlightsUnder20Years));
        context.write(new Text("Total flights on newer planes"), new LongWritable(totalFlightsUnder20Years));

        if(totalFlightsUnder20Years > 0) {
            long average = (long)((double) delayedFlightsUnder20Years /totalFlightsUnder20Years * 100);
            context.write(new Text("Percent of delayed flights on newer planes"), new LongWritable(average));
        }
    }
}
