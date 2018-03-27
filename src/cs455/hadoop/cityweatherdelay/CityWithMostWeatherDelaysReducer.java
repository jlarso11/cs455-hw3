package cs455.hadoop.cityweatherdelay;

import cs455.hadoop.utils.MapSorts;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static cs455.hadoop.utils.TypeCheckUtil.isInteger;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class CityWithMostWeatherDelaysReducer extends Reducer<Text, Text, Text, IntWritable> {

    private static Map<Text, IntWritable> countMap = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String city = "";
        int delayedFlights = 0;
        for(Text value : values) {
            String[] fileSplit = value.toString().split("-");

            if("f2".equalsIgnoreCase(fileSplit[0])) {
                String[] valueSplit = fileSplit[1].split(",");
                if(valueSplit.length > 3) {
                    city = valueSplit[2].replace("\"", "");
                } else {
                    return;
                }
            } else {
                delayedFlights++;
            }
        }

        if(countMap.containsKey(new Text(city))) {
            delayedFlights += countMap.get(new Text(city)).get();
        }

        countMap.put(new Text(city), new IntWritable(delayedFlights));

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<Text, IntWritable> sortedMap = MapSorts.sortByValues(countMap, -1);

        int counter = 0;
        for(Map.Entry<Text, IntWritable> entry : sortedMap.entrySet()) {
            if(counter  == 10) {
                return;
            }
            context.write(entry.getKey(), entry.getValue());
            counter++;
        }
    }
}
