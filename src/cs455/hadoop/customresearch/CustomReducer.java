package cs455.hadoop.customresearch;

import cs455.hadoop.utils.MapSorts;
import cs455.hadoop.utils.TypeCheckUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CustomReducer extends Reducer<Text, Text, Text, IntWritable> {

    private static Map<Text, Map<Text, FlightCounts>> countMap = new HashMap<>();
    private static Map<Text, FlightCounts> overallCount = new HashMap<>();
    private static Map<Text, Text> carrierNames = new HashMap<>();
    private static Map<Text, Text> airportNames = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value : values){
            String[] fileSplit = value.toString().split("-");
            if("f2".equalsIgnoreCase(fileSplit[0])) {
                carrierNames.put(new Text(key), new Text(fileSplit[1]));
            }
            else if("f3".equalsIgnoreCase(fileSplit[0])) {
                airportNames.put(new Text(key), new Text(fileSplit[1]));
            }
            else if(fileSplit.length == 2) {
                String[] valueSplit = fileSplit[1].split(",");
                if(valueSplit.length == 3 && TypeCheckUtil.isInteger(valueSplit[0]) && TypeCheckUtil.isInteger(valueSplit[2])) {
                    Text mapKey = new Text(valueSplit[1]);
                    int totalDelayForFlight = Integer.parseInt(valueSplit[0]);
                    int totalFlightCount = Integer.parseInt(valueSplit[2]);

                    //total count addition
                    if(!overallCount.containsKey(mapKey)) {
                        overallCount.put(mapKey, new FlightCounts(totalFlightCount, totalDelayForFlight));
                    } else {
                        overallCount.get(mapKey).incrementValues(totalDelayForFlight, totalFlightCount);
                    }

                    //each carrier at each airport addition
                    Text airportCode = new Text(valueSplit[1]);
                    Text carrierCode = new Text(key);

                    if(!countMap.containsKey(airportCode)) {
                        Map<Text, FlightCounts> carrierInfo = new HashMap<>();
                        carrierInfo.put(carrierCode, new FlightCounts(totalFlightCount, totalDelayForFlight));
                        countMap.put(airportCode, carrierInfo);
                    } else { // airport is already in the map
                        Map<Text, FlightCounts> foundMap = countMap.get(airportCode);

                        if(!foundMap.containsKey(carrierCode)) {
                            foundMap.put(carrierCode, new FlightCounts(1, totalDelayForFlight));
                        } else { //carrier is already in there.
                            foundMap.get(carrierCode).incrementValues(totalDelayForFlight, totalFlightCount);
                        }
                    }

                }

            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Map<Text, Map<Text, IntWritable>> averageMap = new HashMap<>();

        for(Map.Entry<Text, Map<Text, FlightCounts>> entry : countMap.entrySet()) {
            Map<Text, IntWritable> getAverages = new HashMap<>();
            for(Map.Entry<Text, FlightCounts> carrierEntries : entry.getValue().entrySet()) {
                getAverages.put(carrierEntries.getKey(), new IntWritable(carrierEntries.getValue().getAverage()));
            }
            averageMap.put(entry.getKey(), getAverages);
        }

        Map<Text, IntWritable> averageTotalMap = new HashMap<>();
        for(Map.Entry<Text, FlightCounts> entry : overallCount.entrySet()) {
            averageTotalMap.put(entry.getKey(), new IntWritable(entry.getValue().getAverage()));
        }

        Map<Text, IntWritable> sortedTotalMap = MapSorts.sortByValues(averageTotalMap, -1);


        for(Map.Entry<Text, IntWritable> entry : sortedTotalMap.entrySet()) {

            context.write(airportNames.get(entry.getKey()), entry.getValue());

            Map<Text, IntWritable> carrierStats = averageMap.get(entry.getKey());

            double standardDeviation = getStandardDeviation(carrierStats, entry.getValue().get());

            Map<Text, IntWritable> sortedCarrierMap = MapSorts.sortByValues(carrierStats, -1);
            for(Map.Entry<Text, IntWritable> sortedCarrierEntry : sortedCarrierMap.entrySet()) {
                context.write(new Text("\t" + carrierNames.get(sortedCarrierEntry.getKey())), sortedCarrierEntry.getValue());
                double numberOfStdDevAway = Math.abs(entry.getValue().get() - sortedCarrierEntry.getValue().get()) / standardDeviation;
                context.write(new Text("\t\t std dev from mean"), new IntWritable((int) Math.round(numberOfStdDevAway)));
            }

        }
    }

    private double getStandardDeviation(Map<Text, IntWritable> carrierStats, int overallAverage) {
        int sumOfSquares = 0;
        for(Map.Entry<Text, IntWritable> entry : carrierStats.entrySet()) {
            sumOfSquares += Math.abs(entry.getValue().get() - overallAverage);
        }

        return Math.round(Math.sqrt(sumOfSquares));
    }
}
