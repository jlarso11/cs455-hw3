package cs455.hadoop.mainjob;

import cs455.hadoop.utils.TypeCheckUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MainCombiner extends Reducer<Text, Text, Text, Text> {

    private static Map<Text, IntWritable> totalPopularityCounts = new HashMap<>();
    private static Map<Text, IntWritable> totalCountMap = new HashMap<>();
    private static Map<Text, IntWritable> totalMinuteMap = new HashMap<>();
    private static Map<Text, IntWritable> airportFlights = new HashMap<>();
    private static Map<Text, IntWritable> planeDelayed = new HashMap<>();
    private static Map<Text, IntWritable> planeTotal = new HashMap<>();
    private static int[] dayCount = new int[7];
    private static int[] dayDelayCount = new int[7];
    private static int[] hourCount = new int[24];
    private static int[] hourDelayCount = new int[24];
    private static int[] monthCount = new int[12];
    private static int[] monthDelayCount = new int[12];

    private void processCarrierFlight(Text carrier, int carrierDelay) {
        if(carrierDelay > 0) {
            int totalCount = 1;
            if(totalCountMap.containsKey(carrier)) {
                totalCount += totalCountMap.get(carrier).get();
            }
            totalCountMap.put(carrier, new IntWritable(totalCount));

            if(totalMinuteMap.containsKey(carrier)) {
                carrierDelay += totalMinuteMap.get(carrier).get();
            }
            totalMinuteMap.put(carrier, new IntWritable(carrierDelay));
        }
    }

    private void processCityDelay(Text origin, Text destination ) {
        int delayedFlights = 1;
        if(airportFlights.containsKey(origin)) {
            delayedFlights += airportFlights.get(origin).get();
        }
        airportFlights.put(new Text(origin), new IntWritable(delayedFlights));

        delayedFlights = 1;
        if(airportFlights.containsKey(destination)) {
            delayedFlights += airportFlights.get(destination).get();
        }
        airportFlights.put(new Text(destination), new IntWritable(delayedFlights));

    }

    private void groupByYearAndTailNum(Text key, String tailNum, int totalDelay) {
        String[] timeOfFlightSplit = key.toString().split(",");
        if(timeOfFlightSplit.length == 4) {
            Text mapKey = new Text(timeOfFlightSplit[0] + "," + tailNum);
            int totalFlights = 1;
            int totalDelayedFlights = 0;
            if(totalDelay > 0) {
                totalDelayedFlights = 1;
            }
            if(planeTotal.containsKey(mapKey)) {
                totalFlights += planeTotal.get(mapKey).get();
            }
            if(planeDelayed.containsKey(mapKey)) {
                totalDelayedFlights += planeDelayed.get(mapKey).get();
            }

            planeTotal.put(mapKey, new IntWritable(totalFlights));
            planeDelayed.put(mapKey, new IntWritable(totalDelayedFlights));
        }
    }

    private void findBestTimeToFly(Text key, int totalDelay) {
        String[] timeOfFlightSplit = key.toString().split(",");
        if(timeOfFlightSplit.length == 4 && TypeCheckUtil.isInteger(timeOfFlightSplit[3]) && TypeCheckUtil.isInteger(timeOfFlightSplit[1]) && TypeCheckUtil.isInteger(timeOfFlightSplit[2])) {
            int month = (Integer.parseInt(timeOfFlightSplit[1]) - 1) % 12;
            int dayOfWeek = (Integer.parseInt(timeOfFlightSplit[2]) -1) % 7;
            int hour = Integer.parseInt(timeOfFlightSplit[3]) % 24;

            dayCount[dayOfWeek]++;
            dayDelayCount[dayOfWeek] += totalDelay;

            monthCount[month]++;
            monthDelayCount[month] += totalDelay;

            hourCount[hour]++;
            hourDelayCount[hour] += totalDelay;
        }
    }

    private void processCityPopularity(Text origin, Text destination, Text key) {
        String[] timeOfFlightSplit = key.toString().split(",");
        String year = timeOfFlightSplit[0];
        Text mapKey = new Text(year + "," + origin);
        Text mapKeyDest = new Text(year + "," + destination);

        int originCount = 1;
        int destinationCount = 1;
        if(totalPopularityCounts.containsKey(mapKey)) {
            originCount += totalPopularityCounts.get(mapKey).get();
        }
        totalPopularityCounts.put(mapKey, new IntWritable(originCount));

        if(totalPopularityCounts.containsKey(mapKeyDest)) {
            destinationCount += totalPopularityCounts.get(mapKeyDest).get();
        }
        totalPopularityCounts.put(mapKeyDest, new IntWritable(destinationCount));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value : values){
            String[] fileSplit = value.toString().split("-");
            if("f2".equalsIgnoreCase(fileSplit[0])) {
                context.write(new Text(key), new Text(value));
            }
            else if("f1".equalsIgnoreCase(fileSplit[0])) {
                context.write(new Text(key), new Text(value));
            }
            else if ("f3".equalsIgnoreCase(fileSplit[0])) {
                if(TypeCheckUtil.isInteger(fileSplit[1])) {
                    context.write(new Text(key), new Text(value));
                }
            }
            else {
                String[] importantValuesFromEachFlight = fileSplit[1].split(",");
                 // Text valueToPassAlong = new Text("main-" + origin + "," + destination + "," + carrier + "," + tailNum + ","
                //                    + String.valueOf(totalDelay) + "," + String.valueOf(carrierDelay) weatherDelay);

                if(importantValuesFromEachFlight.length == 7 && TypeCheckUtil.isInteger(importantValuesFromEachFlight[4])
                        && TypeCheckUtil.isInteger(importantValuesFromEachFlight[5])  && TypeCheckUtil.isInteger(importantValuesFromEachFlight[6])) {
                    Text origin = new Text(importantValuesFromEachFlight[0]);
                    Text destination = new Text(importantValuesFromEachFlight[1]);
                    Text carrier = new Text(importantValuesFromEachFlight[2]);
                    String tailNum = importantValuesFromEachFlight[3];
                    int totalDelay = Integer.parseInt(importantValuesFromEachFlight[4]);
                    int carrierDelay = Integer.parseInt(importantValuesFromEachFlight[5]);
                    int weatherDelay = Integer.parseInt(importantValuesFromEachFlight[6]);

                    this.processCarrierFlight(carrier, carrierDelay);

                    if(weatherDelay > 0) {
                        this.processCityDelay(origin, destination);
                    }

                    this.processCityPopularity(origin, destination, key);

                    this.groupByYearAndTailNum(key, tailNum, totalDelay);

                    this.findBestTimeToFly(key, totalDelay);

                }

            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(Map.Entry<Text, IntWritable> entry : totalCountMap.entrySet()) {
            if(totalMinuteMap.containsKey(entry.getKey())) {
                context.write(new Text("carrierFlightsDelayed~" + entry.getKey()), new Text(entry.getValue().toString()));
                context.write(new Text("carrierMinutesDelayed~" + entry.getKey()), new Text(totalMinuteMap.get(entry.getKey()).toString()));
            }
        }

        for(Map.Entry<Text, IntWritable> entry : airportFlights.entrySet()) {
            context.write(new Text("cityWeatherDelay~"+entry.getKey()), new Text(entry.getValue().toString()));
        }

        for(Map.Entry<Text, IntWritable> entry : planeDelayed.entrySet()) {
            context.write(new Text("planeDelayedCount~"+entry.getKey()), new Text(entry.getValue().toString()));
        }

        for(Map.Entry<Text, IntWritable> entry : planeTotal.entrySet()) {
            context.write(new Text("planeTotalCount~"+entry.getKey()), new Text(entry.getValue().toString()));
        }

        for(Map.Entry<Text, IntWritable> entry : totalPopularityCounts.entrySet()) {
            context.write(new Text("popularStop~" + entry.getKey()), new Text(entry.getValue().toString()));
        }

        for(int i = 0; i < dayCount.length; i++) {
            context.write(new Text("dayFlightCount~" + i), new Text(String.valueOf(dayCount[i])));
            context.write(new Text("dayFlightDelayCount~" + i), new Text(String.valueOf(dayDelayCount[i])));
        }

        for(int i = 0; i < monthCount.length; i++) {
            context.write(new Text("monthFlightCount~" + i), new Text(String.valueOf(monthCount[i])));
            context.write(new Text("monthFlightDelayCount~" + i), new Text(String.valueOf(monthDelayCount[i])));
        }

        for(int i = 0; i < hourCount.length; i++) {
            context.write(new Text("hourFlightCount~" + i), new Text(String.valueOf(hourCount[i])));
            context.write(new Text("hourFlightDelayCount~" + i), new Text(String.valueOf(hourDelayCount[i])));
        }

    }

}
