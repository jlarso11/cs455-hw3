package cs455.hadoop.mainjob;

import cs455.hadoop.utils.MapSorts;
import cs455.hadoop.utils.TypeCheckUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.*;

public class MainReducer extends Reducer<Text, Text, Text, IntWritable> {

    private static Map<Text, Text> carrierNames = new HashMap<>();
    private static Map<Text, Text> airportNames = new HashMap<>();
    private static Map<Text, IntWritable> planeData = new HashMap<>();
    private static Map<Text, IntWritable> carrierTotalCountMap = new HashMap<>();
    private static Map<Text, IntWritable> carrierTotalMinuteMap = new HashMap<>();
    private static Map<Text, IntWritable> carrierAverageMinuteMap = new HashMap<>();
    private static Map<Text, IntWritable> cityWeatherDelays = new HashMap<>();
    private static Map<Text, IntWritable> delayedPlaneCounts = new HashMap<>();
    private static Map<Text, IntWritable> totalPlaneCounts = new HashMap<>();
    private static Map<String, Map<Text, IntWritable>> popularAirports = new HashMap<>();
    private static Map<Text, IntWritable> totalPopularAirports = new HashMap<>();
    private static int[] dayCount = new int[7];
    private static int[] dayDelayCount = new int[7];
    private static int[] dayAverage = new int[7];
    private static int[] hourCount = new int[24];
    private static int[] hourDelayCount = new int[24];
    private static int[] hourAverage = new int[24];
    private static int[] monthCount = new int[12];
    private static int[] monthDelayCount = new int[12];
    private static int[] monthAverage = new int[12];
    private static String[] days =  {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
    private static String[] months = {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
    private MultipleOutputs<Text, IntWritable> mos;

    @Override
    public void setup(Context context) {
        mos = new MultipleOutputs(context);
    }

    private void processCarrierFlightDelayedCount(Text carrier, int totalCount) {
        if(carrierTotalCountMap.containsKey(carrier)) {
            totalCount += carrierTotalCountMap.get(carrier).get();
        }
        carrierTotalCountMap.put(carrier, new IntWritable(totalCount));
    }

    private void processCarrierFlightDelayedMinutes(Text carrier, int delayedMinutes) {
        if(carrierTotalMinuteMap.containsKey(carrier)) {
            delayedMinutes += carrierTotalMinuteMap.get(carrier).get();
        }
        carrierTotalMinuteMap.put(carrier, new IntWritable(delayedMinutes));
    }

    private void averageCarrierDelays() {
        for(Map.Entry<Text, IntWritable> entry : carrierTotalCountMap.entrySet()) {
            if(carrierTotalMinuteMap.containsKey(entry.getKey())) {
                int totalCount = entry.getValue().get();
                int totalDelay = carrierTotalMinuteMap.get(entry.getKey()).get();
                int average = (int) Math.round((double)totalDelay/totalCount);
                carrierAverageMinuteMap.put(new Text(entry.getKey()), new IntWritable(average));
            }
        }
    }

    private void processCityWeatherDelay(Text airport, int count) {
        if(cityWeatherDelays.containsKey(airport)) {
            count += cityWeatherDelays.get(airport).get();
        }
        cityWeatherDelays.put(airport, new IntWritable(count));
    }

    private void combineAirportDelaysToCities() {
        Map<Text, IntWritable> actualCityDelays = new HashMap<>();
        for(Map.Entry<Text, IntWritable> entry : cityWeatherDelays.entrySet()) {
            int count = entry.getValue().get();
            String[] airportData = airportNames.get(entry.getKey()).toString().split(",");
            if(airportData.length == 2) {
                String cityKey = airportData[1];
                if(actualCityDelays.containsKey(cityKey)) {
                    count += actualCityDelays.get(cityKey).get();
                }
                actualCityDelays.put(new Text(cityKey), new IntWritable(count));
            }
        }

        cityWeatherDelays = actualCityDelays;
    }

    private void processPlaneDelayedCount(Text yearAndTailNum, int delayedCount) {
        if(delayedPlaneCounts.containsKey(yearAndTailNum)) {
            delayedCount += delayedPlaneCounts.get(yearAndTailNum).get();
        }
        delayedPlaneCounts.put(yearAndTailNum, new IntWritable(delayedCount));
    }

    private void processPlaneTotalCount(Text yearAndTailNum, int totalCount) {
        if(totalPlaneCounts.containsKey(yearAndTailNum)) {
            totalCount += delayedPlaneCounts.get(yearAndTailNum).get();
        }
        totalPlaneCounts.put(yearAndTailNum, new IntWritable(totalCount));
    }

    private int[] calculateOldPlaneStats() {
        int[] countsToReturn = new int[4]; // totalNewerPlanes, totalNewerPlanesDelayed, totalOlderPlanes, totalOlderPlanesDelayed
        for(Map.Entry<Text, IntWritable> entry : totalPlaneCounts.entrySet()) {
            String[] splitYearAndTailNum = entry.getKey().toString().split(",");
            if(splitYearAndTailNum.length == 2 && planeData.containsKey(new Text(splitYearAndTailNum[1])) && TypeCheckUtil.isInteger(splitYearAndTailNum[0])) {
                int yearPlaneMade = planeData.get(new Text(splitYearAndTailNum[1])).get();
                int yearOfFlight = Integer.parseInt(splitYearAndTailNum[0]);
                if(Math.abs(yearOfFlight - yearPlaneMade) > 20) {
                    countsToReturn[2] += entry.getValue().get();
                    if(delayedPlaneCounts.containsKey(entry.getKey())) {
                        countsToReturn[3] += delayedPlaneCounts.get(entry.getKey()).get();
                    }
                }
                else {
                    countsToReturn[0] += entry.getValue().get();
                    if(delayedPlaneCounts.containsKey(entry.getKey())) {
                        countsToReturn[1] += delayedPlaneCounts.get(entry.getKey()).get();
                    }
                }
            }
        }
        return countsToReturn;
    }

    private void populateAverageForBestTimeToFly() {
        for(int i = 0; i < dayCount.length; i++) {
            if(dayCount[i] > 0) {
                dayAverage[i] = dayDelayCount[i]/dayCount[i];
            }
        }

        for(int i = 0; i < monthCount.length; i++) {
            if(monthCount[i] > 0) {
                monthAverage[i] = monthDelayCount[i]/monthCount[i];
            }
        }

        for(int i = 0; i < hourCount.length; i++) {
            if(hourCount[i] > 0) {
                hourAverage[i] = hourDelayCount[i]/hourCount[i];
            }
        }
    }

    private void handlePopularStopData(Text key, int totalCount) {
        String[] splitYearAndAirport = key.toString().split(",");
        if(splitYearAndAirport.length == 2) {
            int cityCount = totalCount;
            Text cityKey = new Text(splitYearAndAirport[1]);
            if(totalPopularAirports.containsKey(cityKey)) {
                cityCount += totalPopularAirports.get(cityKey).get();
            }
            totalPopularAirports.put(cityKey, new IntWritable(cityCount));

            String yearKey = splitYearAndAirport[0];
            if(!popularAirports.containsKey(yearKey)) {
                Map<Text, IntWritable> localCountMap = new HashMap<>();
                popularAirports.put(yearKey, localCountMap);
            }

            if(popularAirports.get(yearKey).containsKey(cityKey)) {
                totalCount += popularAirports.get(yearKey).get(cityKey).get();
            }
            popularAirports.get(yearKey).put(cityKey, new IntWritable(totalCount));
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) {
        for(Text value : values){
            String[] fileSplit = value.toString().split("-");
            if("f2".equalsIgnoreCase(fileSplit[0])) {
                carrierNames.put(new Text(key), new Text(fileSplit[1]));
            }
            else if("f1".equalsIgnoreCase(fileSplit[0])) {
                airportNames.put(new Text(key), new Text(fileSplit[1]));
            }
            else if ("f3".equalsIgnoreCase(fileSplit[0])) {
                if(TypeCheckUtil.isInteger(fileSplit[1])) {
                    planeData.put(new Text(key), new IntWritable(Integer.parseInt(fileSplit[1])));
                }
            }
            else {
                String[] keyParts = key.toString().split("~");
                if("carrierFlightsDelayed".equalsIgnoreCase(keyParts[0])) {
                    this.processCarrierFlightDelayedCount(new Text(keyParts[1]), Integer.parseInt(value.toString()));
                }
                else if("carrierMinutesDelayed".equalsIgnoreCase(keyParts[0])) {
                    this.processCarrierFlightDelayedMinutes(new Text(keyParts[1]), Integer.parseInt(value.toString()));
                }
                else if("cityWeatherDelay".equalsIgnoreCase(keyParts[0])) {
                    this.processCityWeatherDelay(new Text(keyParts[1]), Integer.parseInt(value.toString()));
                }
                else if("planeDelayedCount".equalsIgnoreCase(keyParts[0])) {
                    this.processPlaneDelayedCount(new Text(keyParts[1]), Integer.parseInt(value.toString()));
                }
                else if("planeTotalCount".equalsIgnoreCase(keyParts[0])) {
                    this.processPlaneTotalCount(new Text(keyParts[1]), Integer.parseInt(value.toString()));
                }
                else if("dayFlightCount".equalsIgnoreCase(keyParts[0]) && TypeCheckUtil.isInteger(keyParts[1])) {
                    int day = Integer.parseInt(keyParts[1]);
                    dayCount[day] += Integer.parseInt(value.toString());
                }
                else if("dayFlightDelayCount".equalsIgnoreCase(keyParts[0]) && TypeCheckUtil.isInteger(keyParts[1])) {
                    int day = Integer.parseInt(keyParts[1]);
                    dayDelayCount[day] += Integer.parseInt(value.toString());
                }
                else if("monthFlightCount".equalsIgnoreCase(keyParts[0]) && TypeCheckUtil.isInteger(keyParts[1])) {
                    int month = Integer.parseInt(keyParts[1]);
                    monthCount[month] += Integer.parseInt(value.toString());
                }
                else if("monthFlightDelayCount".equalsIgnoreCase(keyParts[0]) && TypeCheckUtil.isInteger(keyParts[1])) {
                    int month = Integer.parseInt(keyParts[1]);
                    monthDelayCount[month] += Integer.parseInt(value.toString());
                }
                else if("hourFlightCount".equalsIgnoreCase(keyParts[0]) && TypeCheckUtil.isInteger(keyParts[1])) {
                    int hour = Integer.parseInt(keyParts[1]);
                    hourCount[hour] += Integer.parseInt(value.toString());
                }
                else if("hourFlightDelayCount".equalsIgnoreCase(keyParts[0]) && TypeCheckUtil.isInteger(keyParts[1])) {
                    int hour = Integer.parseInt(keyParts[1]);
                    hourDelayCount[hour] += Integer.parseInt(value.toString());
                }
                else if("popularStop".equalsIgnoreCase(keyParts[0])) {
                    this.handlePopularStopData(new Text(keyParts[1]), Integer.parseInt(value.toString()));
                }
            }
        }
    }

    private void printTopDays() throws IOException, InterruptedException {
        this.populateAverageForBestTimeToFly();

        List<Integer> topDays = MapSorts.getIndexOfLowest(dayAverage);
        List<Integer> worstDays = MapSorts.getIndexOfLargest(dayAverage);

        for(int i = 0; i < topDays.size(); i++) {
            mos.write("bestTimeToFly", new Text(days[topDays.get(i)]), new IntWritable(dayAverage[topDays.get(i)]));
        }

        for(int i = 0; i < worstDays.size(); i++) {
            mos.write("worstTimeToFly", new Text(days[worstDays.get(i)]), new IntWritable(dayAverage[worstDays.get(i)]));
        }

        List<Integer> topHours = MapSorts.getIndexOfLowest(hourAverage);
        List<Integer> worstHours = MapSorts.getIndexOfLargest(hourAverage);

        for(int i = 0; i < topHours.size(); i++) {
            mos.write("bestTimeToFly", new Text(topHours.get(i).toString()), new IntWritable(hourAverage[topHours.get(i)]));
        }

        for(int i = 0; i < worstHours.size(); i++) {
            mos.write("worstTimeToFly", new Text(worstHours.get(i).toString()), new IntWritable(hourAverage[worstHours.get(i)]));
        }

        List<Integer> topMonths = MapSorts.getIndexOfLowest(monthAverage);
        List<Integer> worstMonths = MapSorts.getIndexOfLargest(monthAverage);

        for(int i = 0; i < topMonths.size(); i++) {
            mos.write("bestTimeToFly", new Text(months[topMonths.get(i)]), new IntWritable(monthAverage[topMonths.get(i)]));
        }

        for(int i = 0; i < worstMonths.size(); i++) {
            mos.write("worstTimeToFly", new Text(months[worstMonths.get(i)]), new IntWritable(monthAverage[worstMonths.get(i)]));
        }

    }

    private void printOldPlaneInfo() throws IOException, InterruptedException {
        int[] oldPlaneData = this.calculateOldPlaneStats();

        mos.write("olderPlanes", new Text("Total Delayed flights on old planes"), new IntWritable(oldPlaneData[3]));
        mos.write("olderPlanes", new Text("Total flights on old planes"), new IntWritable(oldPlaneData[2]));

        if(oldPlaneData[2] > 0) {
            int average = (int)((double) oldPlaneData[3] /oldPlaneData[2] * 100);
            mos.write("olderPlanes", new Text("Percent of delayed flights on old planes"), new IntWritable(average));
        }


        mos.write("olderPlanes", new Text("Total Delayed flights on newer planes"), new IntWritable(oldPlaneData[1]));
        mos.write("olderPlanes", new Text("Total flights on newer planes"), new IntWritable(oldPlaneData[0]));

        if(oldPlaneData[0] > 0) {
            int average = (int)((double) oldPlaneData[1] /oldPlaneData[0] * 100);
            mos.write("olderPlanes", new Text("Percent of delayed flights on newer planes"), new IntWritable(average));
        }
    }

    private void printCityDelayInfo() throws IOException, InterruptedException {
        this.combineAirportDelaysToCities();
        Map<Text, IntWritable> sortedCityDelayMap = MapSorts.sortByValues(cityWeatherDelays, -1);

        int counter = 0;
        for(Map.Entry<Text, IntWritable> entry : sortedCityDelayMap.entrySet()) {
            if(counter==10) {
                break;
            }
            mos.write("weatherDelayCities", entry.getKey(), entry.getValue());
            counter++;
        }
    }

    private void printCarrierDelayInfo() throws IOException, InterruptedException {
        this.averageCarrierDelays();

        Map<Text, IntWritable> sortedCountMap = MapSorts.sortByValues(carrierTotalCountMap, -1);
        Map<Text, IntWritable> sortedMinuteCount = MapSorts.sortByValues(carrierTotalMinuteMap, -1);
        Map<Text, IntWritable> sortedAverageCount = MapSorts.sortByValues(carrierAverageMinuteMap, -1);

        int counter = 0;

        mos.write("carrierDelays", new Text("\t\ttotal flight count"), new IntWritable(-1));
        for(Map.Entry<Text, IntWritable> entry : sortedCountMap.entrySet()) {
            if(counter==10) {
                break;
            }
            mos.write("carrierDelays", carrierNames.get(entry.getKey()), entry.getValue());
            counter++;
        }

        mos.write("carrierDelays", new Text("\t\ttotal minute count"), new IntWritable(-1));
        counter = 0;
        for(Map.Entry<Text, IntWritable> entry : sortedMinuteCount.entrySet()) {
            if(counter==10) {
                break;
            }
            mos.write("carrierDelays", carrierNames.get(entry.getKey()), entry.getValue());
            counter++;
        }

        mos.write("carrierDelays", new Text("\t\taverage delay"), new IntWritable(-1));
        counter = 0;
        for(Map.Entry<Text, IntWritable> entry : sortedAverageCount.entrySet()) {
            if(counter==10) {
                break;
            }
            mos.write("carrierDelays", carrierNames.get(entry.getKey()), entry.getValue());
            counter++;
        }
    }

    private void printPopularAirportInfo() throws IOException, InterruptedException {
        mos.write("majorHubs", new Text("Overall Results"), new IntWritable(-1));
        Map<Text, IntWritable> overallSortedMap = MapSorts.sortByValues(totalPopularAirports, -1);

        int overallCounter = 0;
        for(Map.Entry<Text, IntWritable> yearEntry : overallSortedMap.entrySet()) {
            if(overallCounter == 10) {
                break;
            }
            mos.write("majorHubs", yearEntry.getKey(), yearEntry.getValue());
            overallCounter++;
        }

        for(Map.Entry<String, Map<Text, IntWritable>> entry : popularAirports.entrySet()) {
            Map<Text, IntWritable> sortedMap = MapSorts.sortByValues(entry.getValue(), -1);

            mos.write("majorHubs", new Text(entry.getKey()), new IntWritable(-1));
            int counter = 0;
            for(Map.Entry<Text, IntWritable> yearEntry : sortedMap.entrySet()) {
                if(counter == 10) {
                    break;
                }
                mos.write("majorHubs", yearEntry.getKey(), yearEntry.getValue());
                counter++;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.printTopDays();
        this.printOldPlaneInfo();
        this.printCityDelayInfo();
        this.printCarrierDelayInfo();
        this.printPopularAirportInfo();

        mos.close();
    }
}
