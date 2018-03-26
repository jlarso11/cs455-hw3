package cs455.hadoop.olderplanes;

import cs455.hadoop.utils.MapSorts;
import cs455.hadoop.utils.TypeCheckUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class OldPlaneDelaysReducer extends Reducer<Text, Text, Text, IntWritable> {

    private class flightDetails {
        int year;
        boolean hadDelay;


        public flightDetails(int year, boolean hadDelay) {
            this.year = year;
            this.hadDelay = hadDelay;
        }

        public int getYear() {
            return year;
        }

        public boolean isHadDelay() {
            return hadDelay;
        }
    }

    private int totalFlightsOver20years = 0;
    private int delayedFlightsOver20years=0;
    private int totalFlightsUnder20years = 0;
    private int delayedFlightsUnder20years = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int year =0;
        List<flightDetails> flightCounts = new ArrayList<>();
        for(Text value : values) {
            String[] fileSplit = value.toString().split("-");
            if("f2".equalsIgnoreCase(fileSplit[0])) {
                String[] valueSplit = fileSplit[1].split(",");
                if(valueSplit.length >= 9 && TypeCheckUtil.isInteger(valueSplit[8])) {
                    year = Integer.parseInt(valueSplit[8]);
                }
                else {
                    return;
                }
            } else {
                String[] valueSplit = fileSplit[1].split(",");
                if(valueSplit.length >= 16 && TypeCheckUtil.isInteger(valueSplit[0]) && TypeCheckUtil.isInteger(valueSplit[14]) && TypeCheckUtil.isInteger(valueSplit[15])) {
                    int yearOfFlight = Integer.parseInt(valueSplit[0]);
                    boolean hadDelay = Integer.parseInt(valueSplit[14]) > 0 || Integer.parseInt(valueSplit[15]) > 0;
                    flightCounts.add(new flightDetails(yearOfFlight, hadDelay));
                }
            }
        }

        for(flightDetails flight : flightCounts) {
            if(flight.getYear() - year > 20) {
                totalFlightsOver20years++;
                if(flight.hadDelay) {
                    delayedFlightsOver20years++;
                }
            } else {
                totalFlightsUnder20years++;
                if(flight.hadDelay) {
                    delayedFlightsUnder20years++;
                }
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Total Delayed flights on old planes"), new IntWritable(delayedFlightsOver20years));
        context.write(new Text("Total flights on old planes"), new IntWritable(totalFlightsOver20years));
//        context.write(new Text("Delayed Percentage on old planes"), new IntWritable(delayedFlightsOver20years/totalFlightsOver20years));
        context.write(new Text("Total Delayed flights on newer planes"), new IntWritable(delayedFlightsUnder20years));
        context.write(new Text("Total flights on newer planes"), new IntWritable(totalFlightsUnder20years));
//        context.write(new Text("Delayed Percentage on newer planes"), new IntWritable(delayedFlightsUnder20years/totalFlightsUnder20years));
    }
}
