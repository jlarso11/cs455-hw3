package cs455.hadoop.mainjob;

import cs455.hadoop.utils.TypeCheckUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static cs455.hadoop.utils.TypeCheckUtil.isInteger;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class FlightMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.equals(new LongWritable(0))) {
            return;
        }
        String valueConvertedToString = value.toString();

        String[] individualValues = valueConvertedToString.split(",");

        if(individualValues.length > 26) {
            Integer totalDelay = 0;
            Integer carrierDelay = 0;
            Integer weatherDelay = 0;


            if (isInteger(individualValues[25])) {
                weatherDelay += Integer.parseInt(individualValues[25]);
            }

            if (isInteger(individualValues[24])) {
                carrierDelay += Integer.parseInt(individualValues[24]);
            }

            if (TypeCheckUtil.isInteger(individualValues[14])) {
                totalDelay += Integer.parseInt(individualValues[14]);
            }
            if (TypeCheckUtil.isInteger(individualValues[15])) {
                totalDelay += Integer.parseInt(individualValues[15]);
            }

            String hourKey = individualValues[4];

            if (hourKey.length() == 4)
            {
                hourKey = hourKey.substring(0,2);
            }
            else
            {
                hourKey = hourKey.substring(0,1);
            }

            String origin = individualValues[16];
            String destination = individualValues[17];
            String carrier = individualValues[8];
            String tailNum = individualValues[10];
            String timeOfFlight = individualValues[0] + "," + individualValues[1] + "," + individualValues[3] + "," + hourKey;  // year + month + dayOfWeek + hour

            Text mapperKey = new Text(timeOfFlight);
            Text valueToPassAlong = new Text("main-" + origin + "," + destination + "," + carrier + "," + tailNum + ","
                    + String.valueOf(totalDelay) + "," + String.valueOf(carrierDelay) + "," + String.valueOf(weatherDelay));

            if(totalDelay > 0) {
                context.write(mapperKey, new Text(valueToPassAlong));
            }
        }
    }
}
