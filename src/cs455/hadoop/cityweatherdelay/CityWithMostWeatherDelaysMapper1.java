package cs455.hadoop.cityweatherdelay;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static cs455.hadoop.utils.TypeCheckUtil.isInteger;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class CityWithMostWeatherDelaysMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.equals(new LongWritable(0))) {
            return;
        }
        String valueConvertedToString = value.toString();

        String[] individualValues = valueConvertedToString.split(",");

        if(individualValues.length > 25 && isInteger(individualValues[25]) && Integer.parseInt(individualValues[25]) > 0) {
            String originKey = individualValues[16];
            context.write(new Text(originKey), new Text("F1-" + individualValues[25]));

            String destKey = individualValues[17];
            context.write(new Text(destKey), new Text("F1-" + individualValues[25]));
        }
    }
}
