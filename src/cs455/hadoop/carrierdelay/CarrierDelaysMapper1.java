package cs455.hadoop.carrierdelay;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static cs455.hadoop.utils.TypeCheckUtil.isInteger;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class CarrierDelaysMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.equals(new LongWritable(0))) {
            return;
        }
        String valueConvertedToString = value.toString();

        String[] individualValues = valueConvertedToString.split(",");

        String mapperKey = individualValues[8];

        if(individualValues.length > 25) {
            Integer totalDelay = 0;

            if (isInteger(individualValues[24])) {
                totalDelay += Integer.parseInt(individualValues[24]);
            }

            if(totalDelay > 0) {
                context.write(new Text(mapperKey), new Text("F1-" + totalDelay.toString()));
            }
        }
    }
}
