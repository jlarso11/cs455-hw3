package cs455.hadoop.mindelay;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static cs455.hadoop.utils.TypeCheckUtil.isInteger;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class BestHourToFlyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.equals(new LongWritable(0))) {
            return;
        }
        String valueConvertedToString = value.toString();

        String[] individualValues = valueConvertedToString.split(",");

        String hour = individualValues[4];

        if (hour.length() == 4)
        {
            hour = hour.substring(0,2);
        }
        else
        {
            hour = hour.substring(0,1);
        }

        int totalDelay = 0;

        if (isInteger(individualValues[14])) {
            totalDelay += Integer.parseInt(individualValues[14]);
        }
        if (isInteger(individualValues[15])) {
            totalDelay += Integer.parseInt(individualValues[15]);
        }

        String mapKey = hour;

        context.write(new Text(mapKey), new IntWritable(totalDelay));

    }
}
