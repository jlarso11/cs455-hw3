package cs455.hadoop.mindelay;

import cs455.hadoop.utils.TypeCheckUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class BestTimeToFlyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.equals(new LongWritable(0))) {
            return;
        }
        String valueConvertedToString = value.toString();

        String[] individualValues = valueConvertedToString.split(",");

        String hourKey = individualValues[4];

        if (hourKey.length() == 4)
        {
            hourKey = hourKey.substring(0,2);
        }
        else
        {
            hourKey = hourKey.substring(0,1);
        }

        int totalDelay = 0;

        if (TypeCheckUtil.isInteger(individualValues[14])) {
            totalDelay += Integer.parseInt(individualValues[14]);
        }
        if (TypeCheckUtil.isInteger(individualValues[15])) {
            totalDelay += Integer.parseInt(individualValues[15]);
        }

        String dayKey = individualValues[3];
        String monthKey = individualValues[1];

        context.write(new Text("Day-" + dayKey), new IntWritable(totalDelay));
        context.write(new Text("Month-"+monthKey), new IntWritable(totalDelay));
        if(TypeCheckUtil.isInteger(hourKey)){
            context.write(new Text("Hour-"+hourKey), new IntWritable(totalDelay));
        }


    }
}
