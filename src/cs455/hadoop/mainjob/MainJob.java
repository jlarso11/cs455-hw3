package cs455.hadoop.mainjob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MainJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf, "Flight data research");

            // Current class.
            job.setJarByClass(MainJob.class);

            MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, FlightMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AirportsMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, CarrierMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, PlaneDataMapper.class);

            // Combiner. We use the reducer as the combiner in this case.
            job.setCombinerClass(MainCombiner.class);

            // Reducer
            job.setReducerClass(MainReducer.class);

            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            MultipleOutputs.addNamedOutput(job, "bestTimeToFly", TextOutputFormat.class, Text.class, IntWritable.class);
            MultipleOutputs.addNamedOutput(job, "worstTimeToFly", TextOutputFormat.class, Text.class, IntWritable.class);
            MultipleOutputs.addNamedOutput(job, "majorHubs", TextOutputFormat.class, Text.class, IntWritable.class);

            MultipleOutputs.addNamedOutput(job, "carrierDelays", TextOutputFormat.class, Text.class, IntWritable.class);
            MultipleOutputs.addNamedOutput(job, "olderPlanes", TextOutputFormat.class, Text.class, LongWritable.class);
            MultipleOutputs.addNamedOutput(job, "weatherDelayCities", TextOutputFormat.class, Text.class, IntWritable.class);

            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[4]));

            // Block until the job is completed.
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }
}
