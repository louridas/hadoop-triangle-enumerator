import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EdgeAugmenter {

    private static final String STEP1_OUTPUT_PATH = "step1_augmenter_output";
   
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "edge augmenter creator step 1");
        job1.setJarByClass(EdgeAugmenter.class);
        job1.setMapperClass(EdgeMapper.class);
        job1.setReducerClass(EdgeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(STEP1_OUTPUT_PATH));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "edge augmenter creator step 2");
        job2.setJarByClass(EdgeAugmenter.class);
        job2.setMapperClass(IdentityEdgeMapper.class);
        job2.setReducerClass(PartialDegreeReducer.class); 
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);       
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(STEP1_OUTPUT_PATH));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
