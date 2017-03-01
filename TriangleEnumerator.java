import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TriangleEnumerator {

    private static final String TRIANGLE_FIRST_PASS_OUTPUT_PATH =
        "triangle_first_pass_output";
   
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "triangle first pass");
        job1.setJarByClass(TriangleEnumerator.class);
        job1.setMapperClass(TriangleFirstMapper.class);
        job1.setReducerClass(TriangleFirstReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,
            new Path(TRIANGLE_FIRST_PASS_OUTPUT_PATH));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "triangle second pass");
        job2.setJarByClass(TriangleEnumerator.class);
        job2.setMapperClass(TriangleSecondMapper.class);
        job2.setReducerClass(TriangleSecondReducer.class); 
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileInputFormat.addInputPath(job2,
            new Path(TRIANGLE_FIRST_PASS_OUTPUT_PATH));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
