import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

public class IdentityEdgeMapper
    extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Logger logger = Logger.getLogger(this.getClass());

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString();
        String[] contents = line.split("\\s+");
        Text edge = new Text(contents[0]);
        LongWritable degree = new LongWritable(Long.parseLong(contents[1]));
        logger.debug(edge + " " + degree);            
        context.write(edge, degree);
    }
}
