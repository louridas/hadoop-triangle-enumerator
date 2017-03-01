import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

public class TriangleSecondMapper
    extends Mapper<LongWritable, Text, Text, Text>{

    private Logger logger = Logger.getLogger(this.getClass());

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = line.split("\\s+");
        Text outputKey = new Text();
        Text outputValue = new Text();
        
        if (data.length > 2) {
            Text edge = new Text(data[0]);
            outputKey.set(edge);
            outputValue.set(edge);
            logger.debug(outputKey + " " + outputValue);
            context.write(outputKey, outputValue);
            String[] nodes = data[0].split(",");
            edge.set(nodes[1] + "," + nodes[0]);
            outputKey.set(edge);
            outputValue.set(edge);
            logger.debug(outputKey + " " + outputValue);
            context.write(outputKey, outputValue);
        } else {
            outputKey.set(data[0]);
            outputValue.set(data[1]);
            logger.debug(outputKey + " " + outputValue);
            context.write(outputKey, outputValue);
        }
    }
}
