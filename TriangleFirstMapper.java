import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

public class TriangleFirstMapper
    extends Mapper<LongWritable, Text, Text, Text>{

    private Logger logger = Logger.getLogger(this.getClass());
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = line.split("\\s+");
        long firstDegree = Long.parseLong(data[1]);
        long secondDegree = Long.parseLong(data[2]);
        String[] nodes = data[0].split(",");
        Text node = new Text();
        Text edge = new Text(data[0]);
        long minDegree = 0;
        if (firstDegree < secondDegree) {
            minDegree = firstDegree;
            node.set(nodes[0]);
        } else {
            minDegree = secondDegree;
            node.set(nodes[1]);
        }
        if (minDegree > 1) {
            logger.debug(node + " " + edge);
            context.write(node, edge);
        }
    }
}
