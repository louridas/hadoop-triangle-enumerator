import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

public class EdgeMapper
    extends Mapper<LongWritable, Text, Text, Text>{

    private Logger logger = Logger.getLogger(this.getClass());

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("#")) {
            return;
        }
        String[] nodes = line.split("\\s+");
        Text source = new Text(nodes[0]);
        Text edge = new Text(String.join(",", nodes));
        context.write(source, edge);
        logger.debug(source + " " + edge);
        source = new Text(nodes[1]);
        logger.debug(source + " " + edge);            
        context.write(source, edge);
    }
}
