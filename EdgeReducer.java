import java.io.IOException;

import java.util.List;
import java.util.LinkedList;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

public class EdgeReducer
    extends Reducer<Text, Text, Text, LongWritable> {

    private Logger logger = Logger.getLogger(this.getClass());

    @Override
    public void reduce(Text node,
        Iterable<Text> edges,
        Context context) throws IOException, InterruptedException {
        String nodeValue = node.toString();
        List<Text> edgeCache = new LinkedList<Text>();
            
        long degree = 0;
        for (Text edge : edges) {
            degree++;
            edgeCache.add(new Text(edge));
        }
        for (Text edge : edgeCache) {
            String[] edgeNodes = edge.toString().split(",");
            long outputDegree = edgeNodes[0].equals(nodeValue)
                ? -degree
                : degree;
            logger.debug(edge + " " + outputDegree);                        
            context.write(edge, new LongWritable(outputDegree));
        }
    }
}
