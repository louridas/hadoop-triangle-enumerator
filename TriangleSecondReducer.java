import java.io.IOException;

import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

public class TriangleSecondReducer
    extends Reducer<Text, Text, Text, Text> {

    private Logger logger = Logger.getLogger(this.getClass());

    @Override
    public void reduce(Text key,
        Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
        List<String> triadCache = new LinkedList<String>();
        String edgeRecord = null;
        Text outputKey = new Text();
        Text outputValue = new Text();
        
        for (Text value : values) {
            String stringValue = value.toString();
            if (stringValue.contains(";")) {
                triadCache.add(stringValue);                
            } else {                
                edgeRecord = stringValue;                
            }
        }
        
        if (triadCache.size() < 1 || edgeRecord == null) {
            return;
        }

        for (String triadRecord : triadCache) {
            Set<String> triangleNodes = new HashSet<String>();            
            for (String edge : triadRecord.split(";")) {
                for (String node : edge.split(",")) {
                    triangleNodes.add(node);
                }
            }
            outputKey.set(new Text(triangleNodes.toString()));
            outputValue.set(new Text(edgeRecord + ";" + triadRecord));
            logger.debug(outputKey + " " + outputValue);
            context.write(outputKey, outputValue);
        }
    }
}
