import java.io.IOException;

import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

public class TriangleFirstReducer
    extends Reducer<Text, Text, Text, Text> {

    private Logger logger = Logger.getLogger(this.getClass());

    @Override
    public void reduce(Text node,
        Iterable<Text> edges,
        Context context) throws IOException, InterruptedException {
        String nodeValue = node.toString();
        List<String> edgeCache = new ArrayList<String>();
        Text outputKey = new Text();
        Text outputValue = new Text();
        
        for (Text edge : edges) {
            edgeCache.add(edge.toString());
        }
        
        if (edgeCache.size() <= 1) {
            return;
        }
        for (int i = 0; i < edgeCache.size(); i++) {
            for (int j = i + 1; j < edgeCache.size(); j++) {
                String edge1 = edgeCache.get(i);
                String edge2 = edgeCache.get(j);
                String[] nodesEdge1 = edge1.split(",");
                String[] nodesEdge2 = edge2.split(",");
                String node1 = nodesEdge1[0].equals(nodeValue)
                    ? nodesEdge1[1]
                    : nodesEdge1[0];
                String node2 = nodesEdge2[0].equals(nodeValue)
                    ? nodesEdge2[1]
                    : nodesEdge2[0];
                outputKey.set(node1 + "," + node2);
                outputValue.set(edge1 + ";" + edge2);
                logger.debug(outputKey + " " + outputValue);
                context.write(outputKey, outputValue);
            }
        }
    }
}
