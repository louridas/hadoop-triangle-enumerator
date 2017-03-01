import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

public class PartialDegreeReducer
    extends Reducer<Text, LongWritable, Text, Text> {

    private Logger logger = Logger.getLogger(this.getClass());

    @Override
    public void reduce(Text edge,
        Iterable<LongWritable> degrees,
        Context context) throws IOException, InterruptedException {
        long leftDegree = 0;
        long rightDegree = 0;
        
        for (LongWritable degree : degrees) {
            long value = degree.get();
            if (value < 0) {
                leftDegree = -value;
            } else {
                rightDegree = value;
            }
        }
        String reducerValue = leftDegree + " " + rightDegree;
        logger.debug(reducerValue);
        context.write(edge, new Text(reducerValue));
    }
}
