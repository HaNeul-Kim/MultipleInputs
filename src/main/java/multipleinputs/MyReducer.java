package multipleinputs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Ha Neul, Kim
 */
public class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        Text writeValue = new Text();
        while (iterator.hasNext()) {
            Text value = iterator.next();
            writeValue.append(value.getBytes(), 0, value.getLength());
        }
        context.write(key, writeValue);
    }
}