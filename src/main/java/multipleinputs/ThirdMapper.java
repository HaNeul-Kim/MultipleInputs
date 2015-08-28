package multipleinputs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Ha Neul, Kim
 */
public class ThirdMapper extends Mapper<IntWritable, ThirdWritable, IntWritable, Text> {

    @Override
    protected void map(IntWritable key, ThirdWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, new Text(value.toString()));
    }
}