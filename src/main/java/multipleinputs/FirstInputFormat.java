package multipleinputs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author Ha Neul, Kim
 */
public class FirstInputFormat extends FileInputFormat<IntWritable, FirstWritable> {

    @Override
    public RecordReader<IntWritable, FirstWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new FirstRecordReader();
    }
}