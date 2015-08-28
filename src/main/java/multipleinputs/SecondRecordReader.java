package multipleinputs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * @author Ha Neul, Kim
 */
public class SecondRecordReader extends RecordReader<IntWritable, SecondWritable> {

    private LineRecordReader lineRecordReader = null;
    private IntWritable key = null;
    private SecondWritable valueSecondClass = null;

    @Override
    public void close() throws IOException {
        if (null != lineRecordReader) {
            lineRecordReader.close();
            lineRecordReader = null;
        }
        key = null;
        valueSecondClass = null;
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public SecondWritable getCurrentValue() throws IOException, InterruptedException {
        return valueSecondClass;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        close();

        lineRecordReader = new LineRecordReader();
        lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!lineRecordReader.nextKeyValue()) {
            key = null;
            valueSecondClass = null;
            return false;
        }

        String[] arr = lineRecordReader.getCurrentValue().toString().split("t", -1);

        key = new IntWritable(Integer.parseInt(arr[2]));
        valueSecondClass = new SecondWritable(arr[0], arr[1]);

        return true;
    }
}