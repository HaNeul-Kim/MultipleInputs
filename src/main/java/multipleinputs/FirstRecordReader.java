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
public class FirstRecordReader extends RecordReader<IntWritable, FirstWritable> {

    private LineRecordReader lineRecordReader = null;
    private IntWritable key = null;
    private FirstWritable valueFirstClass = null;

    @Override
    public void close() throws IOException {
        if (null != lineRecordReader) {
            lineRecordReader.close();
            lineRecordReader = null;
        }
        key = null;
        valueFirstClass = null;
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public FirstWritable getCurrentValue() throws IOException, InterruptedException {
        return valueFirstClass;
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
            valueFirstClass = null;
            return false;
        }

        String[] arr = lineRecordReader.getCurrentValue().toString().split("t", -1);
        key = new IntWritable(Integer.parseInt(arr[0]));
        valueFirstClass = new FirstWritable(arr[1] + " " + arr[2]);

        return true;
    }
}