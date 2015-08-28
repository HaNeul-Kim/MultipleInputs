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
public class ThirdRecordReader extends RecordReader<IntWritable, ThirdWritable> {

    private LineRecordReader lineRecordReader = null;
    private IntWritable key = null;
    private ThirdWritable valueThirdClass = null;

    @Override
    public void close() throws IOException {
        if (null != lineRecordReader) {
            lineRecordReader.close();
            lineRecordReader = null;
        }
        key = null;
        valueThirdClass = null;
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public ThirdWritable getCurrentValue() throws IOException, InterruptedException {
        return valueThirdClass;
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
            valueThirdClass = null;
            return false;
        }

        String[] arr = lineRecordReader.getCurrentValue().toString().split("t", -1);
        key = new IntWritable(Integer.parseInt(arr[0]));
        valueThirdClass = new ThirdWritable(arr[1] + " " + arr[2]);

        return true;
    }
}