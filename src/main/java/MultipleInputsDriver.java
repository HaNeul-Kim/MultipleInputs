import multipleinputs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Ha Neul, Kim
 */
public class MultipleInputsDriver implements Tool {

    private Configuration conf;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new MultipleInputsDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path firstPath = new Path(args[0]);
        Path secondPath = new Path(args[1]);
        Path thirdPath = new Path(args[2]);
        Path outputPath = new Path(args[3]);

        //Job job = new Job(conf);
        Job job = Job.getInstance();
        job.setJarByClass(MultipleInputsDriver.class);
        job.setJobName("MultipleInputs Test");

        //output format for mapper
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        //output format for reducer
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(MyReducer.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        //use MultipleOutputs and specify different Record class and Input formats
        MultipleInputs.addInputPath(job, firstPath, FirstInputFormat.class, FirstMapper.class);
        MultipleInputs.addInputPath(job, secondPath, SecondInputFormat.class, SecondMapper.class);
        MultipleInputs.addInputPath(job, thirdPath, ThirdInputFormat.class, ThirdMapper.class);

        return job.waitForCompletion(true) ? Constants.JOB_SUCCESS : Constants.JOB_FAIL;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return this.conf;
    }
}