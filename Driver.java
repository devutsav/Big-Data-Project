import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Driver 
{
    public static void main(String[] args) throws IOException, ClassNotFoundException,InterruptedException
    {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf, "job");
        job.setJarByClass(Driver.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job,new Path("data.csv"));
        FileOutputFormat.setOutputPath(job, new Path("output_age0"));
  }
  job.waitForCompletion(true);
}
