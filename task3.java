import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task3{

  public static class BMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
      String line = value.toString();
      String[] values = line.split(",");
      if(values[0] == "ball"){
        context.write(new Text(values[4] + "," + values[6] + "," + values[7] + "," + values[8]), one);
      }
    }
}

    public static class BReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
      private IntWritable result = new IntWritable();

      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int sum = 0;

        for(IntWritable val: values){
          sum += val.get();
        }

        result.set(sum);
        context.write(key, result);
      }

    }
  


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "task 3");
    job.setJarByClass(Task3.class);
    job.setMapperClass(BMapper.class);
    job.setCombinerClass(BReducer.class);
    job.setReducerClass(BReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

