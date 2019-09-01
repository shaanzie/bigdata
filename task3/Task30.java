import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task30{

  public static class BMapper
       extends Mapper<Object, Text, Text, IntArrayWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntArrayWritable>.Context context) throws IOException, InterruptedException{
      String line = value.toString();
      String[] values = line.split(",");

      if(values[0].equals("ball")){
		    int[] balls = new int[2];
        balls[0] = Integer.parseInt(values[7]);
        balls[1] = Integer.parseInt(values[8]);
        IntArrayWritable ball_data = new IntArrayWritable(balls);
        context.write(new Text(values[4] + "," + values[6]), ball_data);
	}
    }
  }

  public static class BReducer
       extends Reducer<Text, IntArrayWritable, Text, Text> {
   
	private Text result = new Text();
private int totalBalls;
    public void reduce(Text key, Iterable<IntArrayWritable> values,
                        org.apache.hadoop.mapreduce.Reducer<Text, IntArrayWritable, Text, Text>.Context context
                       ) throws IOException, InterruptedException {
      int ballsnum = 0;
      int runs = 0;
      totalBalls = 0;

      for (IntArrayWritable val : values) {
        Writable[] vals = val.get();
        ballsnum += Integer.valueOf(vals[0].toString());
        runs += Integer.valueOf(vals[1].toString());
	totalBalls += 1;
      }

      result.set(new Text(Integer.toString(ballsnum) + "," + Integer.toString(runs) + "," + Integer.toString(totalBalls)));
      context.write(key, result);
    }
  }
  static class IntArrayWritable extends ArrayWritable {

        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(int[] integers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[integers.length];
            for (int i = 0; i < ints.length; i++) {
                ints[i] = new IntWritable(integers[i]);
            }
            set(ints);
        }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "bd task3");
    job.setJarByClass(Task30.class);
    job.setMapperClass(BMapper.class);
//    job.setCombinerClass(BReducer.class);
    job.setReducerClass(BReducer.class);
    job.setOutputKeyClass(Text.class);
 
    job.setMapOutputValueClass(IntArrayWritable.class);
 	 
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

//I think it's type, matchno, over, team batting, on strike batsman, non strike batsman, bowler, //runs, extras, how the batsman got out, who got out
