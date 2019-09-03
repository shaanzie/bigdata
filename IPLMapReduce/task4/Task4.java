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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Task4{

    public static class BMapper
      extends Mapper<Object, Text, Text, IntWritable>{

          private Text word = new Text();

          public void map(Object key, Text Value, Context content) throws IOException, InterruptedException{
                String line = value.toString();
                String[] values = line.split(",");

                String venue = "unset";

                if(values[2].equals["venue"]){
                  venue = values[2];
                }

                if(values[0].equals("ball")){
                    context.write(new Text(venue + "," values[4] + "," + values[8]), IntWritable(1));
                }

          }
      }

      public static class BReducer
        extends Reducer<Text, IntWritable, Text, Text>{
          private double max = -1;
          private String bat;
          public void reduce(Text key, Iterable<IntArrayWritable> values, Context context
          ) throws IOException, InterruptedException {
            String batsman = "new";
            double current = 0;
            double currentBalls = 0;

            for(IntWritable val: values){
              String[] records = key.toString().split(",");
              if (!batsman.equals(records[1])){
                double sr;

                if(currentBalls != 0) sr = current/currenBalls;
                else sr = 0;

                if (sr > max){
                  max = sr;
                  bat = batsman;
                }

                batsman = records[1];
              }

              else{
                currentBalls += 1;
                current += records[2];
              }

            }

            //last batsman
            double sr;

            if(currentBalls != 0) sr = current/currenBalls;
            else sr = 0;

            if (sr > max){
              max = sr;
              bat = batsman;
            }

            context.write(new Text(key.toString().split(",")[0]), new Text(bat));
          }
        }

      public static GroupComp extends WritableComparator{
        protected GroupComp(){
          super(Text.class, true);
        }

        @Override
        public int compare(Text a, Text b){
          String A = a.toString().split(",")[0];
          String B = b.toString().split(",")[0];

          return A.compareTo(B);
        }
      }


      public static void main(String[] args) throws Exception {
          Configuration conf = new Configuration();
          Job job = Job.getInstance(conf, "task 4");
          job.setJarByClass(Task4.class);
          job.setMapperClass(BMapper.class);
          // job.setCombinerClass(IntSumReducer.class);
          job.setReducerClass(BReducer.class);
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(Text.class);
          job.setGroupingComparatorClass(MyGroupComparator.class);
          FileInputFormat.addInputPath(job, new Path(args[0]));
          FileOutputFormat.setOutputPath(job, new Path(args[1]));
          System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
