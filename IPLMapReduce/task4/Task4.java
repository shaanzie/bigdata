import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.reflect.Array;
import java.util.*;
import java.io.DataOutput;
import java.io.DataInput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Task4{
  public static int stringCompare(String str1, String str2)
      {
        //from geeksforgeeks
        int l1 = str1.length();
        int l2 = str2.length();
        int lmin = Math.min(l1, l2);

        for (int i = 0; i < lmin; i++) {
            int str1_ch = (int)str1.charAt(i);
            int str2_ch = (int)str2.charAt(i);

            if (str1_ch != str2_ch) {
                return str1_ch - str2_ch;
            }
        }

        if (l1 != l2) {
            return l1 - l2;
        }

        else {
            return 0;
        }
    }

 public static class MyWritableComparable implements WritableComparable<MyWritableComparable> {

    protected String key1 = new String();
    protected String key2 = new String();

    public String getKey1() {
        return key1;
    }

    public void setKey1(String key1) {
        this.key1 = key1;
    }

    public String getKey2() {
        return key2;
    }

    public void setKey2(String key2) {
        this.key2 = key2;
    }

    MyWritableComparable(Text key1, String key2) {
        this.key1 = key1.toString();
        this.key2 = key2.toString();
    }

    MyWritableComparable() {
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(key1);
        d.writeUTF(key2);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        key1 = di.readUTF();
        key2 = di.readUTF();
    }

    @Override
    public String toString() {
        return key1.concat(key2);
    }

    @Override
    public int compareTo(MyWritableComparable t) {
        String thiskey1 = this.key1;
        String thatkey1 = t.key1;

        String thiskey2 = this.key2;
        String thatkey2 = t.key2;


        // return thiskey2.compareTo(thatkey2) != 0 ? thiskey2.compareTo(thatkey2)
        //         : (thiskey3 < thatkey3 ? -1 : (thiskey3 == thatkey3
        //                         ? (thiskey1 < thatkey1 ? -1 : (thiskey1 == thatkey1
        //                                         ? 0 : 1)) : 1));


         int x = stringCompare(thiskey1, thatkey1);
         int y = stringCompare(thiskey2, thatkey2);

         if(x > 0){
           return 1;
         }
         else{
           if(x == 0){
             if(y > 0) return 1;
             else if(y == 0) return 0;
             return -1;
           }
           return -1;
         }
         }
      	 }
  	

    public static class BMapper
      extends Mapper<Object, Text, MyWritableComparable, Text>{

          private Text word = new Text();
	  private ArrayList<String> val =  new ArrayList<String>();
	
          public void map(Object key, Text Value,
            org.apache.hadoop.mapreduce.Mapper<Object, Text, MyWritableComparable, Text>.Context context)
            throws IOException, InterruptedException{
                String line = Value.toString();
                String[] values = line.split(",");
		if(val.size() == 0){

				val.add("na");
			}
                String venue = "unset";

                if(values[1].equals("venue")){
   
			
				val.set(0, values[2]);
				
		}

                if(values[0].equals("ball")){
                    context.write(new MyWritableComparable( new Text(val.get(0)), values[4]), new Text(values[4] + "," + values[8]));
                }

          }
      }

      public static class BReducer
        extends Reducer<MyWritableComparable, Text, Text, Text>{
          
          public void reduce(MyWritableComparable key, Iterable<Text> values,
          org.apache.hadoop.mapreduce.Reducer<MyWritableComparable, Text, Text, Text>.Context context
          ) throws IOException, InterruptedException {
		
		double max = -1;
		String bat;
            //int count = 0;
            //for (Text val: values){
             // count += 1;
            //}

            //context.write(new Text(key.getKey1()), new Text(Integer.toString(count)));
             ArrayList<String> batsman = new ArrayList<String>();	
		batsman.add("zzzz");
             double current = 0;
             double currentBalls = 0;
		ArrayList<String> lastbat = new ArrayList<String>();
		lastbat.add("zzz");
          
             for(Text val: values){
               	String[] records = val.toString().split(",");
		lastbat.set(0, records[1]);
               if (!batsman.get(0).equals(records[0])){
                double sr;
          
                 if(currentBalls != 0) sr = current/currentBalls;
                 else sr = 0;
          
                 if (sr > max){
                   max = sr;
                   batsman.set(0, records[0]);
                 }
		
		current = 0;
		currentBalls = 0;
          
                 
               }
          
               else{
                 currentBalls += 1;
                 current += Integer.parseInt(records[1]);
               }
          
             }
          //
          //   //last batsman
             double sr;
         
            if(currentBalls != 0) sr = current/currentBalls;
             else sr = 0;
          
           if (sr > max){
               max = sr;
               batsman.set(0, lastbat.get(0));
//who knows what value this will have after the loop dies
            }
                       
             context.write(new Text(key.toString().split(",")[0]), new Text(batsman.get(0)));
           }
        }


        public static class NaturalKeyComp extends WritableComparator{
          protected NaturalKeyComp(){
		     super(MyWritableComparable.class, true);
          }

          @SuppressWarnings("rawtypes")
	         @Override
	          public int compare(WritableComparable w1, WritableComparable w2) {
		            MyWritableComparable k1 = (MyWritableComparable)w1;
		              MyWritableComparable k2 = (MyWritableComparable)w2;

		             return k1.getKey1().compareTo(k2.getKey1());
                 //int x =
	        }
        }

        public class NaturalKeyPart extends Partitioner<MyWritableComparable, Text>{

          @Override
          public int getPartition(MyWritableComparable key, Text val, int numPartitions){
            int hash = key.getKey1().hashCode();
            int partition = hash % numPartitions;
	
            return partition;
          }
        }


      public static void main(String[] args) throws Exception {
          Configuration conf = new Configuration();
          Job job = Job.getInstance(conf, "task 4");
          job.setJarByClass(Task4.class);
          job.setMapperClass(BMapper.class);
          // job.setCombinerClass(IntSumReducer.class);
          job.setPartitionerClass(NaturalKeyPart.class);
          job.setGroupingComparatorClass(NaturalKeyComp.class);
          job.setReducerClass(BReducer.class);
          job.setMapOutputKeyClass(MyWritableComparable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


          FileInputFormat.addInputPath(job, new Path(args[0]));
          FileOutputFormat.setOutputPath(job, new Path(args[1]));
          System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
