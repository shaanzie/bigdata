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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class Task31 {
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

  static class MyWritableComparable implements WritableComparable<MyWritableComparable> {

      protected String key1 = new String();
      protected String key4 = new String();
      protected Integer key2;
      protected Integer key3;

      public String getKey1() {
          return key1;
      }

      public void setKey1(String key1) {
          this.key1 = key1;
      }

      public Integer getKey2() {
          return key2;
      }

      public void setKey2(Integer key2) {
          this.key2 = key2;
      }

      public Integer getKey3() {
          return key3;
      }

      public void setKey3(Integer key3) {
          this.key3 = key3;
      }

      public void setKey4(String key4){
        this.key4 = key4;
      }

      public String getKey4(){
        return this.key4;
      }

      MyWritableComparable(Text key1, Integer key2, Integer key3, Text key4) {
          this.key1 = key1.toString();
          this.key2 = key2;
          this.key3 = key3;
          this.key4 = key4.toString();
      }

      MyWritableComparable() {
      }

      @Override
      public void write(DataOutput d) throws IOException {
          d.writeUTF(key1);
          d.writeInt(key2);
          d.writeInt(key3);
          d.writeUTF(key4);
      }

      @Override
      public void readFields(DataInput di) throws IOException {
          key1 = di.readUTF();
          key2 = di.readInt();
          key3 = di.readInt();
          key4 = di.readUTF();
      }

      @Override
      public String toString() {
          return key1.concat(key2.toString().concat(key3.toString())).concat(key4);
      }

      @Override
      public int compareTo(MyWritableComparable t) {
          String thiskey1 = this.key1;
          String thatkey1 = t.key1;

          String thiskey4 = this.key4;
          String thatkey4 = t.key4;

          int thiskey2 = this.getKey2();
          int thatkey2 = t.getKey2();
          int thiskey3 = this.getKey3();
          int thatkey3 = t.getKey3();


          // return thiskey2.compareTo(thatkey2) != 0 ? thiskey2.compareTo(thatkey2)
          //         : (thiskey3 < thatkey3 ? -1 : (thiskey3 == thatkey3
          //                         ? (thiskey1 < thatkey1 ? -1 : (thiskey1 == thatkey1
          //                                         ? 0 : 1)) : 1));


	         if(thiskey2 > thatkey2) return -1;
	         else{
		           if(thiskey2 == thatkey2){
			              if(thiskey3 > thatkey3){
				                  return 1;
			              }
			         else if(thiskey3 == thatkey3){
				             int x = stringCompare(thiskey1, thatkey1);

              				if(x < 0){
              					return -1;
              				}
              				else{
					                 if(x == 0) return 0;
					                 else return 1;
				              }
			          }
			          return	-1;
		         }
		         return 1;
	          }
          }
    }


      public static class BMapper extends Mapper<Object, Text, MyWritableComparable, IntWritable>{

        public void map(Object key, Text value,
            org.apache.hadoop.mapreduce.Mapper<Object, Text, MyWritableComparable, IntWritable>.Context context)
            throws IOException, InterruptedException
            {
              String line = value.toString();

              String[] values = line.split(",");
              String bat = values[0];
              String bowl = values[1];
              Integer runs = Integer.parseInt(values[2]);
              Integer balls = Integer.parseInt(values[3]);
    	        context.write(new MyWritableComparable(new Text(bowl), runs, balls, new Text(bat)), new IntWritable(1));
        }
      }

        public static class BReducer
            extends Reducer<MyWritableComparable, IntWritable, Text, Text>{

              public void reduce(MyWritableComparable key, Iterable<IntWritable> vals,
                  org.apache.hadoop.mapreduce.Reducer<MyWritableComparable, IntWritable, Text, Text>.Context context)
                  throws IOException, InterruptedException{
  		                context.write(new Text(key.getKey1() + ", " + key.getKey4()), new Text("balls: " + key.getKey3().toString() + " ," +  "runs: " + key.getKey2().toString()));
              }
            }

      public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Task 3");
        job.setJarByClass(Task31.class);
        job.setMapperClass(BMapper.class);
        // job.setCombinerClass(BReducer.class);
        job.setReducerClass(BReducer.class);

        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(MyWritableComparable.class);
      	job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}
