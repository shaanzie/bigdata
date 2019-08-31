package IPL;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class assignDriver {

    public static void main(String[] args)
    {
        JobClient assignClient = new JobClient();

        JobConf assignConf = new JobConf(assignDriver.class);

        assignConf.setJobName("BatToBowlVul");
        
        assignConf.setOutputKeyClass(Text.class);
        assignConf.setOutputValueClass(IntWritable.class);

        assignConf.setMapperClass(IPL.assignMap.class);
        assignConf.setReducerClass(IPL.assignReduce.class);

        assignConf.setInputFormat(TextInputFormat.class);
        assignConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(assignConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(assignConf, new Path(args[1]));

        assignClient.setConf(assignConf);
        try {
            //run the job
            JobClient.runJob(assignConf);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        

    }
}