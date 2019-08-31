package IPL;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class assignMap extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, OutputCollector <Text, IntWritable> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] bowlerBatsmanStrings = valueString.split((","));
        if(!bowlerBatsmanStrings[9].isEmpty())
        {
            output.collect(new Text(bowlerBatsmanStrings[4] + " " + bowlerBatsmanStrings[6]), one);
        }
    }
}