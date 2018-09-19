import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class ex1_2 extends Configured implements Tool {

    //mapper function
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> 
    {
    	//declare floatwritable since we know it's col4
    	private final static Text key_combo = new Text();
    	
    	public void configure(JobConf job) {
		}

	protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws 
	IOException {
		
		/***
		Read in each record, and emits a key-value pair of <(col30,31,32,33), col(4)>
		***/

		//read in each line from file
	    String line = value.toString();
	    String[] split_str = line.split(",");

	    //Grab keys from each record 
	    String mapkey = split_str[29] + "," + split_str[30] + "," + split_str[31] + "," + split_str[32]; 
	    key_combo.set(mapkey);
	    
	    //grab value (col4)
	    float col_4;
	    col_4 = Float.parseFloat(split_str[3]);

	    //Only collect if the last column is false
	    if (split_str[split_str.length -1].equals("false"))
	    	output.collect(key_combo, new FloatWritable(col_4));
	    }

	protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
		}
 	}

    //reducer function
    public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}

	public void reduce(Text key, Iterator<FloatWritable> values, 
		OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException 
	{
	   
	    float sum = 0;
	    float count = 0;
	    float avg_col4 = 0;

	    while (values.hasNext()) 
	    {
	    		float value = values.next().get();
	    		sum += value;
	    		count += 1;
	    }
	    avg_col4 = sum/count;
	    output.collect(key, new FloatWritable(avg_col4));
	}

	protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}
    }

    //run class
    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), ex1_2.class);
	conf.setJobName("exercise2");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(FloatWritable.class);

	conf.setMapperClass(Map.class);
	//conf.setCombinerClass(Reduce.class); delete Combiner because average calculation is not distributive
	conf.setReducerClass(Reduce.class);

	//file readin method
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception 
    {
    		int res = ToolRunner.run(new Configuration(), new exercise2(), args);
    		System.exit(res);
    }
}
