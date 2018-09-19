import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class ex2_4 {

    //mapper function
    public static class Map extends Mapper<Object, Text, Text, DoubleWritable> {
    		
    		public void map(Object key, Text value, Context context) 
    				throws IOException, InterruptedException {
		/***
		Read in each record, and emits a key-value pair of <artist, duration>
		***/
    			
	    String input[] = value.toString().split(",");
	    String artist = input[2];
	    double dur = Double.parseDouble(input[3]);
	    try {
	    		context.write(new Text(artist), new DoubleWritable(dur));
	    	    }
	    catch (Exception e) {
	    	System.out.println("error");
	    }
 	}
    }
    
    //set partitioner to use 5 reducers
    public static class Partition extends Partitioner<Text, DoubleWritable> {

        public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {
        	
        	 	String artist = key.toString().toLowerCase();
        	 	
             if (artist.substring(0,1).compareTo("f") < 0) {
                  return 0;
              } else if (artist.substring(0,1).compareTo("k") < 0) {
                  return 1;
              } else if (artist.substring(0,1).compareTo("p") < 0) {
                  return 2;
              } else if (artist.substring(0,1).compareTo("u") < 0) {
                  return 3;
              } else {
                  return 4;
              }
        }
    }
    
    //reducer function - find max duration by artist
    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    		
    		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
    				throws IOException, InterruptedException {

    		double max_dur = 0;
    		
    		for (DoubleWritable val: values) {
        		double current = val.get();
    				if (max_dur < current) {
    					max_dur = current;}}
    		try {
    		context.write(key, new DoubleWritable(max_dur));}
    		catch (Exception e) {
    			System.out.println("error");}
    	}
  }
    
    //run class
    public static void main(String[] args) throws Exception {
    	 	// configure job
    		Configuration conf = new Configuration();
    	    Job job = Job.getInstance(conf, "ex4");
    	    job.setJarByClass(ex2_4.class);
    	    job.setMapperClass(Map.class);
    	    job.setPartitionerClass(Partition.class);
    	    job.setReducerClass(Reduce.class);
    	    
    		// set mapper types
    		job.setMapOutputKeyClass(Text.class);
    		job.setMapOutputValueClass(DoubleWritable.class);
    	    
    	    // set key and value types
    	    job.setOutputKeyClass(Text.class);
    	    job.setOutputValueClass(DoubleWritable.class);
    	    
    	    //configure paths
    	    FileInputFormat.addInputPath(job, new Path(args[0]));
    	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	    
    	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  }

