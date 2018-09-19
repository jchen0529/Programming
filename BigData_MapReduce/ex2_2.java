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

public class ex2_2 {	
	
	    //Mapper class for 1 gram file
	    public static class Map1 extends Mapper<Object, Text, Text, Text> {
	    		private Text mapkey = new Text("");
	    	
	    		public void map(Object key, Text value, Context context) 
	    				throws IOException, InterruptedException {
	    			
			/***
			Read in each record from 1gram file, emits a key-value pair of <"" + volumes>
			***/
	    			//read in each line from file
	    		    String input[] = value.toString().split("\\s+");
	    		    	String vol = input[3];
	    		    	String val = vol + ","+ String.valueOf(Math.pow(Double.parseDouble(vol),2));
	    		    	context.write (new Text(mapkey), new Text(val));
	    		    	}
	    		}
	    
	  //Mapper class for bi-gram file
	    public static class Map2 extends Mapper<Object, Text, Text, Text> {
	    		private Text mapkey = new Text("");
	    		
	    		public void map(Object key, Text value, Context context) 
	    				throws IOException, InterruptedException {
			/***
			Read in each record from 1gram file, emits a key-value pair of <"" + volumes>
			***/
	    			//read in each line from file
	    		    String input[] = value.toString().split("\\s+");
	    		    	String vol = input[4];
	    		    	String val = vol + ","+ String.valueOf(Math.pow(Double.parseDouble(vol),2));
	    		    	context.write (new Text(mapkey), new Text(val));
	    		    	}
	    		}
	  
		public static class Combine extends Reducer<Text, Text, Text, Text>{
			
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
				double tot_vol = 0;
				double sum_sqr = 0;
				double count = 0;
				
				for (Text val : values) {
					String input[] = val.toString().split(",");
					tot_vol += Double.parseDouble(input[0]);
					sum_sqr += Double.parseDouble(input[1]);
					count+=1;
				}
				context.write(key, new Text(String.valueOf(tot_vol) + "," + String.valueOf(sum_sqr) + "," + String.valueOf(count)));
			}
		}
	    
	    //reducer function - calculate std
	    public static class Reduce extends Reducer<Text, Text, Text, Text> {
	    		
	    		public void reduce(Text key, Iterable<Text> values, Context context) 
	    				throws IOException, InterruptedException {

	    		double tot_count = 0;
	    		double tot_vol = 0;
	    		double tot_vol_sqr = 0;
	    		
	    		for (Text val: values) {
	    			String input[] = val.toString().split(",");
	        		tot_vol += Double.parseDouble(input[0]);
	        		tot_vol_sqr += Double.parseDouble(input[1]);
	        		tot_count += Double.parseDouble(input[2]);
	    		}
        		double avg_vol = tot_vol / tot_count;
        		double variance = tot_vol_sqr + avg_vol * avg_vol * tot_count - avg_vol *2 * tot_vol;
        		double std = Math.sqrt(variance/(tot_count - 1));
	    		context.write(key, new Text(String.valueOf(std)));
	    	}
	  }	
	    
	    //run class
	    public static void main(String[] args) throws Exception {
	    	 	// configure job
	    		Configuration conf = new Configuration();
	    	    Job job = Job.getInstance(conf, "ex2");
	    	    job.setJarByClass(ex2_2.class);
	    	    job.setMapperClass(Map1.class);
	    	    job.setMapperClass(Map2.class);
	    	    job.setCombinerClass(Combine.class);
	    	    job.setReducerClass(Reduce.class);
	    	    
	    	    //set mapper key and value types
	    	    job.setMapOutputKeyClass(Text.class);
	    	    job.setMapOutputValueClass(Text.class);
	    	    
	    	    // set key and value types
	    	    job.setOutputKeyClass(Text.class);
	    	    job.setOutputValueClass(Text.class);
	    	    
	    	    //configure paths
	    	   MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map1.class);
	    	   MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Map2.class);
	    	   
	    	   FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    	    
	    	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
	  }
   
    
