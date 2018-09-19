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

public class ex2_1 {	
	
	    //Mapper class for 1 gram file
	    public static class Map1 extends Mapper<Object, Text, Text, Text> {

	    		public void map(Object key, Text value, Context context) 
	    				throws IOException, InterruptedException {
	    			
			/***
			Read in each record from 1gram file, emits a key-value pair of <year + substring, [occurrences, volumes]>
			***/
	    			//read in each line from file
	    		    String input[] = value.toString().split("\\s+");
	    		    		if (input[1].matches("^[0-9]{4}$")) {
	    		    			//check if "nu" is contained
	    		    			if (input[0].toLowerCase().contains("nu")) {
	    		    				String Key = input[1] + "," + "nu";
	    		    				String nuVal = input[2] + "," + input[3];
	    		    				context.write (new Text(Key), new Text(nuVal));
	    		    			}
	    		    			//check if "chi" is contained
	    		    			if (input[0].toLowerCase().contains("chi")) {
	    		    				String Key = input[1] + "," + "chi";
	    		    				String chiVal = input[2] + "," + input[3];
	    		    				context.write (new Text(Key), new Text(chiVal));
	    		    			}
	    		    			//check if "haw" is contained
	    		    			if (input[0].toLowerCase().contains("haw")) {
	    		    				String Key = input[1] + "," + "haw";
	    		    				String hawVal = input[2] + "," + input[3];
	    		    				context.write (new Text(Key), new Text(hawVal));
	    		    			}
	    		    		}
	    			}
	    		}
	    
	  //Mapper class for bi-gram file
	    public static class Map2 extends Mapper<Object, Text, Text, Text> {

	    		public void map(Object key, Text value, Context context) 
	    				throws IOException, InterruptedException {
			/***
			Read in each record from 2gram file, emits a key-value pair of <year + substring, [occurrences, volumes]>
			***/
	    			//read in each line from file
	    		    String input[] = value.toString().split("\\s+");
	    		    	String nuVal = "";
	    		    	String chiVal = "";
	    		    	String hawVal = "";
	    		    	
	    		    if (input[2].matches("^[0-9]{4}$")) {
						
	    		    			//check if "nu" is contained
						if (input[0].toLowerCase().contains("nu") || input[1].toLowerCase().contains("nu")) {
	    		    				//if substring is found in any word, set key = year + substring	
							String Key = input[2] + "," + "nu";
	    		    				
							//if both words contain strings, record occurrences and vols for both under same key
	    		    				if (input[0].toLowerCase().contains("nu") && input[1].toLowerCase().contains("nu")) {
	    		    						nuVal = String.valueOf(Double.parseDouble(input[3]) *2) + "," + String.valueOf(Double.parseDouble(input[4]) *2);}
	    		    						else
	    		    							nuVal = input[3] + "," + input[4];					
	    		    							context.write (new Text(Key), new Text(nuVal)); }

		    				//check if "chi" is contained
						if (input[0].toLowerCase().contains("chi") || input[1].toLowerCase().contains("chi")) {
		    					//if substring is found in any word, set key = year + substring	
							String Key = input[2] + "," + "chi";
		    				
							//if both words contain strings, record occurrences and vols for both under same key
		    					if (input[0].toLowerCase().contains("chi") && input[1].toLowerCase().contains("chi")) {
		    						chiVal = String.valueOf(Double.parseDouble(input[3]) *2) + "," + String.valueOf(Double.parseDouble(input[4]) *2);}
		    						else
		    							chiVal = input[3] + "," + input[4];					
		    							context.write (new Text(Key), new Text(chiVal)); }
	    					//check if "haw" is contained
						if (input[0].toLowerCase().contains("haw") || input[1].toLowerCase().contains("haw")) {
	    						//if substring is found in any word, set key = year + substring	
							String Key = input[2] + "," + "haw";
	    				
							//if both words contain strings, record occurrences and vols for both under same key
	    						if (input[0].toLowerCase().contains("haw") && input[1].toLowerCase().contains("haw")) {
	    							hawVal = String.valueOf(Double.parseDouble(input[3]) *2) + "," + String.valueOf(Double.parseDouble(input[4]) *2);}
	    							else
	    								hawVal = input[3] + "," + input[4];					
	    								context.write (new Text(Key), new Text(hawVal)); }
	    		    			}
	    		    		}
	    			}
	  
		public static class Combine extends Reducer<Text, Text, Text, Text>{
			
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
				double tot_occur = 0;
				double tot_vol = 0;
				
				for (Text val : values) {
					String input[] = val.toString().split(",");
					tot_occur += Double.parseDouble(input[0]);
					tot_vol += Double.parseDouble(input[1]);
				}
				context.write(key, new Text(Double.toString(tot_occur) + "," + Double.toString(tot_vol)));
			}
		}
	    
	    //reducer function - find occurrences/volumes
	    public static class Reduce extends Reducer<Text, Text, Text, Text> {
	    		
	    		public void reduce(Text key, Iterable<Text> values, Context context) 
	    				throws IOException, InterruptedException {

	    		double occur = 0;
	    		double vol = 0;
	    		double avg_vol = 0;
	    		
	    		for (Text val: values) {
	    			String input[] = val.toString().split(",");
	        		occur += Double.parseDouble(input[0]);
	        		vol += Double.parseDouble(input[1]);
	    		}
        		avg_vol = occur/vol;
        		
	    		context.write(key, new Text(String.valueOf(avg_vol)));
	    	}
	  }	
	    
	    //run class
	    public static void main(String[] args) throws Exception {
	    	 	// configure job
	    		Configuration conf = new Configuration();
	    	    Job job = Job.getInstance(conf, "ex1");
	    	    job.setJarByClass(ex2_1.class);
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
   
    
