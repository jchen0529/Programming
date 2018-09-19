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

public class ex2_3 {

    //mapper function
    public static class Map extends Mapper<Object, Text, Text, Text> {
    		private Text mapkey = new Text("");

    		public void map(Object key, Text value, Context context) 
    				throws IOException, InterruptedException {
		/***
		Read in each record, and emits a key-value pair of <"", [song title + artist's name + duration]>
		***/
    			
	    String input[] = value.toString().split(",");

	    //Grab value if the year conditions are met, years inclusive
	    if ((input[165].matches("^[0-9]{4}$")) && Integer.parseInt(input[165]) >= 2000 && Integer.parseInt(input[165]) <= 2010) {
	    			String val = input[0] + "," + input[2] + "," + input[3];
	    			context.write(mapkey, new Text(val));
	    	    }
 		}
    }

    //run class
    public static void main(String[] args) throws Exception {
    	 	Configuration conf = new Configuration();
    	    Job job = Job.getInstance(conf, "ex3");
    	    job.setJarByClass(ex2_3.class);
    	    job.setMapperClass(Map.class);
    	    
    	    job.setOutputKeyClass(Text.class);
    	    job.setOutputValueClass(Text.class);
    	    
    	    FileInputFormat.addInputPath(job, new Path(args[0]));
    	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  }

