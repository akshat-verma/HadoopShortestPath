package shortestpath;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapred.*;




public class Driver {
	public enum Progress {
		Completion
	};
	public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			Logger logger = Logger.getLogger("sds");
			String[] otherArgs = new GenericOptionsParser( conf, args ).getRemainingArgs();
			conf.set("Source", otherArgs[2] +" "+otherArgs[3]);
			String dest = otherArgs[4] + " "+otherArgs[5];
			conf.set("Destination", dest);
			Job job_init = new Job(conf, "Shortest Path");
			job_init.setJarByClass( Driver.class );
			job_init.setMapperClass(InitMapper.class);
			job_init.setNumReduceTasks(0);
			job_init.setMapOutputKeyClass( LongWritable.class );
			job_init.setMapOutputValueClass( Text.class );
			job_init.setOutputKeyClass( LongWritable.class );
			job_init.setOutputValueClass( Text.class );
			FileInputFormat.addInputPath( job_init, new Path( otherArgs[0] ) );
			FileOutputFormat.setOutputPath(job_init, new Path( otherArgs[1]+"_temp") );
			job_init.waitForCompletion(true);
			String inputfile = otherArgs[1]+"_temp"+"/part-m-00000";
			String outputfile = otherArgs[1]+System.currentTimeMillis();
			Progress p = Progress.Completion;
			while(true) {
		        Job job = new Job(conf,"Shortest Path");  
		        job.setJarByClass(Driver.class);
		        job.setMapOutputKeyClass( Text.class );
			    job.setMapOutputValueClass( Text.class );
		        job.setOutputKeyClass(Text.class);  
		        job.setOutputValueClass(Text.class);  
		        job.setMapperClass(BFSMapper.class);  
		        job.setReducerClass(BFSReducer.class);    
		        FileInputFormat.addInputPath(job, new Path(inputfile));  
		        FileOutputFormat.setOutputPath(job, new Path(outputfile));  
		        job.waitForCompletion(true);
		        inputfile = outputfile+"/part-r-00000";
		        outputfile = otherArgs[1]+System.currentTimeMillis();
		        if(job.getCounters().findCounter(Progress.Completion).getValue()==1)
		        	break;
			 	}	
			Job job = new Job(conf,"Shortest Path");  
	        job.setJarByClass(Driver.class);
	        job.setMapOutputKeyClass( Text.class );
		    job.setMapOutputValueClass( Text.class );
	        job.setOutputKeyClass(Text.class);  
	        job.setOutputValueClass(Text.class);
	        job.setMapperClass(FinalMapper.class);
	        job.setNumReduceTasks(0);
	        FileInputFormat.addInputPath(job, new Path(inputfile));  
	        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	        job.waitForCompletion(true);
	        
	}

}
