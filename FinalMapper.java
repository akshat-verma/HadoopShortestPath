package shortestpath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class FinalMapper extends Mapper<LongWritable,Text,Text,Text>{
	 public void map(LongWritable key, Text value,  Context context ) throws IOException, InterruptedException  {
	      
	      String line = value.toString();
	      Configuration conf = context.getConfiguration();
	      StringBuffer neighbours = new StringBuffer();
	      ArrayList<String> fields = new ArrayList<String>();
	      for(int i=0 ; i < line.split("\t").length ; i++) {
	    	  fields.add((line.split("\t"))[i]);
	      }
	     String name = fields.get(0);
	     if (name.trim().equals(conf.get("Destination").trim()))
	    	 context.write(new Text(name),new Text(": "+fields.get(4)+name+" #Order of Connection "+fields.get(2)+"rd degree"));
	   }
}











