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
public class InitMapper extends Mapper<LongWritable,Text,Text,Text>{
	 public void map(LongWritable key, Text value,  Context context ) throws IOException, InterruptedException  {
	      
	      String line = value.toString();
	      Configuration conf = context.getConfiguration();
	      StringBuffer neighbours = new StringBuffer();
	      ArrayList<String> fields = new ArrayList<String>();
	      for(int i=0 ; i < line.split(",").length ; i++) {
	    	  fields.add((line.split(","))[i]);
	      }
	      String node = fields.get(0);
	      int distance;
	      String status;
	      fields.remove(0);
	      for(int i=0;i<fields.size();i++){
	    	  neighbours.append(fields.get(i)).append(",");
	      }
	     if(conf.get("Source").equals(node)) {
	    	 distance = 0;
	    	 status = "visited";
	     }
	     else{
	    	 distance = Integer.MAX_VALUE;
	    	 status = "unvisited";
	     }
	     context.write(new Text(node),new Text(neighbours.toString()+"\t"+Integer.toString(distance)+"\t"+status));
	   }

}
