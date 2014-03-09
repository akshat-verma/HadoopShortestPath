package shortestpath;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class BFSMapper extends Mapper <LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
	Node node = new Node(value.toString());
	if (node.getState() == Node.State.visited) {
	  for (String s : node.getAdj()) {
	    Node neighbour = new Node(new Text(s));
	    neighbour.setDistance(node.getDistance() + 1);
	    neighbour.setState(Node.State.visited);
	    neighbour.setShortest_Path(node.getShortest_path());
	    if(!neighbour.getShortest_path().contains(node.getName()) && !neighbour.getShortest_path().contains(neighbour.getName()))
	    	neighbour.addShortest_path(node.getName());
	    context.write(new Text(neighbour.getName()), neighbour.getLine());
	  }
	  node.setState(Node.State.processed);
	}

	context.write(new Text(node.getName()), node.getLine());
	}
}
