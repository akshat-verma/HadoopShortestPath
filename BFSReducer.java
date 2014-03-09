package shortestpath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;




public class BFSReducer extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key, Iterable<Text> values,Context context)throws IOException,InterruptedException{
			ArrayList<String> adj = new ArrayList<String>();
			int distance = Integer.MAX_VALUE;
			Node.State status = Node.State.unvisited;
			ArrayList<String> Shortest_path = new ArrayList<String>();
			for(Text val: values)
			{
			Node n = new Node(key.toString()+"\t"+val.toString());
			adj = n.getAdj();
			if (n.getAdj().size() > 0) {
		          adj = n.getAdj();
		    }
			if (n.getState().ordinal() > status.ordinal()) {
		          status = n.getState();
		    }
			if (n.getShortest_path().size() > 0) {
		          Shortest_path = n.getShortest_path();
		    }
			
			if (n.getDistance() < distance) {
		    	  distance = n.getDistance();
			}
			}
			Node newnode = new Node(key);
			newnode.setAdj(adj);
			newnode.setState(status);
			newnode.setDistance(distance);
			newnode.setShortest_Path(Shortest_path);
			
			if(newnode.getName().trim().equals(context.getConfiguration().get("Destination").trim()) && newnode.getState().equals(Node.State.visited)){
				context.getCounter(Driver.Progress.Completion).increment(1);

			}
				
			context.write(new Text(newnode.getName()),newnode.getLine());
	}
}