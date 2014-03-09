package shortestpath;

import java.util.ArrayList;
import org.apache.hadoop.io.Text;

public class Node {
public static enum State{
		unvisited,visited,processed
};

private String name;
private int distance;
private State status = State.unvisited;
private ArrayList<String> adj = new ArrayList<String>();
private ArrayList<String> shortest_path = new ArrayList<String>();

public Node(String value)
{
String[] map = value.split("\t");
name = (map[0]);
String[] neighbours = map[1].split(",");
for(String s: neighbours) {
	if(!s.isEmpty()) adj.add(s);
}
if(map[2].equals("Integer.MAX_VALUE"))
this.distance = Integer.MAX_VALUE;
else
this.distance = Integer.parseInt(map[2]);
status = State.valueOf(map[3]);
if (map.length >= 5){
	for (String s : map[4].split(",")) {
	if (s.length() > 0) {
	shortest_path.add(s);
	}
}
}
}

public Node(Text id)
{
name = id.toString();
}
public String getName() {
return name;
}

public int getDistance() {
return distance;
}

public void setDistance(int distance) {
this.distance = distance;
}

public State getState() {
return this.status;
}

public void setState(State status) {
this.status = status;
}

public ArrayList<String> getAdj() {
return this.adj;
}

public void setAdj(ArrayList<String>adj) {
this.adj = adj;
}
public ArrayList<String> getShortest_path() {
	return this.shortest_path;
}

public void setShortest_Path(ArrayList<String> shortest_path) {
	this.shortest_path = shortest_path;
}

public void addShortest_path(String path_taken) {
	this.shortest_path.add(path_taken);
}
public Text getLine() {
StringBuffer line = new StringBuffer();
for (String neighbour : adj) {
line.append(neighbour).append(",");
}
line.append("\t");
if (this.distance < Integer.MAX_VALUE) {
line.append(this.distance).append("\t");
} else {
line.append("Integer.MAX_VALUE").append("\t");
}
line.append(status.toString());
line.append("\t");
for (String n : shortest_path) {
	{   if(!n.isEmpty())
			line.append(n).append("->");
	}
}
return new Text(line.toString());
}

}


