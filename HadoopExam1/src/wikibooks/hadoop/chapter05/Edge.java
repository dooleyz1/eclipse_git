package wikibooks.hadoop.chapter05;

import java.io.*;

import org.apache.hadoop.io.WritableComparable;

public class Edge implements WritableComparable<Edge> {
	
	private String departureNode;
	private String arrivalNode;
	
	public void readFields(DataInput in) throws IOException
	{
		departureNode = in.readUTF();
		arrivalNode = in.readUTF();
	}
	
	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(departureNode);
		out.writeUTF(arrivalNode);
	}
	
	public int compareTo(Edge o)
	{
		return ( departureNode.compareTo(o.departureNode) != 0 ) ? 
				departureNode.compareTo(o.departureNode) : arrivalNode.compareTo(o.arrivalNode) ; 
	}

}
