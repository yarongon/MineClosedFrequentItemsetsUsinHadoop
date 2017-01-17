package minecloseditemsets.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class TwitterInputSplit extends InputSplit implements Writable {
	
	private String query;
	private String startDate; // format is YYYY-MM-DD
	private String endDate;   // format is YYYY-MM-DD
	private int limit = Integer.MAX_VALUE;
	
	public TwitterInputSplit(String query, String startDate, String endDate) {
		super();
		this.query = query;
		this.startDate = startDate;
		this.endDate = endDate;
	}
	
	public TwitterInputSplit(String query, String startDate, String endDate, int limit) {
		this(query,startDate,endDate);
		this.limit = limit;
	}
	
	
	public TwitterInputSplit() {
		
	}

	public String getQuery() {
		return query;
	}

	public String getStartDate() {
		return startDate;
	}
	
	public String getEndDate() {
		return endDate;
	}
	
	public int getLimit() {
		return this.limit;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		// Fixed for a single day.
		return 1;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[] {};
	}

	@Override
	public void write(DataOutput out) throws IOException {
		String lineSeperator = System.getProperty("line.separator"); 
		
		out.writeBytes(this.query);
		out.writeBytes(lineSeperator);
		out.writeBytes(this.startDate);
		out.writeBytes(lineSeperator);
		out.writeBytes(this.endDate);
		out.writeBytes(lineSeperator);
		out.writeInt(this.limit);
		
		/*
		out.writeChars(this.query);
		out.writeChar(10);
		out.writeChars(this.startDate);
		out.writeChar(10);
		out.writeChars(this.endDate);
		out.writeChar(10);
		*/
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.query = in.readLine();
		this.startDate = in.readLine();
		this.endDate = in.readLine();
		this.limit = in.readInt();
	}

}
