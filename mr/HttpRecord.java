package cn.smq.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class HttpRecord implements WritableComparable<Object>{

	public String flow;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		flow = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(flow);
	}

	@Override
	public int compareTo(Object o) {
		
		return 0;
	}

}
