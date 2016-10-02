package cn.smq.mr;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class HttpReducer extends Reducer<Text, HttpRecord, NullWritable, Text>{
	
	public static Text valueText = new Text();
	HttpRecord hr = new HttpRecord();
	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
	String timedir;
	//define Mutlti-output
	private MultipleOutputs<NullWritable, Text> mout;
	
	//init
	@Override
	protected void setup(Reducer<Text, HttpRecord, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		//instan
		mout = new MultipleOutputs<NullWritable, Text>(context);
		timedir = sdf.format(new Date());
		
	}

	//reducer method
	@Override
	protected void reduce(Text key, Iterable<HttpRecord> values,
			Reducer<Text, HttpRecord, NullWritable, Text>.Context context) throws IOException, InterruptedException {
		
		Iterator<HttpRecord> it = values.iterator();
		double flow = 0.0;
		while (it.hasNext() == true) {
			hr = it.next();
			flow = flow + Double.parseDouble(hr.flow);
		}
		
		StringBuffer sb_value = new StringBuffer();
		sb_value.append(key).append("|");
		sb_value.append(flow);
		valueText.set(sb_value.toString());
		
		//context.write(NullWritable.get(), valueText);
		
		//data storage real directory, driver class to implement
		String path = "/user/smq/Hopper/" + timedir + "/"; 
		
		mout.write(NullWritable.get(), valueText, path);
	}

	//clean 
	@Override
	protected void cleanup(Reducer<Text, HttpRecord, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		//when invoke, data output export
		mout.close();
	}

	@Override
	public void run(Reducer<Text, HttpRecord, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		super.run(context);
	}

	
}
