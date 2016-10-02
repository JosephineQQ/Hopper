package cn.smq.mr;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.smq.utils.Metadata;

public class HttpMapper extends Mapper<Object, Text, Text, HttpRecord>{
	
	//define separator ","
	private static Pattern pattern = Pattern.compile("\\,");
	public static Text keytext = new Text();
	public static Text valuetext = new Text();
	
	HttpRecord hr = new HttpRecord();
	
	//**********data set init all 0*********
	static int INTERFACE = 0;
	static int IMSI = 0;
	static int IMEI = 0;
	static int MSISDN = 0;
	static int USER_IP = 0;
	static int FLOW = 0;
	//***************************
	
	@Override
	protected void setup(Mapper<Object, Text, Text, HttpRecord>.Context context) throws IOException, InterruptedException {
		//init Metadata, parameters
		
		Metadata md = new Metadata("/config/MmsDemo.properties");
		//convert String to Integer
		INTERFACE = Integer.parseInt(md.getValue("INTERFACE"));
		IMSI = Integer.parseInt(md.getValue("IMSI"));
		IMEI = Integer.parseInt(md.getValue("IMEI"));
		MSISDN = Integer.parseInt(md.getValue("MSISDN"));
		USER_IP = Integer.parseInt(md.getValue("USER_IP"));
		FLOW = Integer.parseInt(md.getValue("FLOW"));
		
		super.setup(context);		
	}

	
	//map method
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, HttpRecord>.Context context)
			throws IOException, InterruptedException {
		//split value
		String[] values = pattern.split(value.toString(), -1);
		
		if(values.length == 6){//check dataset, filter garbage dataset out
			StringBuffer sb_key = new StringBuffer();
			sb_key.append(values[INTERFACE]).append("|");
			//数据回填 if flow is 0, data fill back
			sb_key.append(StringUtils.isBlank(values[IMSI])?"999999":values[IMSI]).append("|");
			sb_key.append(StringUtils.isBlank(values[IMEI])?"999999":values[IMEI]).append("|");
			sb_key.append(values[MSISDN]).append("|");
			sb_key.append(values[USER_IP]);
			keytext.set(sb_key.toString());
			keytext.set(sb_key.toString());
			
//			valuetext.set(StringUtils.isBlank(values[FLOW])?"0":values[FLOW]);
			hr.flow = StringUtils.isBlank(values[FLOW])?"0":values[FLOW];
			
			context.write(keytext, hr);
		
	/*	
		//check length, filter data
		if (values.length == 6) {
			StringBuffer sb_key = new StringBuffer();
			sb_key.append(values[INTERFACE]).append("|");
			sb_key.append(values[IMSI]).append("|");
			sb_key.append(values[IMEI]).append("|");
			sb_key.append(values[MSISDN]).append("|");
			sb_key.append(values[USER_IP]).append("|");
			keytext.set(sb_key.toString()); 
			
			valuetext.set(StringUtils.isBlank(values[FLOW])?"0":values[FLOW]);
			
			context.write(keytext, valuetext); */
			
		} else {
			// not valid value
			System.out.println("values.length == " + values.length);
		}
		
		super.map(key, value, context);
	}
	
	
	
	@Override
	protected void cleanup(Mapper<Object, Text, Text, HttpRecord>.Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}


	@Override
	public void run(Mapper<Object, Text, Text, HttpRecord>.Context context) throws IOException, InterruptedException {
		super.run(context);
	}
	
}
