package com.hadoop.poc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RecordMapper extends Mapper<Object,Text,Text,Text> {
	
	public void map(Object key,Text val,Context context)throws IOException,InterruptedException {
	
		String []str = val.toString().split("\t");
		
		System.out.println("str[0]="+str[0]);
		System.out.println("str[1]="+str[1]);
		System.out.println("str[2]="+str[2]);
		System.out.println("str[3]="+str[3]);
		System.out.println("str[4]="+str[4]);
		System.out.println("str[5]="+str[5]);
		System.out.println("str[6]="+str[6]);
		System.out.println("str[7]="+str[7]);
//		System.out.println("abc6");
		//String str5 = str[1]+","+str[2]+","+str[3];//+","+str[4]+","+str[5]);//+","+str[6]+","+str[7]);
		String retailId = str[0];
		String recordData = str[1]+","+str[2]+","+str[3]+","+str[4]+","+str[5]+","+str[6]+","+str[7]+","+str[8];
	//	System.out.println("abc4");
		//System.out.println(str[1]+","+str[2]+","+str[3]+","+str[4]+","+str[5]+","+str[6]+","+str[7]);
		 context.write(new Text(retailId), new Text(recordData));
		 
		// context.write(new Text(patId), new Text(medicalRecord));
			}

}
 