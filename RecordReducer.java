package com.hadoop.poc;


import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class RecordReducer extends Reducer<Text,Text,Text,Text> {
	
MultipleOutputs<Text,Text> mos;

public void setup(Context context)
{
	mos = new MultipleOutputs<Text,Text>(context);
}
protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
{
	
	String retailname = "";
	String typeofcrawling = "";
	String product_url = "";
	String title = "";
	float salesprice = 0;
	float reg_price = 0;
	float rebate =0;
	String stock_info ="";
	String retail_id="";
  
	for( Text val : values)
	{
		String [] str2 = val.toString().split(",");
	//	retail_id=str2[0];
		retailname = str2[0];
		typeofcrawling = str2[1];
		product_url = str2[2];
		title = str2[3];
		salesprice = Float.parseFloat(str2[4]);
		reg_price = Float.parseFloat(str2[5]);
		rebate = Float.parseFloat(str2[6]);
		stock_info = str2[7];
		
	}
	
	String result = retailname+"\t"+typeofcrawling+"\t"+product_url+"\t"+title+"\t"+salesprice+"\t"+reg_price+"\t"+rebate+"\t"+stock_info;
	
	

	if (typeofcrawling.equals("BS"))
	{
		/*if( (reg_price>150.00 || rebate<25)  && title.length()<100 )
		{
			return 0;
		}
		
		else*/ if(salesprice<100 && rebate >50)
		{
			mos.write("HighBuzzProducts", new Text(key), new Text(result));
		}
		
		else if(reg_price<150 && (rebate>25 && rebate <50))
		
		{
			mos.write("NormalProducts", new Text(key), new Text(result));
		}
		else if(title.length()>100)
		{
			mos.write("RareProducts", new Text(key), new Text(result));
		}
		else
		{
			mos.write("OtherProducts", new Text(key), new Text(result));
		}
	}
	else if(typeofcrawling.equals("ODC"))
	{
		if(salesprice<150)
			mos.write("OnDemandCrawlProducts", new Text(key), new Text(result));
		else if(stock_info.equals("InStock"))
				mos.write("AvailableProducts", new Text(key), new Text(result));
		else
			mos.write("OtherProducts", new Text(key), new Text(result));
	}
	else
		mos.write("OtherProducts", new Text(key), new Text(result));
	
	
	
	
	
	
			
}
 public void cleanup(Context context) throws IOException,InterruptedException
 {
	 mos.close();
 }
}




