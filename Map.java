package mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;

import javax.naming.Context;
import javax.xml.soap.Text;

public class Map extends Mapper<Object, Text, IntWritable, Text> {
		private Text word= new Text();
	 public void map(Object key, Text value,
	   Context context) throws IOException, InterruptedException {
		 int a = 0;
	    StringTokenizer itr = new StringTokenizer(value.toString(),","); 
	    
	    while (itr.hasMoreTokens()){
	    	
	    	word.setData(key.toString() +"," +  itr.nextToken());
	    	context.write(new IntWritable(a), word;
	    	a++;
	    	
	    }
	   
	    }
	 }



public static class PivotReduce extends Reducer<Object, Text, IntWritable,Text>

{
	private Text Result = new Text();
	public void reduce(IntWritable key, Iterable <Text>, Context context) throws IOException, InterruptedException
		{
		TreeMap<Integer, String> keyValues = new TreeMap<Integer, String>();
		String temp = ""; 
		for(Text value : values){
			int tempInt = Integer.parseInt(value.toString().split(",")[0]);
			String tempString = value.toString().split(",")[1];
			keyValues.put(tempInt, tempString);
			
		}
		for(Map.Entry<Integer, String> entry : keyValues.entrySet()){
			temp += entry.getValue() +";"; 
			
		}
		
		result.set(temp);
		context.write(key, result);
}





