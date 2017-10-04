package mapreduce;

import java.io.IOException;
import java.util.List;

import javax.naming.Context;

public class Map extends Mapper<Object, Text, Text, Text> {

	 @Override
	 public void map(Object key, Text value,
	   Context context) throws IOException, InterruptedException {

	    String[][] Pivot = value.toString().split("\\t", -1); 
	    for (int i = 0; i < Pivot.length; ++i) {
	        if ("".equals(Pivot[i])) Pivot[i] = null;
	    }
	    List<String> Pivot_list = Arrays.asList(Pivot);
	    Text textKey = new Text(Pivot_list.get(0));
	    Text textValue = new Text(Pivot_list.get(1));
	    context.write(textKey,textValue);
	    }
	 }
public static class Reduce extends Reducer<Object, Text, Text,Text>
{
public void reduce(Object key, Text value, Context context) throws IOException, InterruptedException
{
int sum = 0;
   for( textValue : value)
   {
   sum += value.get();
   }
   context.write(key, new textValue(sum));
}
}
}



}

