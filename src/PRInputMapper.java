import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PRInputMapper  extends Mapper<Object, Text, Text, Text>   {
	
	
	@Override
	protected void map(Object key, Text value, Context context) 
			throws IOException ,InterruptedException
	{
		String line = value.toString();
		String[] values = line.split("\\s+");
		Text outputKey = new Text(values[0]);
		Text outputValue = new Text(values[1]);
		//System.out.println("PRInput - Map - Key: "+outputKey.toString() + " Value: " + outputValue.toString());
		context.write(outputKey, outputValue);
		
	}

}
