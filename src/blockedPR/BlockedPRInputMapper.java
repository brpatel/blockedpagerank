package blockedPR;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class BlockedPRInputMapper  extends Mapper<Object, Text, Text, Text>   {
	
	
	@Override
	protected void map(Object key, Text value, Context context) 
			throws IOException ,InterruptedException
	{
		String line = value.toString();
		String[] values = line.split("\\s+");
		Text outputKey = new Text(values[0]);
		Text outputValue = new Text(values[1]);
		context.write(outputKey, outputValue);
		
	}
	
	

}
