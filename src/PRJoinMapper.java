import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PRJoinMapper  extends Mapper<Object, Text, Text, Text>   {
	
	
	@Override
	protected void map(Object key, Text value, Context context) 
			throws IOException ,InterruptedException
	{
		
		String line = value.toString();
		String[] values = line.split("\\s+");
		String keyText = values[0];
		String mapperValue = values[1];//.substring(2, values[1].length());
		System.out.println("PRJoin - Map - Key: "+ keyText + " Value: " + mapperValue);
		
		context.write(new Text(keyText), new Text(mapperValue));
		
	}

}
