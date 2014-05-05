package blockedPR;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class BlockedPROutputMapper  extends Mapper<Object, Text, Text, Text>   {
	
	
	@Override
	protected void map(Object key, Text value, Context context) 
			throws IOException ,InterruptedException
	{
		String[] line = value.toString().split(BlockedPRConstants.STEP1_ID);
		String blockNoKey = line[0].trim();
		String[] nodePR = line[1].split(BlockedPRConstants.VALUE_SEPERATOR);
		
		context.write(new Text(blockNoKey), new Text(nodePR[0] + BlockedPRConstants.LIST_SEPERATOR + nodePR[1]));
	}
	
	

}
