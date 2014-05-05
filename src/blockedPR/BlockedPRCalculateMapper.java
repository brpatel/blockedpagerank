package blockedPR;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class BlockedPRCalculateMapper  extends Mapper<Object, Text, Text, Text>   {
	
	
	@Override
	protected void map(Object key, Text value, Context context) 
			throws IOException ,InterruptedException
	{
		String strippedValue = value.toString().split(BlockedPRConstants.STEP1_ID)[1];
		String blockIDKey =  value.toString().split(BlockedPRConstants.STEP1_ID)[0].trim();
		String[] values = strippedValue.toString().split(BlockedPRConstants.VALUE_SEPERATOR);
		Integer sourceNode = Integer.parseInt(values[0]);
		Double nodePageRank = Double.parseDouble(values[1]);
		System.out.println("inside blockedprcalculatemapper");	
		
		String[] outgoingNodes = values[2].split(BlockedPRConstants.LIST_SEPERATOR);
		int	nodeDegree = outgoingNodes.length;
		Double prByDegree = nodePageRank/nodeDegree;
		
		for (String outgoingNode : outgoingNodes)
		{	
			strippedValue =  strippedValue.concat(BlockedPRConstants.VALUE_SEPERATOR + outgoingNode +
					BlockedPRConstants.LIST_SEPERATOR + prByDegree);
			System.out.println("PRCalc - Map - Key: "+ outgoingNode + " Value: " + prByDegree.toString());
		}
		context.write(new Text(blockIDKey), new Text(strippedValue));
		
	}

}
