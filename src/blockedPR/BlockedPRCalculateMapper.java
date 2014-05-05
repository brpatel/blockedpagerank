package blockedPR;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class BlockedPRCalculateMapper  extends Mapper<Object, Text, Text, Text>   {

	/***
	 * 
	 * Input : <b(u);u,PR(u),{v|u->v}>  : <b(u);1#u_PR(u)_v1,v2,v3>
	 * Output: <b(u);u,PR(u),{v|u->v},{<v,PR(u)/deg(u)>|v|u->v}> : <b(u);u_PR(u)_v1,v2,v3_v1,PR(u)/d(u)_v2,PR(u)/d(u)_..>
	 */
	@Override
	protected void map(Object key, Text value, Context context) 
			throws IOException ,InterruptedException
			{
		String strippedValue = value.toString().split(BlockedPRConstants.STEP1_ID)[1];
		String blockIDKey =  value.toString().split(BlockedPRConstants.STEP1_ID)[0].trim();
		String[] values = strippedValue.toString().split(BlockedPRConstants.VALUE_SEPERATOR);
		Integer sourceNode = Integer.parseInt(values[0]);
		Double nodePageRank = Double.parseDouble(values[1]);

		String[] outgoingNodes = values[2].split(BlockedPRConstants.LIST_SEPERATOR);
		int	nodeDegree = outgoingNodes.length;
		Double prByDegree = nodePageRank/nodeDegree;

		for (String outgoingNode : outgoingNodes)
		{	

			int outGoingNodeBlockId = BlockedPRConstants.getBlockIdFromNode(Integer.parseInt(outgoingNode));
			if(outGoingNodeBlockId != Integer.parseInt(blockIDKey))	
			{
				String blockingConstants = BlockedPRConstants.STEP2_ID + outgoingNode 
						+BlockedPRConstants.VALUE_SEPERATOR + String.valueOf(prByDegree);

				context.write(new Text(String.valueOf(outGoingNodeBlockId)), new Text(blockingConstants));
			}else{

				strippedValue =  strippedValue.concat(BlockedPRConstants.VALUE_SEPERATOR + outgoingNode +
						BlockedPRConstants.LIST_SEPERATOR + prByDegree);
			}


		}
		strippedValue = BlockedPRConstants.STEP1_ID + strippedValue;
		context.write(new Text(blockIDKey), new Text(strippedValue));

			}

}
