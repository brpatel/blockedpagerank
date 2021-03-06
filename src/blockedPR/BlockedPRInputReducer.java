package blockedPR;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class BlockedPRInputReducer extends Reducer<Text, Text, Integer, Text> {


	/***
	 * Input: <u;v> : <u;v>
	 * Output: <b(u);u,PR(u),{v|u->V}>: <b(u);1#u_PR(u)_v1,v2,v3>
	 * 
	 */

	@Override
	protected void reduce(Text key, 
			java.lang.Iterable<Text> values, 
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Integer, Text>.Context context) 
					throws IOException ,InterruptedException 
					{
		Double pageRank = (double) 1/BlockedPRConstants.TOTAL_NODES;
		Integer sourceNode = Integer.parseInt(key.toString());
		Integer blockID = BlockedPRConstants.getBlockIdFromNode(sourceNode);

		StringBuilder outLinkNodes = new StringBuilder();
		for (Text value : values) {
			outLinkNodes.append(value.toString());
			outLinkNodes.append(BlockedPRConstants.LIST_SEPERATOR);
		} 
		StringBuilder mapperValue = new StringBuilder(BlockedPRConstants.STEP1_ID+sourceNode+BlockedPRConstants.VALUE_SEPERATOR+pageRank.toString()+
				BlockedPRConstants.VALUE_SEPERATOR +outLinkNodes.substring(0, outLinkNodes.length()-1));

	
		context.write(blockID, new Text(mapperValue.toString()));
	}
}
