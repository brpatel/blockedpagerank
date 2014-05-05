package blockedPR;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class BlockedPRInputReducer extends Reducer<Text, Text, Integer, Text> {



	@Override
	protected void reduce(Text key, 
			java.lang.Iterable<Text> values, 
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Integer, Text>.Context context) 
					throws IOException ,InterruptedException 
					{
		Double pageRank = (double) 1/BlockedPRConstants.TOTAL_NODES;
		System.out.println("inside blockedPRInputreducer");
		Integer sourceNode = Integer.parseInt(key.toString());
		Integer blockID = BlockedPRConstants.getBlockIdFromNode(sourceNode);

		StringBuilder outLinkNodes = new StringBuilder();
		for (Text value : values) {
			outLinkNodes.append(value.toString());
			outLinkNodes.append(BlockedPRConstants.LIST_SEPERATOR);
		} 
		StringBuilder mapperValue = new StringBuilder(BlockedPRConstants.STEP1_ID+sourceNode+BlockedPRConstants.VALUE_SEPERATOR+pageRank.toString()+
				BlockedPRConstants.VALUE_SEPERATOR +outLinkNodes.substring(0, outLinkNodes.length()-1));

		System.out.println("PRInput - PR in reduce: blockID "+ pageRank + "===   " + blockID);

		context.write(blockID, new Text(mapperValue.toString()));
	}
}
