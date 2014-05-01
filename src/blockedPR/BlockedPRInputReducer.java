package blockedPR;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class BlockedPRInputReducer extends Reducer<Text, Text, Text, Text> {



	@Override
	protected void reduce(Text key, 
			java.lang.Iterable<Text> values, 
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context) 
					throws IOException ,InterruptedException 
					{
		Double pageRank = (double) 1/BlockedPRConstants.TOTAL_NODES;

		StringBuilder outLinkNodes = new StringBuilder();
		for (Text value : values) {
			outLinkNodes.append(value.toString());
			outLinkNodes.append(BlockedPRConstants.LIST_SEPERATOR);
		} 
		StringBuilder mapperValue = new StringBuilder(BlockedPRConstants.STEP1_ID+pageRank.toString()+
				BlockedPRConstants.VALUE_SEPERATOR +outLinkNodes.substring(0, outLinkNodes.length()-1));


		//System.out.println("PRInput - Reduce - Key: "+key.toString() + " Value: " + mapperValue.toString());

		context.write(key, new Text(mapperValue.toString()));
					}

	private Integer getBlockIdFromNode(Integer nodeId){

		List<Integer> blockList =  BlockedPRConstants.blockList;

		for(Integer blockNode : blockList)
		{
			if(blockNode >= nodeId)
				return blockList.indexOf(blockNode);
		}

		return 0;

	}

}
