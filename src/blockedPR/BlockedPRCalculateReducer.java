package blockedPR;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class BlockedPRCalculateReducer extends Reducer<Text, Text, Text, Text> {

	/***
	 * INPUT: <b(u);{u,PR(u),{v|u->v}, {<v,PR(u)/deg(u)>|v|u->v}, {{<v,R> }}> 
	 * : <b(u);{1#u_PR(u)_v1,v2,v3_v1,PR(u)/d(u)_v2,PR(u)/d(u)_...,2#v5,PR(u)/d(u)}>
	 * OUTPUT: 
	 */
	@Override
	protected void reduce(Text key, 
			java.lang.Iterable<Text> values, 
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context) 
					throws IOException ,InterruptedException 
					{

		HashMap<Integer, String> sourceToDestinationMap = new HashMap<Integer, String>();
		HashMap<Integer, Double> destinationWithSourcePRMap = new HashMap<Integer, Double>(); // Key is destination , value is a comma separate value, PR(s), PR(s+1)
		HashMap<Integer, Double> blockConstantMap = new HashMap<Integer, Double>(); // Blocked constants
		context.getCounter(BlockedPRConstants.PR_COUNTER.BLOCK_COUNT).increment(1);
		int numberOfNodesinBlock = 0;

		for(Text value : values)
		{

			String inputValue = value.toString();

			if(inputValue.contains(BlockedPRConstants.STEP1_ID)){

				inputValue = inputValue.split(BlockedPRConstants.STEP1_ID)[1];
				System.out.println("inside blocked pr calculate reduce");
				System.out.println("first value ==" + value);
				numberOfNodesinBlock++;	
				String[] splitValues = inputValue.split(BlockedPRConstants.VALUE_SEPERATOR);
				Integer srcNode = Integer.parseInt(splitValues[0]);
				//System.out.println(" split[1] = " + splitValues[0] +"_" + splitValues[1] +"_"+ splitValues[2]);
				sourceToDestinationMap.put(srcNode, splitValues[1]+BlockedPRConstants.VALUE_SEPERATOR+splitValues[2]);

				String[] listOfDest =  splitValues[2].split(BlockedPRConstants.LIST_SEPERATOR);

				for( int i = 0; i < listOfDest.length; i++)
				{
					if(splitValues.length != 4 + i)
						System.out.println(inputValue.toString());
					else{
						if(destinationWithSourcePRMap.containsKey(Integer.parseInt(listOfDest[i])))
						{
							Double pr = destinationWithSourcePRMap.get(Integer.parseInt(listOfDest[i]));
							pr += Double.parseDouble(splitValues[3+i].split(BlockedPRConstants.LIST_SEPERATOR)[1]);
							destinationWithSourcePRMap.put(Integer.parseInt(listOfDest[i]), pr);
						}
						else
						{
							Double pr = Double.parseDouble(splitValues[3 + i].split(BlockedPRConstants.LIST_SEPERATOR)[1]);
							destinationWithSourcePRMap.put(Integer.parseInt(listOfDest[i]), pr);				
						}

					}
				}
			}else if(inputValue.contains(BlockedPRConstants.STEP2_ID))
			{
				inputValue = inputValue.split(BlockedPRConstants.STEP2_ID)[1];
				String[] splitValues = inputValue.split(BlockedPRConstants.VALUE_SEPERATOR);
				Integer srcNode = Integer.parseInt(splitValues[0]);
				Double R = Double.parseDouble(splitValues[1]);
				if(blockConstantMap.containsKey(srcNode))
				{
					Double previousR = blockConstantMap.get(srcNode);
					previousR += R;
					blockConstantMap.put(srcNode, previousR);
				}
				else
				{
					blockConstantMap.put(srcNode, R);				
				}

			}


		}

		double residual = 0.0;
		HashSet <Integer> dstNodeSet = new HashSet<Integer>();
		HashMap<Integer, Double> previousPageRank = new HashMap<Integer, Double>();


		for(int j=0;j<BlockedPRConstants.Iterations;j++)
		{
			dstNodeSet.clear();

			double innerResidual =0.0;
			for(Map.Entry<Integer,String> entry : sourceToDestinationMap.entrySet())
			{
				double pageRank = 0.0;

				Integer srcKey = (Integer) entry.getKey();
				String destinationNodes = (String) entry.getValue();	
				destinationNodes = destinationNodes.split(BlockedPRConstants.VALUE_SEPERATOR)[1];//list of destination nodes, [0] - PR(S)
				String[] destNodes = destinationNodes.split(BlockedPRConstants.LIST_SEPERATOR);// each destination
				Integer degree = destNodes.length;

				System.out.println("degree ==== " + degree);

				if(!previousPageRank.containsKey(srcKey))
					previousPageRank.put(srcKey, Double.parseDouble(sourceToDestinationMap.get(srcKey).split(BlockedPRConstants.VALUE_SEPERATOR)[0]));

				pageRank = ((1.0/numberOfNodesinBlock) * (1 - BlockedPRConstants.D));
				// Add PR from nodes  with in Block : BE
				if (destinationWithSourcePRMap.containsKey(srcKey))
					pageRank += ( BlockedPRConstants.D * destinationWithSourcePRMap.get(srcKey));

				//// Add PR nodes from outside Block : BC
				if(blockConstantMap.containsKey(srcKey))
					pageRank += BlockedPRConstants.D * blockConstantMap.get(srcKey);


				double prByDegree = pageRank/degree;
				System.out.println("pageRank ==== " + pageRank);
				System.out.println("prByDegree ==== " + prByDegree);
				double pr =0.0;

				for( int i = 0; i < degree; i++)
				{	
					if (dstNodeSet.contains(Integer.parseInt(destNodes[i])))
					{
						System.out.println("in if ==== ");
						pr = destinationWithSourcePRMap.get(Integer.parseInt(destNodes[i]));
						pr += prByDegree;
						destinationWithSourcePRMap.put(Integer.parseInt(destNodes[i]), pr);
					}
					else
					{
						System.out.println("in else ");
						pr = prByDegree;
						destinationWithSourcePRMap.put(Integer.parseInt(destNodes[i]), prByDegree);
						dstNodeSet.add(Integer.parseInt(destNodes[i]));
					}
				}

				// calculate inner residual
				double prevPR = previousPageRank.get(srcKey);
				innerResidual += (Math.abs(prevPR - pageRank)/pageRank);

				if (j==BlockedPRConstants.Iterations - 1)
				{					
					StringBuilder mapperValue = new StringBuilder(BlockedPRConstants.STEP1_ID + srcKey + BlockedPRConstants.VALUE_SEPERATOR  + pageRank +
							BlockedPRConstants.VALUE_SEPERATOR + sourceToDestinationMap.get(srcKey).split(BlockedPRConstants.VALUE_SEPERATOR)[1]);

					double oldPageRank = Double.parseDouble(sourceToDestinationMap.get(srcKey).split(BlockedPRConstants.VALUE_SEPERATOR)[0]);
					System.out.println("pageRank ==== " + pageRank);
					residual += (Math.abs(oldPageRank - pageRank)/pageRank);
					context.getCounter(BlockedPRConstants.PR_COUNTER.NODES_COUNT).increment(1);

					context.write(key, new Text(mapperValue.toString()));
				}

				previousPageRank.put(srcKey, pageRank);
			}
			System.out.println(" converted LONG for pass ==" + (long)((residual/numberOfNodesinBlock)*1000));
			System.out.println("Inner avg residual for  iteration " + j + "  is == " + innerResidual/numberOfNodesinBlock);
		}

		System.out.println("residual ====blockID " + residual + "==="+ key);

		System.out.println(" converted LONG ===" + (long)((residual/numberOfNodesinBlock)*1000));
		context.getCounter(BlockedPRConstants.PR_COUNTER.RESIDUALS_SUM).increment((long)((residual/numberOfNodesinBlock)*1000));
					}

}
