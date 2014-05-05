package blockedPR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BlockedPRConstants {

	public final static String LIST_SEPERATOR = ",";
	public final static String VALUE_SEPERATOR = "_";

	public final static int TOTAL_NODES = 685230;
	public final static double D = 0.85;
	public static final String STEP1_ID = "1#";
	public static final String STEP2_ID = "2#";
	
	public static final String INPUT_JOB_INPUT = "/user/hue/100edges_input";
	public static final String INPUT_JOB_OUTPUT = "/user/hue/100edges_input_job_output";
	public static final String CALCULATE_JOB_INPUT = "/user/hue/100edges_calculate_job_input";
	public static final String CALCULATE_JOB_OUTPUT = "/user/hue/100edges_calculate_job_output";
	public static final String JOIN_JOB_INPUT = "/user/hue/100edges_join_job_input";	
	public static final String JOIN_JOB_OUTPUT = "/user/hue/100edges_join_job_output";
	public static final String BLOCK_PATH_STRING = "/user/hue/blocks.txt";
	public static final String OUTPUT_JOB_OUTPUT = "/user/hue/100edges_output";
	public static final int Iterations = 5;

	/*public static final String INPUT_JOB_INPUT = "100edges_input";
	public static final String INPUT_JOB_OUTPUT = "100edges_input_job_output";
	public static final String CALCULATE_JOB_INPUT = "100edges_calculate_job_input";
	public static final String CALCULATE_JOB_OUTPUT = "100edges_calculate_job_output";
	public static final String JOIN_JOB_INPUT = "100edges_join_job_input";	
	public static final String JOIN_JOB_OUTPUT = "100edges_join_job_output";
	public static final String BLOCK_PATH_STRING = "blocks.txt";*/
	

	public static enum PR_COUNTER {
		RESIDUALS_AVG,
		RESIDUALS_SUM,
		NODES_COUNT,
		BLOCK_COUNT
	};

	public static List<Integer> blockList = Arrays.asList(
	10328, 20373, 30629, 40645,
        50462, 60841, 70591, 80118, 90497, 100501, 110567, 120945,
        130999, 140574, 150953, 161332, 171154, 181514, 191625, 202004,
        212383, 222762, 232593, 242878, 252938, 263149, 273210, 283473,
        293255, 303043, 313370, 323522, 333883, 343663, 353645, 363929,
        374236, 384554, 394929, 404712, 414617, 424747, 434707, 444489,
        454285, 464398, 474196, 484050, 493968, 503752, 514131, 524510,
        534709, 545088, 555467, 565846, 576225, 586604, 596585, 606367,
        616148, 626448, 636240, 646022, 655804, 665666, 675448, 685230 );
	
	public static int getBlockIdFromNode(int nodeId){

		System.out.println("nodeID===" + nodeId);
		
		List<Integer> blockList =  BlockedPRConstants.blockList;

		for(int i= 0; i<blockList.size();i++ )
		{
			if(blockList.get(i) >= nodeId)
			{
				System.out.println("list index ===" + i);
				return i;
			}
				
		}
		
		System.out.println("list index ENDDD===");
		return 0;

	}
}
