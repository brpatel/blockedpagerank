package blockedPR;

import java.util.ArrayList;
import java.util.List;

public class BlockedPRConstants {

	public final static String LIST_SEPERATOR = ",";
	public final static String VALUE_SEPERATOR = "_";

	public final static int TOTAL_NODES = 100;
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


	public static enum PR_COUNTER {
		RESIDUALS_AVG,
		RESIDUALS_SUM,
		NODES_COUNT
	};

	public static List<Integer> blockList = new ArrayList<Integer>();
}
