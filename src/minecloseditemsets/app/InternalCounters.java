package minecloseditemsets.app;

public enum InternalCounters {
	
	/**
	 * Counts the number of emits by the reducer.
	 * Used as the stopping condition for the loop.
	 */
	REDUCER_OUTPUT,
	
	/**
	 * Counts the number of reducer emits in every step.
	 * Summed up in the main job.
	 */
	REDUCE_EMITS,

	/**
	 * Number of candidates pruned by the mapper
	 * because the new item was not frequent.
	 */
	PRUNED_MESSAGES
}
