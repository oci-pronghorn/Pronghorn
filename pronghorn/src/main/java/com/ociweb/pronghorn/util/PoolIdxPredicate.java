package com.ociweb.pronghorn.util;

public interface PoolIdxPredicate {

	/**
	 * 
	 * @param i the value in question, is this value i an acceptable choice
	 * @return true if this is an acceptable choice, does not guarantee that is is chosen.
	 */
	boolean isOk(int i);

}
