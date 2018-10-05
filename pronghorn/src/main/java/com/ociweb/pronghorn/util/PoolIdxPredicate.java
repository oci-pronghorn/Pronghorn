package com.ociweb.pronghorn.util;

public interface PoolIdxPredicate {

	PoolIdxPredicate allOk = new PoolIdxPredicate() {		
		@Override
		public boolean isOk(int i) {
			return true;
		}
	};
	
	/**
	 * 
	 * @param i the value in question, is this value i an acceptable choice
	 * @return true if this is an acceptable choice, does not guarantee that is is chosen.
	 */
	boolean isOk(int i);

}
