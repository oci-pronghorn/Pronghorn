package com.ociweb.pronghorn.util;

import java.util.HashSet;
import java.util.Set;

public class ByteTestSequenceVisitor implements ByteSquenceVisitor {

		Set<Long> result_set = new HashSet<Long>();
		
		@Override
		public void addToResult(long l) {
			result_set.add(l);
		}
		
		public void clearResult(){
			result_set.clear();
		}
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			for(long l: result_set){
				sb.append(l).append(" ");
			}
			return sb.toString().trim();
		}
	
	
}
