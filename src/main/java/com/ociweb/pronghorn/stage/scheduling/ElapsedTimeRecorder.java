package com.ociweb.pronghorn.stage.scheduling;

public class ElapsedTimeRecorder {

	private final int[] buckets; 
	private long totalCount;
	
	public ElapsedTimeRecorder() {
		
		buckets = new int[64];
		
	}
	
	public static void record(ElapsedTimeRecorder that, long valueNS) {		
		that.buckets[63 - Long.numberOfLeadingZeros(valueNS)]++;
		that.totalCount++;
	}
	
	public static long elapsedAtPercentile(ElapsedTimeRecorder that, float pct) {
		long targetCount = (long)(pct * that.totalCount);
		
		if (0 != targetCount) {
			int i = 0;
			int j = 0; //max;
			while (i<64) {
				
				int b = that.buckets[i++];
				if (b>0) {
					j = i;
				}
				if (targetCount<=b) {
					
					long floor = i<=1 ? 0 : (1L<<(i-2));
					long ceil = (1L<<(i-1));
					
					long dif = ((ceil-floor) * targetCount)/(long)b ; 
					
					return floor + dif; 
					
				} else {
					targetCount -= b;				
				}
			}
			
			return 1L<<(j-1);//largest value		
		} else {
			return 0;
		}
	}
	
	
}
