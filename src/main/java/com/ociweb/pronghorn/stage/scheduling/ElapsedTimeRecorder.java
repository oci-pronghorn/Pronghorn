package com.ociweb.pronghorn.stage.scheduling;

import com.ociweb.pronghorn.util.Appendables;

public class ElapsedTimeRecorder {

	private final int[] buckets; 
	private long totalCount;
	
	public ElapsedTimeRecorder() {
		
		buckets = new int[64];
		
	}
	
	public String toString() {
		return report(new StringBuilder()).toString();
	}
	
	public <A extends Appendable> A report(A target) {
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .50f), " 50 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .80f), " 80 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .90f), " 90 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .95f), " 95 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .98f), " 98 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .99f), " 99 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .999f), " 99.9 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .9999f), " 99.99 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, 1f), " max update\n");
		return target;
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

	public void add(ElapsedTimeRecorder source) {
		int i = buckets.length;
		while (--i>=0) {
			buckets[i] += source.buckets[i];
		}
		
	}
	
	
}
