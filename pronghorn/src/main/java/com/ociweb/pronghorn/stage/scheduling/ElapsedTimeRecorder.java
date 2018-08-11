package com.ociweb.pronghorn.stage.scheduling;

import java.io.IOException;
import java.util.Arrays;

import com.ociweb.pronghorn.util.Appendables;

public class ElapsedTimeRecorder {

	private final int[] buckets = new int[65]; 
	private final long[] sums   = new long[65]; 
	
	
	private long totalCount;
	private long maxValue;
	
	public ElapsedTimeRecorder() {
				
	}
	
	public String toString() {
		return report(new StringBuilder()).toString();
	}
	
	public static long totalCount(ElapsedTimeRecorder that) {
		return that.totalCount;
	}
	
	public <A extends Appendable> A report(A target) {
		try {
			Appendables.appendValue(target.append("Total:"), totalCount).append("\n");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .25f), " 25 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .50f), " 50 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .80f), " 80 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .90f), " 90 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .95f), " 95 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .98f), " 98 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .99f), " 99 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .999f), " 99.9 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .9999f), " 99.99 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .99999f), " 99.999 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, .999999f), " 99.9999 percentile\n");
		Appendables.appendNearestTimeUnit(target, ElapsedTimeRecorder.elapsedAtPercentile(this, 1f), " max\n");
		return target;
	}
	
	public static void record(ElapsedTimeRecorder that, long valueNS) {

		int base = 64 - Long.numberOfLeadingZeros(valueNS);
		that.buckets[base]++;
		that.sums[base]+=valueNS;
	
		that.totalCount++;
		that.maxValue = Math.max(that.maxValue, valueNS);
	}
	
	public static long elapsedAtPercentile(ElapsedTimeRecorder that, double pct) {
		if (pct>1) {
			throw new UnsupportedOperationException("pct should be entered as a value between 0 and 1 where 1 represents 100% and .5 represents 50%");
		}		
		long targetCount = (long)Math.rint(pct * that.totalCount);
		if (targetCount==that.totalCount) {
			return that.maxValue;
		}
		
		if (0 != targetCount) {
			int i = 0;
			while (i<that.buckets.length-1) {
				
				long sum = that.sums[i];
				int b = that.buckets[i++];
		
				if (targetCount<=b) {
				
					long floor = i<=1 ? 0 : (1L<<(i-2));					
					long avg = sum/(long)b;
										
					long dif;
					int half = b>>1;
					if (half>1) {
						long center = avg-floor;
						
						//weighted to the average
						if (targetCount<half) {
							dif = (center * targetCount)/half ;						
						} else {
							
							dif = center+(((floor-center) * (targetCount-half))/half) ;
						}
					} else {
						//this is the old linear less accurate solution
						dif = ((2*floor) * targetCount)/(long)b ; 
					}
					return Math.min(that.maxValue, floor + dif); 
					
				} else {
					targetCount -= b;				
				}
			}
			return that.maxValue;		
		} else {
			return 0;
		}
	}

	public void add(ElapsedTimeRecorder source) {
		int i = buckets.length;
		while (--i>=0) {
			buckets[i] += source.buckets[i];	
			sums[i]+= source.sums[i];
		}
		totalCount += source.totalCount;
		maxValue = Math.max(maxValue, source.maxValue);
		
	}

	public static void clear(ElapsedTimeRecorder that) {
		that.totalCount = 0;
		that.maxValue = 0;
		Arrays.fill(that.buckets, 0);	
		Arrays.fill(that.sums, 0);
	}
	
	
}
