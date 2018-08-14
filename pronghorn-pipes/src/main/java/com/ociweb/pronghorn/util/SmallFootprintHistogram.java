package com.ociweb.pronghorn.util;

import java.io.IOException;
import java.util.Arrays;

import com.ociweb.pronghorn.util.Appendables;

public class SmallFootprintHistogram {

	private final int[] buckets = new int[64]; 
	private final long[] sums   = new long[64]; 
	
	
	private long totalCount;
	private long maxValue;
	
	public SmallFootprintHistogram() {
				
	}
	
	public String toString() {
		return report(new StringBuilder()).toString();
	}
	
	public static long totalCount(SmallFootprintHistogram that) {
		return that.totalCount;
	}
	
	public <A extends Appendable> A report(A target) {
		try {
			Appendables.appendValue(target.append("Total:"), totalCount).append("\n");
			Appendables.appendValue(target, SmallFootprintHistogram.elapsedAtPercentile(this, .25f)).append(" 25 percentile\n");
			Appendables.appendValue(target, SmallFootprintHistogram.elapsedAtPercentile(this, .50f)).append( " 50 percentile\n");
			Appendables.appendValue(target, SmallFootprintHistogram.elapsedAtPercentile(this, .80f)).append( " 80 percentile\n");
			Appendables.appendValue(target, SmallFootprintHistogram.elapsedAtPercentile(this, .90f)).append( " 90 percentile\n");
			Appendables.appendValue(target, SmallFootprintHistogram.elapsedAtPercentile(this, .95f)).append( " 95 percentile\n");
			Appendables.appendValue(target, SmallFootprintHistogram.elapsedAtPercentile(this, .98f)).append( " 98 percentile\n");
			Appendables.appendValue(target, SmallFootprintHistogram.elapsedAtPercentile(this, .99f)).append( " 99 percentile\n");
			Appendables.appendValue(target, SmallFootprintHistogram.elapsedAtPercentile(this, .999f)).append( " 99.9 percentile\n");
			Appendables.appendValue(target, SmallFootprintHistogram.elapsedAtPercentile(this, .9999f)).append( " 99.99 percentile\n");
			Appendables.appendValue(target, SmallFootprintHistogram.elapsedAtPercentile(this, .99999f)).append( " 99.999 percentile\n");
			Appendables.appendValue(target, SmallFootprintHistogram.elapsedAtPercentile(this, .999999f)).append( " 99.9999 percentile\n");
			Appendables.appendValue(target, SmallFootprintHistogram.elapsedAtPercentile(this, 1f)).append( " max\n");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return target;
	}
	
	public static void record(SmallFootprintHistogram that, long valueNS) {

		int base = 64 - Long.numberOfLeadingZeros(valueNS);
		that.buckets[base]++;
		that.sums[base]+=valueNS;
	
		that.totalCount++;
		that.maxValue = Math.max(that.maxValue, valueNS);
	}
	
	public static long elapsedAtPercentile(SmallFootprintHistogram that, double pct) {
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

	public void add(SmallFootprintHistogram source) {
		int i = buckets.length;
		while (--i>=0) {
			buckets[i] += source.buckets[i];	
			sums[i]+= source.sums[i];
		}
		totalCount += source.totalCount;
		maxValue = Math.max(maxValue, source.maxValue);
		
	}

	public static void clear(SmallFootprintHistogram that) {
		that.totalCount = 0;
		that.maxValue = 0;
		Arrays.fill(that.buckets, 0);	
		Arrays.fill(that.sums, 0);
	}
	
	
}
