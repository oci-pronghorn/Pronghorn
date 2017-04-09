package com.ociweb.pronghorn.util;

import java.util.Arrays;

public class RollingBloomFilter extends BloomFilter {

	private final long[] bloom2;
	private final long half;
	
	public RollingBloomFilter(long n, double p) {
		super(n,p);
		
		bloom2 = new long[bloom.length];
		half = n/2;
		
	}

	 public long estimatedSize() {
		 return super.estimatedSize()+(8L*bloom2.length)+8L;
	 }
	
	@Override
	public void clear() {
		super.clear();
		Arrays.fill(bloom, 0);
	}

	@Override
	protected int updateBloom(long[] bloom, long bloomMask, int seen, int hash32) {
		// TODO Auto-generated method stub
		seen = super.updateBloom(bloom, bloomMask, seen, hash32);
				
		if (memberCount()>half) {
			
			//also record here since we are in the second half
			int secondSeen = super.updateBloom(bloom2, bloomMask, 1, hash32);
						
			if (memberCount()>=n) {
				//switch to the second now that we have reached the end.
				memberCount = n/2;
				System.arraycopy(bloom2, 0, bloom, 0, bloom2.length);
				Arrays.fill(bloom2, 0);
		    }	
		}
		
		return seen;
	}


	
	
}
