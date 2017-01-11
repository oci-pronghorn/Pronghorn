package com.ociweb.pronghorn.util.math;

public class Decimal {

    public static void sum(final long aM, final int aE, final long bM, final int bE, DecimalResult result) {
    	
		if (aE==bE) {
			result.result(aM + bM, bE);
		} else if (aE>bE){
			int dif = (aE-bE);		
			//if dif is > 18 then we will loose the data anyway..  
			long temp =	dif>=longPow.length? 0 : aM*longPow[dif];
			result.result(bM + temp, bE);
		} else {
			int dif = (bE-aE);						
			long temp =	dif>=longPow.length? 0 : bM*longPow[dif];
			result.result(aM+temp, aE);
		}
		
    }
    
	private static long[] longPow = new long[] {1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 
            1_000_000_000, 10_000_000_000L, 100_000_000_000L ,1000_000_000_000L,
            1_000_000_000_000L, 10_000_000_000_000L, 100_000_000_000_000L ,1000_000_000_000_000L,
            1_000_000_000_000_000L, 10_000_000_000_000_000L, 100_000_000_000_000_000L ,1000_000_000_000_000_000L};
	

}
