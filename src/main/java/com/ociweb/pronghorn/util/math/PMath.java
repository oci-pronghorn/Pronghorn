package com.ociweb.pronghorn.util.math;

/*
 * @Author Nathan Tippy
 */
public class PMath {
    
    //WARNING: asking for large primes or factors may cause this array to grow out of control
    private static int[] primes = new int[] {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 
                                             101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193};

    //can deal with prime factors up to the "length" prime number    
    public static void factors(long value, byte[] target, int offset, int length, int mask) {
        
        int pIndex = 0;
        do {
            int exponent = 0;
            if (1 != value) { //no work to do if the value is 1
            
                int p = primeAtIdx(pIndex++);
                boolean continueCheck = false;
                do {
        
                    long d = value/p;
                    long r = value%p;
                    
                    if (r==0) {
                        value = d;
                        exponent++;
                        continueCheck = (d!=1);
                    } else {
                        continueCheck = false;
                    }
                    
                } while (continueCheck);
            }
            target[mask&offset++] = (byte)exponent; 
            //increment even it was not divisible so we can record that fact as blank spot
           
        } while (--length>0);
        
    }
    
    public static void greatestCommonFactor( byte[] backingA, int offsetA, int lengthA, int maskA,
                                             byte[] backingB, int offsetB, int lengthB, int maskB,
                                             byte[] target,   int offset, int length, int mask) {
        
        if (length<lengthA || length<lengthB) {
            throw new UnsupportedOperationException("Target array must be as large as either of the input arrays. Only found room for "+length+" but requires "+(Math.max(lengthA, lengthB)));
        }
        
        while (--length>=0) {
            
            int a = length>=lengthA ? 0 : (backingA[(offsetA+length)&maskA]);
            int b = length>=lengthB ? 0 : (backingB[(offsetB+length)&maskB]);
            target[(offset+length)&mask] = (byte)Math.min(a, b);
                        
        }

    }
    
    
    public static void greatestCommonFactor( byte[][] backingA, int[] offsetA, int[] lengthA, int[] maskA,
                                             byte[] target, int offset, int length, int mask) {        

        assert(isNotLessThanAny(length, lengthA));
        
        while (--length>=0) {
            
            int min = Integer.MAX_VALUE;            
            int i = backingA.length;
            while (--i>=0) {
                assert( (backingA[i][(offsetA[i]+length)&maskA[i]]) >= 0) : "only works on integers not rationals";                
                min = Math.min((int) (length>=lengthA[i] ? 0 : (backingA[i][(offsetA[i]+length)&maskA[i]])), min);
            }
            
            target[(offset+length)&mask] = (min==Integer.MAX_VALUE?0:(byte)min);
                        
        }

    }   
    
    
    private static boolean isNotLessThanAny(int x, int[] y) {
        int i = y.length;
        while (--i>=0) {
            if (x<y[i]) {
                return false;
            }
        }
        return true;
    }

    /*
     * A contains the factors of B and we want them removed. The result is in target
     * 
     * This operation is the same as integer divide. the result is A/B
     */
    public static void removeFactors( byte[] backingA, int offsetA, int lengthA, int maskA,
                                     byte[] backingB, int offsetB, int lengthB, int maskB,
                                     byte[] target,   int offset, int length, int mask) {
     
        if (length<lengthA || length<lengthB) {
            throw new UnsupportedOperationException("Target array must be as large as either of the input arrays. Only found room for "+length+" but requires "+(Math.max(lengthA, lengthB)));
        }
        
        while (--length>=0) {
            
            int a = length>=lengthA ? 0 : (backingA[(offsetA+length)&maskA]);
            int b = length>=lengthB ? 0 : (backingB[(offsetB+length)&maskB]);
            target[(offset+length)&mask] = (byte)(a-b);
                        
        }
        
    }
    
    /*
     * A contains the factors of B and we want them removed. The result is in target
     * Any factors not found in A are not removed. Eg. this is a modulus divide leaving the remainder.
     */
    public static void removeExistingFactors( byte[] backingA, int offsetA, int lengthA, int maskA,
                                             byte[] backingB, int offsetB, int lengthB, int maskB,
                                             byte[] target,   int offset, int length, int mask) {
     
        if (length<lengthA || length<lengthB) {
            throw new UnsupportedOperationException("Target array must be as large as either of the input arrays. Only found room for "+length+" but requires "+(Math.max(lengthA, lengthB)));
        }
        
        while (--length>=0) {
            
            int a = length>=lengthA ? 0 : (backingA[(offsetA+length)&maskA]);
            int b = length>=lengthB ? 0 : (backingB[(offsetB+length)&maskB]);
            target[(offset+length)&mask] = (byte)Math.max(a-b,0); //never goes negative
                        
        }
        
    }
    
    
    /*
     * 
     * 
     * This operation is the same as integer multiply. the result is A*B
     */
    public static void addFactors( byte[] backingA, int offsetA, int lengthA, int maskA,
                                  byte[] backingB, int offsetB, int lengthB, int maskB,
                                  byte[] target,   int offset, int length, int mask) {
     
        if (length<lengthA || length<lengthB) {
            throw new UnsupportedOperationException("Target array must be as large as either of the input arrays. Only found room for "+length+" but requires "+(Math.max(lengthA, lengthB)));
        }
        
        while (--length>=0) {
            
            int a = length>=lengthA ? 0 : (backingA[(offsetA+length)&maskA]);
            int b = length>=lengthB ? 0 : (backingB[(offsetB+length)&maskB]);
            target[(offset+length)&mask] = (byte)(a+b);
                        
        }        
    }
    
    
    public static int factorsToInt(byte[] target, int offset, int length, int mask) {
        int value = 1;
        while (--length>=0) {
            int j = target[(offset+length)&mask];
            if (j<0) {
                throw new UnsupportedOperationException("This rational number can not be expressed as an integer");
            }
            while (--j>=0) {
                value = value * primeAtIdx(length);
            }
        }
        return value;
    }
    
    public static long factorsToLong(byte[] target, int offset, int length, int mask) {
        long value = 1;
        while (--length>=0) {
            int j = target[(offset+length)&mask];
            if (j<0) {
                throw new UnsupportedOperationException("This rational number can not be expressed as an integer");
            }
            while (--j>=0) {
                value = value * primeAtIdx(length);
            }
        }
        return value;
    }
    
    /**
     * Grows the internal array as needed. Then returns the prime at that index.
     * NOTE: 0 index will return 2 and 1 index will return 3  (they are zero based)
     * @param i
     * @return
     */
    private static int primeAtIdx(int i) {

        int[] localPrimes = primes;
        while (i>=localPrimes.length) {
            //Must build out primes to the required index
                        
            int v = localPrimes[localPrimes.length-1];
            
            while (!isPrime(++v)) {}
            
            int[] newPrimes = new int[primes.length+1];
            System.arraycopy(primes, 0, newPrimes, 0, primes.length);
            newPrimes[primes.length]=v;
            localPrimes = primes = newPrimes;
            
        } 
        
        //return the value
        return primes[i];
    }

    /**
     * Not a general method, this only works for up to numbers 1 larger than the last prime discovered.
     * @param i
     * @return true if is prime
     */
    private static boolean isPrime(int i) {
        int j = primes.length;
        while (--j>=0) {
            if (i%primes[j] == 0) {
                return false;
            }
        }        
        return true;
    }


    public static ScriptedSchedule buildScriptedSchedule(long[] schedulePeriods) {
    	return buildScriptedSchedule(schedulePeriods, false);
    }
    
    /**
     * 
     * @param schedulePeriods array of periods that the item at each index is epxpected to run
     * @param reverseOrder the reversed order schedule may be desirable under heavy load conditions with directed graphs.
     * @return new scripted schedule object to be used at runtime.
     */
    public static ScriptedSchedule buildScriptedSchedule(long[] schedulePeriods, final boolean reverseOrder) {

    	assert(schedulePeriods.length<Integer.MAX_VALUE) : "Maximum schedule can only be "+Integer.MAX_VALUE;
    	
        int maxPrimeBits  = 4;
        int maxPrimes     = 1<<maxPrimeBits;
        int maxPrimesMask = maxPrimes-1;
                
        byte[][] factors = new byte[schedulePeriods.length][];
        int[] offsets = new int[schedulePeriods.length];
        int[] lengths = new int[schedulePeriods.length];
        int[] masks = new int[schedulePeriods.length];        
        
        for(int i=0;i<schedulePeriods.length;i++) {
            lengths[i] = maxPrimes;
            masks[i] = maxPrimesMask;
            factors[i] = new byte[maxPrimes];
            factors(schedulePeriods[i], factors[i], 0, maxPrimes, maxPrimesMask);            
        }
        
        final byte[] gcm = new byte[maxPrimes];               
        greatestCommonFactor(factors, offsets, lengths, masks,
                                   gcm, 0, maxPrimes, maxPrimesMask);
        
        final long commonClock = factorsToLong(gcm, 0, maxPrimes, maxPrimesMask);
        
        //remove GCM from each rate and roll-up steps to find the point when the schedule loops
      
        byte[] repeatLength = new byte[maxPrimes];
        byte[] temp = new byte[maxPrimes];
        int[] steps = new int[schedulePeriods.length];
        int[] bases = new int[schedulePeriods.length];
        int largestPrimeIdx = -1;
        for(int i=0;i<schedulePeriods.length;i++) {
           
            //remove the GCM from the factors for this particular rate
            removeFactors(factors[i], 0, maxPrimes, maxPrimesMask,
                               gcm,        0, maxPrimes, maxPrimesMask,
                               factors[i], 0, maxPrimes, maxPrimesMask);
            
            //from the remaining factors remove the ones already accounted for in repeat length
            removeExistingFactors(factors[i],          0, maxPrimes, maxPrimesMask,
                                       repeatLength,        0, maxPrimes, maxPrimesMask,
                                       temp,                0, maxPrimes, maxPrimesMask);
                        
            //add the unaccounted for factors into repeat length
            addFactors(repeatLength, 0, maxPrimes, maxPrimesMask,
                            temp,         0, maxPrimes, maxPrimesMask,
                            repeatLength, 0, maxPrimes, maxPrimesMask);
            
            steps[i] = factorsToInt(factors[i], 0, maxPrimes, maxPrimesMask);
            
            //finding the index of the largest prime used in any of these.
            largestPrimeIdx = Math.max(largestPrimeFactorIdx(factors[i],0,maxPrimes,maxPrimesMask), largestPrimeIdx);
            
        }
        int groupsCount = factorsToInt(repeatLength, 0, maxPrimes, maxPrimesMask);
        int scriptLength = groupsCount;//one for the -1 (stop flag) of each iteration
       
        for(int i=0;i<schedulePeriods.length;i++) {
            assert(0 == (groupsCount%steps[i])): "Internal compute error";
            scriptLength += (groupsCount/steps[i]);
            
            ///NOTE: we can count how many have the steps[i] value and divide them into steps[i] groups...
            //       in most cases this is probably not required but consider it for the future.
                        
            //what if everything with same freq has same prime?
            //then those in a run.... are expected to run together.

            //each must start at a different base to minimize collision.
            //by using increasing prime numbers we ensure a good distribution
            bases[i]=primeAtIdx(++largestPrimeIdx);
 
        }
    
        // -1 is the end of a block
        int[] script = new int[scriptLength];
        int s = 0;
        int maxRun = 0;
        for(int r = 0; r < groupsCount; r++) {
            
            int runCount = 0;

            if (reverseOrder) {
            	int i = schedulePeriods.length;
            	while (--i>=0) {
	                
            		if (0==((bases[i] + r) % steps[i])) {
	                    if (++runCount >= maxRun) {
	                        maxRun = runCount;
	                    }
	                    script[s++]=i;
	                }
	                
	            }
            } else {            
            	//NOTE: this run covers all the items to run at the "same" time
	            for(int i=0;i<schedulePeriods.length;i++) {	            	
	         	
            		if (0==((bases[i] + r) % steps[i])) {
	                    if (++runCount >= maxRun) {
	                        maxRun = runCount;
	                    }
	                    script[s++]=i;
	            	}
		           
	            }
            }
             
            
            
            //finished with run.
            script[s++] = -1;
        
        
        }
        //System.out.println(Arrays.toString(script));
        
        return new ScriptedSchedule(commonClock, script, maxRun);
    }

    private static int largestPrimeFactorIdx(byte[] target, int offset, int length, int mask) {

        while (--length>=0) {
            int j = target[(offset+length)&mask];
            if (j!=0) {
                return length;
            }
        }
        return -1;
    }
    
}
