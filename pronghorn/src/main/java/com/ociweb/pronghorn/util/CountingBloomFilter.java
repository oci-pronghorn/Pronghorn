package com.ociweb.pronghorn.util;

import java.io.Serializable;
import java.util.Random;

import com.ociweb.pronghorn.pipe.util.hash.MurmurHash;

public class CountingBloomFilter implements Serializable {

    // n    Number of items in the filter
    // p    Probability of false positives, float between 0 and 1 or a number indicating 1-in-p
    
    // m    Number of bits in the filter,  m = ceil((n * log(p)) / log(1.0 / (pow(2.0, log(2.0)))));
    // k    Number of hash functions, k = round(log(2.0) * m / n);
    

    private static final long serialVersionUID = -5898034207488511755L;
    
    private final long n;
    private final int k; 
    private final double p;
    
    private final int bloomBits;
    private final long bloomMask;
    private final long bloomLongSize;
    
    private final static int countBits = 16;//Must be a power of 2
    private final static int countSize = 1<<countBits;
    private final static int countMask = countSize-1;
    
    
    private final long[] bloom;
    private final int[] bloomSeeds;
    private long memberCount;
    
    private static transient final Random r = new Random(777);

    //TODO: requires unit tests
    @Deprecated //not ready for use
    public CountingBloomFilter(long n, double p) {
        this(p, n, hashFunCount(n, p), bitsInFilter(n, p));   
        assert(p<=1);
    }
    
    private static int hashFunCount(long n, double p) {
        return (int)Math.round(Math.log(2.0) * bitsInFilter(n,p) / n);
    }
    
    private static long bitsInFilter(long n, double p) {
        return (long)Math.ceil((n * Math.log(p)) / Math.log(1.0d / (Math.pow(2.0, Math.log(2.0)))));
    }
        
    private CountingBloomFilter(double p, long n, int k, long m) {
        this.p = p;
        this.n = n;
        this.k = k;
                
        this.bloomBits = (int)Math.ceil(Math.log(m)/Math.log(2));
        this.bloomMask = (1L<<bloomBits)-1;
        this.bloomLongSize = (countBits<<(bloomBits-(3+3))); 
                        
        this.bloom = new long[(int)bloomLongSize];
        this.bloomSeeds = buildSeeds(k);
                
    }
    
    public float pctConsumed() {
        
        int count = 0;
        int i = bloom.length;
        while (--i>=0) {
            if (0 != bloom[i]) {
                count++;
            }
        }        
        return count/(float)bloom.length;
    }
    
    public long estimatedSize() {
        return 8L+4L+8L+(3L*4L)+(4L*(long)bloomSeeds.length)+(2L*8L)+(8L*(long)bloom.length);
    }
    
    public long memberCount() {
        return memberCount;
    }
    
    public long memberSoftLimit() {
        return n;
    }
    
    private static int[] buildSeeds(int k) {
        int[] result = new int[k];
        synchronized(r) {
            while (--k>=0) {
                result[k] = r.nextInt();
            }
        }
        return result;
    }
    
    /**
     * Returns 0 if this was a new addition, and 1 if this may have been and old value
     */
    public int addValue(CharSequence value) {
        return add(value, bloom, bloomSeeds, bloomMask);
    }

    public int addValue(byte[] source, int sourcePos, int sourceLen, int sourceMask) {
        return add(source, sourcePos, sourceLen, sourceMask, bloom, bloomSeeds, bloomMask);
    }
    
    /**
     * Returns 0 if this was a new addition, and 1 if this may have been and old value
     * @return
     */
    private int add(CharSequence value, long[] bloom, int[] bloomSeeds, long bloomMask) {
        int i = bloomSeeds.length;
        int seen = 1;
        while (--i>=0) {
            seen = updateBloom(bloom, bloomMask, seen, MurmurHash.hash32(value, bloomSeeds[i]));  
        }
        memberCount++;
        return seen;
    }
    
    private int add(byte[] source, int sourcePos, int sourceLen, int sourceMask, long[] bloom, int[] bloomSeeds, long bloomMask) {
        int i = bloomSeeds.length;
        int seen = 1;
        while (--i>=0) {
            seen = updateBloom(bloom, bloomMask, seen, MurmurHash.hash32(source, sourcePos, sourceLen, sourceMask, bloomSeeds[i]));  
        }
        memberCount++;
        return seen;
    }

    private int updateBloom(long[] bloom, long bloomMask, int seen, int hash32) {
        long h = (hash32 & bloomMask) << countBits ;

        int idx = 0x7FFF_FFFF & (int)(h>>6);
        int shift = (int)h&0x3F;
        
        int incValue = (int)((bloom[idx]>>shift)&countMask)+1;
        if (incValue>countMask) {
            incValue = countMask;//saturation;
        }
        incValue = incValue<<shift;
        long mask =  ~(((long)countMask)<<shift);
        bloom[idx] = (bloom[idx]&mask)|incValue;
        return seen&1;
    }
    
    public boolean mayContain(CharSequence value) {
        return countFound(value, bloom, bloomSeeds, bloomMask)>0;
    }
    
    public boolean mayContain(byte[] source, int sourcePos, int sourceLen, int sourceMask) {
        return countFound(source, sourcePos, sourceLen, sourceMask, bloom, bloomSeeds, bloomMask)>0;
    }

    public int maxCount(CharSequence value) {
        return countFound(value, bloom, bloomSeeds, bloomMask);
    }
    
    public int maxCount(byte[] source, int sourcePos, int sourceLen, int sourceMask) {
        return countFound(source, sourcePos, sourceLen, sourceMask, bloom, bloomSeeds, bloomMask);
    }

    
    private static int countFound(CharSequence value, long[] bloom, int[] bloomSeeds, long bloomMask) {

        int i = bloomSeeds.length;
        int min = Integer.MAX_VALUE;
        while (--i>=0) {
            long h = MurmurHash.hash32(value,bloomSeeds[i])&bloomMask;
            min = Math.min(min, countFound(bloom, h));
        }
        return min;
    }

    private static int countFound(long[] bloom, long h) {
        
        h = h << countBits ;
        
        int idx = 0x7FFF_FFFF & (int)(h>>6);
        int shift = (int)(h&0x3F); //values 0 to 63
        
        return (int)((bloom[idx]>>shift)&countMask);
        
    }
    
    private static int countFound(byte[] source, int sourcePos, int sourceLen, int sourceMask, long[] bloom, int[] bloomSeeds, long bloomMask) {

        int i = bloomSeeds.length;
        int min = Integer.MAX_VALUE;
        while (--i>=0) {
            long h = MurmurHash.hash32(source, sourcePos, sourceLen, sourceMask, bloomSeeds[i])&bloomMask;
            min = Math.min(min, countFound(bloom, h));
        }
        return min;
    }

    /**
     * Returns 0 if this was a new addition, and 1 if this may have been and old value
     */
    public int add(int[] slab, int slabPos, int slabMask, int slabLength, 
                   byte[] blob, int blobPos, int blobMask, int blobLength) {
        
        int i = bloomSeeds.length;
        int seen = 1;
        while (--i>=0) {
            int seed = bloomSeeds[i];
            int slabHash = MurmurHash.hash32(slab, slabPos, slabLength, slabMask, seed);
            int blobHash = MurmurHash.hash32(blob, blobPos, blobLength, blobMask, seed);            
            seen = updateBloom(bloom, bloomMask, seen, MurmurHash.hash32(slabHash, blobHash, seed));  
        }
        return seen;
    }

    
}
