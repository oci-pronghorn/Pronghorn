package com.ociweb.pronghorn.stage.filter;

import java.util.Random;

import com.ociweb.pronghorn.pipe.util.hash.MurmurHash;

public class BloomFilter {

    // n    Number of items in the filter
    // p    Probability of false positives, float between 0 and 1 or a number indicating 1-in-p
    
    // m    Number of bits in the filter,  m = ceil((n * log(p)) / log(1.0 / (pow(2.0, log(2.0)))));
    // k    Number of hash functions, k = round(log(2.0) * m / n);
    
    private final long n;
    private final int k; 
    private final double p;
    
    private final int bloomBits;
    private final long bloomMask;
    private final long bloomLongSize;
    
    private final long[] bloom;
    private final int[] bloomSeeds;
    
    private static final Random r = new Random(777);
    
    public BloomFilter(long n, double p) {
        this(p, n, hashFunCount(n, p), bitsInFilter(n, p));   
        assert(p<=1);
    }
    
    private static int hashFunCount(long n, double p) {
        return (int)Math.round(Math.log(2.0) * bitsInFilter(n,p) / n);
    }
    
    private static long bitsInFilter(long n, double p) {
        return (long)Math.ceil((n * Math.log(p)) / Math.log(1.0d / (Math.pow(2.0, Math.log(2.0)))));
    }
        
    private BloomFilter(double p, long n, int k, long m) {
        this.p = p;
        this.n = n;
        this.k = k;
                
        this.bloomBits = (int)Math.ceil(Math.log(m)/Math.log(2));
        this.bloomMask = (1L<<bloomBits)-1;
        this.bloomLongSize = 1L<<(bloomBits-(3+3)); 
                
        System.out.println("bloom array size "+bloomLongSize);
        
        this.bloom = new long[(int)bloomLongSize];
        this.bloomSeeds = buildSeeds(k);
        
        //System.out.println("seeds "+bloomSeeds.length);
        
    }
    
    public long estimatedSize() {
        return 8L+4L+8L+(3L*4L)+(8L*bloom.length)+(4L*bloomSeeds.length)+(2L*8L);
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
     * @return
     */
    public int addValue(CharSequence value) {
        return add(value, bloom, bloomSeeds, bloomMask);
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
        return seen;
    }

    private int updateBloom(long[] bloom, long bloomMask, int seen, int hash32) {
        long h = hash32 & bloomMask;
        int idx = 0x7FFF_FFFF & (int)(h>>6);
        int shift = (int)h&0x3F;
        long val = 1L<<shift; //values 0 to 63  //  11 1111
        long prev = bloom[idx];
        seen = seen & (int)((prev&val)>>shift);
        bloom[idx] = prev | val ;
        return seen;
    }
    

    public boolean mayContain(CharSequence value) {
        return isFound(value, bloom, bloomSeeds, bloomMask);
    }
    
    private static boolean isFound(CharSequence value, long[] bloom, int[] bloomSeeds, long bloomMask) {

        int i = bloomSeeds.length;
        while (--i>=0) {
            long h = MurmurHash.hash32(value,bloomSeeds[i])&bloomMask;
            int idx = 0x7FFF_FFFF & (int)(h>>6);
            long val = 1L<<(h&0x3F); //values 0 to 63
            if (0 == (bloom[idx]&val)) {
                return false;
            }
        }
        return true;
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
