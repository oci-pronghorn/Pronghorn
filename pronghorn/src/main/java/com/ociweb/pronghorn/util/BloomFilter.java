package com.ociweb.pronghorn.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

import com.ociweb.pronghorn.pipe.util.hash.MurmurHash;

public class BloomFilter implements Serializable {

    // n    Number of items in the filter
    // p    Probability of false positives, float between 0 and 1 or a number indicating 1-in-p
    
    // m    Number of bits in the filter,  m = ceil((n * log(p)) / log(1.0 / (pow(2.0, log(2.0)))));
    // k    Number of hash functions, k = round(log(2.0) * m / n);
    

    private static final long serialVersionUID = -5898034207488511755L;
    
    protected final long n;
    private final int k; 
    private final double p;
    
    
    private final int bloomBits;
    private final long bloomMask;
    public final int bloomSize;//in longs
    
    protected final long[] bloom;
    private final int[] bloomSeeds;
    protected long memberCount;
    
    //TODO: replace murmur hash with Rabin for deterministic collision rates
    
    private static transient final Random r = new Random(777);
    
    public BloomFilter(long n, double p) {
        this(p, n, hashFunCount(n, p), bitsInFilter(n, p));   
        assert(p<=1);
    }
    
    private static int hashFunCount(long n, double p) {
        return (int)Math.round(Math.log(2.0) * bitsInFilter(n,p) / n);
    }
    
    public boolean isMatchingSeeds(BloomFilter that) {
        return Arrays.equals(this.bloomSeeds, that.bloomSeeds);
    }
    
    private static long bitsInFilter(long n, double p) {
        return (long)Math.ceil((n * Math.log(p)) / Math.log(1.0d / (Math.pow(2.0, Math.log(2.0)))));
    }
        
    private BloomFilter(double p, long n, int k, long m) {
        this(p,n,k,(int)Math.ceil(Math.log(m)/Math.log(2)),buildSeeds(k));
    }
    
    public BloomFilter(BloomFilter template) {
        this(template.p,template.n,template.k,template.bloomBits,template.bloomSeeds);
    }
    
    public BloomFilter(BloomFilter a, BloomFilter b, boolean intersection) {
        this(a.p,a.n,a.k,a.bloomBits,a.bloomSeeds);
        
        assert(a.bloomSize == b.bloomSize) : "Operation only supported on compatible filters, try creating both from the same template filter.";
        assert(Arrays.equals(a.bloomSeeds, b.bloomSeeds)) : "Operation only supported on compatible filters, try creating both from the same template filter.";
        
        int j = a.bloom.length;
        
        if (intersection) {
            while (--j>=0) {
                bloom[j] = a.bloom[j] & b.bloom[j];
            }
        } else {
            while (--j>=0) {
                bloom[j] = a.bloom[j] | b.bloom[j];
            }
        }
        
    }
    
    public void clear() {
    	Arrays.fill(bloom, 0);
    }
    
    private BloomFilter(double p, long n, int k, int bits, int[] seeds) {
        this.p = p;
        this.n = n;
        this.k = k;
        
        if(bits<6) {//smallest allowed
            bits = 6;
        }
        this.bloomBits = bits;
        this.bloomMask = (1L<<bloomBits)-1;
        long temp = 1L<<(bloomBits-(3+3));
        assert(temp<=(long)Integer.MAX_VALUE) : Long.toBinaryString(temp)+" bits "+bloomBits;
        this.bloomSize = (int)temp;  
                        
        this.bloom = new long[(int)bloomSize];
        this.bloomSeeds = seeds;
        
        //System.out.println("seeds "+bloomSeeds.length);
        
    }
    
    public float pctConsumed() {
        
        int count = 0;
        int i = bloom.length;
        while (--i>=0) {
            count += Long.bitCount(bloom[i]);
        }        
        return count/(float)(bloom.length*64);
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
        
        memberCount -= (seen-1);
        
        return seen;
    }
    
    private int add(byte[] source, int sourcePos, int sourceLen, int sourceMask, long[] bloom, int[] bloomSeeds, long bloomMask) {
        int i = bloomSeeds.length;
        int seen = 1;
        while (--i>=0) {
            seen = updateBloom(bloom, bloomMask, seen, MurmurHash.hash32(source, sourcePos, sourceLen, sourceMask, bloomSeeds[i]));  
        }
        
        memberCount -= (seen-1);
        
        return seen;
    }

    protected int updateBloom(long[] bloom, long bloomMask, int seen, int hash32) {
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
    
    public boolean mayContain(byte[] source, int sourcePos, int sourceLen, int sourceMask) {
        return isFound(source, sourcePos, sourceLen, sourceMask, bloom, bloomSeeds, bloomMask);
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
    
    public static long getDataAtIndex(BloomFilter filter, int idx) {
        return filter.bloom[idx];
    }
    
    private static boolean isFound(byte[] source, int sourcePos, int sourceLen, int sourceMask, long[] bloom, int[] bloomSeeds, long bloomMask) {

        int i = bloomSeeds.length;
        while (--i>=0) {
            long h = MurmurHash.hash32(source, sourcePos, sourceLen, sourceMask, bloomSeeds[i])&bloomMask;
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
