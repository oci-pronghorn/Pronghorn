package com.ociweb.pronghorn.util;

public class GeneratedHashRules {

    private final boolean needsMoreSplitting;
    private final int lenMask;
    private final byte[] lengthShiftsStack;
    private final int lengthShiftStackDepth;
    private final int[] charHashSeed;
    private final int charHashSeedDepth;
    
    public GeneratedHashRules(boolean needsMoreSplitting, int lenMask, byte[] lengthShiftsStack, int lengthShiftStackDepth, int[] charHashSeed, int charHashSeedDepth) {
        this.needsMoreSplitting = needsMoreSplitting;
        this.lenMask = lenMask; //may not be the most optimal way to pull this value
        this.lengthShiftsStack = lengthShiftsStack;
        this.lengthShiftStackDepth = lengthShiftStackDepth;
        this.charHashSeed = charHashSeed;
        this.charHashSeedDepth = charHashSeedDepth;
    }

    //add method for doing hash here.
    
    public boolean needsMoreSplitting() {
        return needsMoreSplitting;
    }
    
    public int lengthMask() {
        return lenMask;
    }
    
    public void report() {
        
        
        System.out.println("Split more : "+needsMoreSplitting);
        System.out.println("Split mask : "+Integer.toBinaryString(lenMask));
        System.out.println("Seeds:"+charHashSeedDepth);
        for(int i = 0; i<charHashSeedDepth;i++) {
            System.out.println("seed: "+charHashSeed[i]);
        }
               
        
    }
    
    //TODO: take in the mix and sig bit values
    //TODO: generate hash given the selected fields
    //TODO: broaden tests and add slow path for similar values
    
//    //building this up as we learn what the compiled signature contains
//    private int hashCharSequence(CharSequence cs, int lengthBits, int[][] charMasks) {                
//        int length = cs.length();
//        int result = gatherBits(length, lengthBits);        
//        return appendSignifiantBitsFromCharSequence(cs, length, result, charMasks[result]);
//    }
//
//    private int appendSignifiantBitsFromCharSequence(CharSequence cs, int length, int result, int[] masks) {
//        for(int i = 0; i<masks.length; i++) {
//            int charBit = masks[i];
//            int idx =charBit>>4;
//            result = (result<<1) | (1&((int) (idx>=length ? 0 : cs.charAt(idx))>>(charBit&0xF)));
//        }
//        return result;
//    }
//    
//    //NOTE: for any fixed mask this can be code generated to be much faster for those specific bits.
//    private int gatherBits(int source, int mask) {        
//        int result = 0;
//        int shift = 32;
//        while (--shift >= 0) {
//            if (0 == (1&(mask>>shift))) {                
//            } else {
//                result = (result<<1) | (1&(source>>shift));                
//            }
//        }
//        return result;
//    }

}
