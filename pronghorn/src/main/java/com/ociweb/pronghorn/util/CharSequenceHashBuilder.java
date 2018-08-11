package com.ociweb.pronghorn.util;

import java.util.Arrays;
import java.util.Comparator;

public class CharSequenceHashBuilder {
    
    //TODO: add a reset method to use this again
    private final LengthComparitor lengthComparitor = new LengthComparitor();
    private final int[] lengthCountSpace = new int[32+1]; //lengths are 32bit integers
    private static final int   MAX_VALUE_IDX = 32;//the index where we find the max value
    private final int[] largestGroup = new int[32];
    private byte[] shiftsStack = new byte[32];
    private int    shiftStackDepth = 0;
    private int[]  charHashSeed = new int[32];
    private int    charHashSeedDepth = 0;
    
    
    public CharSequenceHashBuilder() {        
    }
    

    
    private boolean recursiveBalanceScan(byte[] shifts, int depth, int primaryMask, int[] values, int base, int[] largeGroupCount, int[] countSpace) {
                
        int idx = depth - 1;     
        //System.out.println("idx "+idx+" base "+Integer.toBinaryString(base));
        if (idx < 0) {
            //use this mask and test this case            
            boolean needsMoreSplitting = countSplitBalancePerBit(values, primaryMask, base, countSpace);
            if (needsMoreSplitting) {
                accumulateLargestGroups(countSpace, largeGroupCount);
            }
            return needsMoreSplitting; //go no deeper
            
        }
        
        return recursiveBalanceScan(shifts, idx, primaryMask, values, base, largeGroupCount, countSpace) | //with zero for the mask
               recursiveBalanceScan(shifts, idx, primaryMask, values, base | (1<<shifts[idx]), largeGroupCount, countSpace);
        //returns true if any group needs to be split more, return false if we are all done
    }
    
    private boolean countSplitBalancePerBit(int[] values, int filterMask, int filterCheck, int[] countSpace) {
        Arrays.fill(countSpace, 0);
        
        int i = values.length;
        int maxTotal = 0;
        
        
        while (--i >= 0) {
            if (filterCheck == (filterMask&values[i])) {
                maxTotal++; //count total members of this group
                sumEachBit(filterMask, countSpace, values[i], countSpace.length-1);
            }            
       }
       countSpace[countSpace.length-1] = maxTotal;
        
       return maxTotal > 1;
    }

    private void sumEachBit(int filterMask, int[] countSpace, int length, int j) {
        while (--j >= 0) {
            //only consider bits that have not already been chosen
            if (0==(filterMask&(1<<j))) {
                countSpace[j] += (1 & (length >> j)); //counting the 1 bits
            }
        }
    }
    
    private void accumulateLargestGroups(int[] localCountSpace, int[] target) {
        
        int k = localCountSpace.length-1;
        int all = localCountSpace[k];
        while (--k >= 0) {
                int biggerSide = Math.max(all-localCountSpace[k], localCountSpace[k]);  
                target[k] = (int) Math.max(target[k], biggerSide); 
        }

    }
    
   
    
    private int findIdxOfSmallestGroup(int[] largestGroupSizes, int maxValue, int alreadyPicked) {

        int result = -1;
        int k = largestGroupSizes.length;
        while (--k >= 0) {            
                int groupSize = largestGroupSizes[k];                
                if (groupSize >= 1 && 
                    groupSize <= maxValue &&
                    (0==((1<<k)&alreadyPicked)) ) {
                    maxValue = groupSize;
                    result = k;
                }           
        }
        return result;
    }
    
    GeneratedHashRules perfectIntegerHashBuilder(int[] values) {
    
        //does not always make the space smaller,
        //if every value is a power of two a perfect hash can not be made and a different (simpler) approach should be taken.
        
        //TODO: add assert that the values are all unique.
           
        byte bitIndex = 0;
        int accumMask = 0;
        boolean needsMoreSplitting;
        shiftStackDepth = 0;

        do {
            Arrays.fill(largestGroup, 0); 
            needsMoreSplitting = recursiveBalanceScan(shiftsStack, shiftStackDepth, accumMask, values, 0, largestGroup, lengthCountSpace);
            bitIndex = (byte)findIdxOfSmallestGroup(largestGroup, Integer.MAX_VALUE, accumMask);
           
            if (bitIndex>=0) {
                
                accumMask |= (1<<bitIndex);
                shiftsStack[shiftStackDepth++] = bitIndex;//this is the number of bits to shift                
             
                if (1==largestGroup[bitIndex]) {
                    needsMoreSplitting = false;
                    break;
                }
            } else {
                break;
            }
            
        } while (true);
        
        return new GeneratedHashRules(needsMoreSplitting, accumMask, shiftsStack, shiftStackDepth, null, 0);
      
    }
    
    GeneratedHashRules perfectCharSequenceHashBuilder(final CharSequence[] values) {
        
        final int mixBits = 0;
        int sigBits = 1;
        
        int maxValueLen = 0;
        int m = values.length;
        while (--m>=0) {
            if (values[m].length()>maxValueLen) {
                maxValueLen = values[m].length();
            }
        }
        maxValueLen = maxValueLen<<sigBits;
        
        int[] charLenCountSpace = new int[maxValueLen+1];
        int[] charLenCountGroups = new int[maxValueLen];
        
        boolean isDone = false;
        
        byte bitShift = 0;        
        int shiftsStackAsBits = 0;
        
        
        shiftStackDepth = 0;
        
        boolean needsMoreSplitting = true;
        int lastGroupSize = Integer.MAX_VALUE;

        do {
            
            if (bitShift>=0) {
                Arrays.fill(largestGroup, 0);
                
                needsMoreSplitting = recursiveBalanceScan(values, shiftsStack, shiftStackDepth, 0, shiftsStackAsBits, 
                                                          charHashSeed, -1, -1, 0, largestGroup, lengthCountSpace, mixBits, sigBits);
                
                //extracting the length bits
                bitShift = (byte)findIdxOfSmallestGroup(largestGroup, (lastGroupSize>>1)+1, shiftsStackAsBits);
                
                if (bitShift>=0) {
                    
                    int groupSize = largestGroup[bitShift];
                    
                    int newBit = 1<<bitShift;
                    if ((shiftsStackAsBits & newBit) != 0) {
                        System.err.println("picked same bit twice");
                        lastGroupSize = Integer.MAX_VALUE;
                        bitShift = -1;//done, we picked a bit we are already using, so must move to next stage
                    } else {
                        //System.out.println("len group size "+groupSize);
                        shiftsStackAsBits |= newBit;
                        shiftsStack[shiftStackDepth++] = bitShift;                
                    }
                    lastGroupSize=groupSize;
                } else {
                    //the value here discards groups where all the values are the same size therefore we must start over before starting the char scan
                    lastGroupSize = Integer.MAX_VALUE;
                }
                
            } else {
                
                
                //now extracting the char indexes                
                Arrays.fill(charLenCountGroups, 0);
                
                needsMoreSplitting = recursiveBalanceScan(values, shiftsStack, shiftStackDepth, 0, shiftsStackAsBits, 
                                     charHashSeed, charHashSeedDepth, charHashSeedDepth, 0, charLenCountGroups, charLenCountSpace, mixBits, sigBits);
                
                int seedIdx = findIdxOfSmallestGroup(charLenCountGroups, lastGroupSize,0);
                                
                if (seedIdx>=0) {
                    
                    int groupSize = charLenCountGroups[seedIdx];
                    
                    charHashSeed[charHashSeedDepth++] = seedIdx;
                    lastGroupSize = groupSize;
                    
                    if (1==groupSize) {
                        needsMoreSplitting = false;
                        break;
                    }
                    
                    
                } else {
                   break;                  
                }
                
            }
            
            
            
        } while (true);
        
        return new GeneratedHashRules(needsMoreSplitting, shiftsStackAsBits, shiftsStack, shiftStackDepth, charHashSeed, charHashSeedDepth);
      
    }
    

    private boolean countSplitBalancePerBitA(CharSequence[] values, int filterMaskForLen, int filterCheckForLen, 
                                            int[] charSeeds, int charSeedsDepth, int charSeedsFilter, int[] countSpace, int mixBits, int sigBits) {
        Arrays.fill(countSpace, 0);
        
        int i = values.length;
        int maxTotal = 0;
        
        int commonLength = -1;
        boolean allSame = true;
        while (--i >= 0) {
            CharSequence value = values[i];
            int length = value.length();            
            if (filterCheckForLen == (filterMaskForLen&length)) {
                
                    if (commonLength<0) {
                        commonLength=length;
                    } else {
                        allSame &= (commonLength==length);
                    }
                
                    maxTotal++; //count total members of this group
                    sumEachBit(filterMaskForLen, countSpace, length, countSpace.length-1);
            }            
       }
       countSpace[countSpace.length-1] = maxTotal;
       return !allSame;
    }
    
    private boolean countSplitBalancePerBitB(CharSequence[] values, int filterMaskForLen, int filterCheckForLen,
            int[] charSeeds, int charSeedsDepth, int charSeedsFilter, int[] countSpace, int mixBits, int sigBits) {
        Arrays.fill(countSpace, 0);

        // these two are constants for the hash.
        // run once with 0, 0 and grow them as needed?

        final int mixerShift = mixBits; // NOTE: selector should favor 0
        final int mixerMask = (1 << mixBits) - 1; // can adjust bigger for
                                                  // sparse similar values, or
                                                  // smaller

        if (mixerMask!=0) {
            throw new UnsupportedOperationException();
        }
        
        final int cBitShift = sigBits; // NOTE:selector should favor 0
        final int cBitsMask = (1 << sigBits) - 1; // can adjust smaller or
                                                  // bigger

        int i = values.length;
        int totalElementsInSelectedGroup = 0;
        while (--i >= 0) {
            CharSequence value = values[i];
            int length = value.length();
            if (filterCheckForLen == (filterMaskForLen & length)) {
                // allow everything except defined char seeds that do not match
                if (isApplicable(mixerShift, mixerMask, cBitShift, cBitsMask, value, length, charSeeds, charSeedsDepth, charSeedsFilter)) {
                    totalElementsInSelectedGroup++; // count total members of this group
                    
                    // count space dictates how many values will be tried
                    int n = countSpace.length - 1;// save last value for total
                                                  // size of group
                    while (--n >= 0) {
                        countSpace[n] += charSplitBit(mixerShift, mixerMask, cBitShift, cBitsMask, n, value, length);
                    }

                }
            }
        }

        countSpace[countSpace.length - 1] = totalElementsInSelectedGroup;
    
    return totalElementsInSelectedGroup>1;
    }

    private boolean isApplicable(final int mixerShift, final int mixerMask, final int cBitShift, final int cBitsMask, CharSequence value, int length, int[] charSeeds, int charSeedsLength, int charSeedsFilter) {
        boolean applicable = true;
        int c = charSeedsLength;
        
        while (--c >= 0 && applicable) {            
            int checkSeedBit = 1&(charSeedsFilter>>c);//expecting 1 or 0            
            applicable &= (checkSeedBit == charSplitBit(mixerShift, mixerMask, cBitShift, cBitsMask, charSeeds[c], value, length));
        }

        return applicable;
    }

    private int charSplitBit(final int mixerShift, final int mixerMask, final int cBitShift, final int cBitsMask, int n, CharSequence cs, int len) {
        return splitBit(mixerMask, cBitShift, cBitsMask, n, cs, len, n >> mixerShift);
    }

    private int splitBit(final int mixerMask, final int cBitShift, final int cBitsMask, int n, CharSequence cs, int len, int bitIndx) {
        return splitBit(cs, len, bitIndx & cBitsMask, bitIndx >> cBitShift, 0, (n & mixerMask)+1);
    }

    private int splitBit(CharSequence cs, int len, int bitShift, int cPos, int c, int mixCount) {
        int t = mixCount;
        while (--mixCount >= 0){
            c += cs.charAt((mixCount+cPos) % len);
        }                 
        return 1 & (c>>bitShift);
    }
    

    private boolean recursiveBalanceScan(final CharSequence[] values, 
                                          byte[] lengthShifts, final int lengthShifsDepth, int lengthShiftGroupFilter, int lengthShiftsInBitFormat,
                                          int[] charSeeds,     int charSeedsDepth, int charSeedCountDown, int charSeedsGroupFilter, int[] largeGroupCount, int[] countSpace,
                                          int mixBits, int sigBits) {
                
        int lengthFilterLeft  = lengthShiftGroupFilter;
        int lengthFitlerRight = lengthShiftGroupFilter;
        
        int charSeedsGroupFilterLeft = charSeedsGroupFilter;
        int charSeedsGroupFilterRight = charSeedsGroupFilter;
        
    //    System.out.println(lengthShifsDepth+" "+charSeedsDepth);
        
        int charSeedFilter = charSeedCountDown;
        
        final int lengthBitFilter = lengthShifsDepth - 1;     
        if (lengthBitFilter < 0) {
         //   lengthBitFilter = lengthShifsDepth;
            //we have reached the bottom of the length masks, the lengthShiftGroupFilter is as detailed as it will get.
                        
            //are we at the true bottom or continuing of the chars
          //  charSeedFilter--;
            charSeedFilter--;
            if (charSeedFilter < 0 ) {
                
                //this is the true bottom of both lengths and charSeeds

                //use this mask and test this case            
                boolean needsSplit;
                
                if (-1==charSeedsDepth) {
                    needsSplit = countSplitBalancePerBitA(values, lengthShiftsInBitFormat, lengthShiftGroupFilter, charSeeds, charSeedsDepth, charSeedsGroupFilter, countSpace, mixBits, sigBits);
                } else {
                    needsSplit = countSplitBalancePerBitB(values, lengthShiftsInBitFormat, lengthShiftGroupFilter, charSeeds, charSeedsDepth, charSeedsGroupFilter, countSpace, mixBits, sigBits);
                }
                
                //only review those that need splitting
                if (needsSplit) {
                    accumulateLargestGroups(countSpace, largeGroupCount);
                }
                
                return needsSplit;                
                
            } else {
               //charSeedsGroupFilterLeft |= 0; //nothing to do
               charSeedsGroupFilterRight |= (1<<charSeedFilter);
            }
        } else {            
            //lengthFilterLeft  |= 0 ;//nothing to do
            lengthFitlerRight   |= (1<<lengthShifts[lengthBitFilter]);
        }
        
        return 
                recursiveBalanceScan(values, lengthShifts, lengthBitFilter, lengthFilterLeft, lengthShiftsInBitFormat,   
                                                    charSeeds, charSeedsDepth, charSeedFilter, charSeedsGroupFilterLeft,largeGroupCount,countSpace, mixBits, sigBits) 
                |
                
               recursiveBalanceScan(values, lengthShifts, lengthBitFilter, lengthFitlerRight, lengthShiftsInBitFormat, 
                                                    charSeeds, charSeedsDepth, charSeedFilter, charSeedsGroupFilterRight,largeGroupCount,countSpace, mixBits, sigBits );
       
    }



    
    private static void build(CharSequence[] values, int[] includeStack, int depth) {
        
        
        //only apply some of the list to do this recursivly.
        //check length mask include bits
        //check length mask exclude bits
        
        //filter length first then this
        //check list of bits to include 
        //check list of bits to exclude
        
        //for case where depth is zero and we do not split the list
        
        int i = values.length;
        while (--i >= 0) {
            
            
            CharSequence value = values[i];
            //divide by length first then use bits.
            
            if (isValueSelected(value)) {
                //find which bit most evenly splits the group.
                //store the lengths as well
                
                //sorted list of lenghts? find the middle?
                
                
                
            }
            
            
            
            
            
            
            
            
        }
        
        
    }


    private static boolean isValueSelected(CharSequence value) {
        
        //TODO: needs the extra args to check the full stack.
        
        return true;
    }
    
    
    public static class LengthComparitor implements Comparator<CharSequence> {

        @Override
        public int compare(CharSequence o1, CharSequence o2) {
            return Integer.compare(o1.length(), o2.length());
        }
        
        
    }
}
