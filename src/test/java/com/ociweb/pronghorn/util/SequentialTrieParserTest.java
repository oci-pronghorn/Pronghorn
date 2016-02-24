package com.ociweb.pronghorn.util;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;

public class SequentialTrieParserTest {

    byte[] data1 = new byte[]{101,102,103,104,105,106,107,108};
    
    byte[] data2  = new byte[]{106,107,108,109,110,111,112,113};
    byte[] data2b = new byte[]{106,107,108,109,110,111,118,119};
    byte[] data3  = new byte[]{106,107,108,109,120,121,122,123};
    byte[] data3b = new byte[]{106,107,108,109,120,121,(byte)128,(byte)129};
    
    byte[] data4 = new byte[]{106,107,108,109,(byte)130,(byte)131,(byte)132,(byte)133};
    
    byte[] data5 = new byte[]{106,117,118,119,110,111,112,113};
    
    int value1 = 10;
    int value2 = 23;
    int value3 = 41;
    int value4 = 57;
    
    
    //TODO: add int block add string block? 
    //TODO: do static to return lenght and value
    //TODO: test different combos
    //TODO: add check for remaining size.
    //TODO: do performance check of shorts vs bytes? (duplicate class to test them at the same time)

    
    @Test
    public void testSimpleValueReplace() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);
                
        map.setValue(data1, 0, 3, 7, value1);        
        assertEquals(value1, reader.query(map,data1, 0, 3, 7));
        
        map.setValue(data1, 0, 3, 7, value2);        
        assertEquals(value2, reader.query(map,data1, 0, 3, 7));
                
    }
    
    @Test
    public void testSimpleValueReplaceWrapping() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);
        
        
        map.setValue(data1, 5, 5, 7, value1);        
        assertEquals(value1, reader.query(map,data1, 5, 5, 7));
        
        map.setValue(data1, 5, 5, 7, value2);        
        assertEquals(value2, reader.query(map,data1, 5, 5, 7));
                
    }
    
    @Test
    public void testTwoNonOverlapValuesWithReplace() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data1, 1, 3, 7, value1);
        map.setValue(data2, 1, 3, 7, value2);
                        
        assertEquals(value1, reader.query(map,data1, 1, 3, 7));
        assertEquals(value2, reader.query(map,data2, 1, 3, 7));
        
        //swap values
        map.setValue(data1, 1, 3, 7, value2);
        map.setValue(data2, 1, 3, 7, value1);
        
        assertEquals(value2, reader.query(map,data1, 1, 3, 7));
        assertEquals(value1, reader.query(map,data2, 1, 3, 7));        
        
    }
    
    @Test
    public void testTwoNonOverlapValuesWrappingWithReplace() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data1, 5, 5, 7, value1);
        map.setValue(data2, 5, 5, 7, value2);
         
        assertEquals(value1, reader.query(map,data1, 5, 5, 7));
        assertEquals(value2, reader.query(map,data2, 5, 5, 7));
        
        //swap values
        map.setValue(data1, 5, 5, 7, value2);
        map.setValue(data2, 5, 5, 7, value1);
        
        assertEquals(value2, reader.query(map,data1, 5, 5, 7));
        assertEquals(value1, reader.query(map,data2, 5, 5, 7));        
        
    }
    
    
    @Test
    public void testTwoOverlapValues() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data2, 2, 5, 7, value1);
        map.setValue(data3, 2, 5, 7, value2);
                        
        assertEquals(value1, reader.query(map,data2, 2, 5, 7));
        assertEquals(value2, reader.query(map,data3, 2, 5, 7));
        
        //swap values
        map.setValue(data2, 2, 5, 7, value2);
        map.setValue(data3, 2, 5, 7, value1);
        
        assertEquals(value2, reader.query(map,data2, 2, 5, 7));
        assertEquals(value1, reader.query(map,data3, 2, 5, 7));        
        
    }
    
    @Test
    public void testThreeOverlapValues() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data3, 2, 5, 7, value2);
        map.setValue(data4, 2, 5, 7, value3);
        map.setValue(data2, 2, 5, 7, value1);
                
        assertEquals(value1, reader.query(map,data2, 2, 5, 7));
        assertEquals(value2, reader.query(map,data3, 2, 5, 7));
        assertEquals(value3, reader.query(map,data4, 2, 5, 7));
        
        //swap values
        map.setValue(data2, 2, 5, 7, value3);
        map.setValue(data3, 2, 5, 7, value2);
        map.setValue(data4, 2, 5, 7, value1);
        
        assertEquals(value1, reader.query(map,data4, 2, 5, 7));
        assertEquals(value2, reader.query(map,data3, 2, 5, 7));
        assertEquals(value3, reader.query(map,data2, 2, 5, 7));        
        
    }
    
    @Test
    public void testInsertBeforeBranch() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data3, 0, 6, 7, value1);
        map.setValue(data4, 0, 6, 7, value2);
        map.setValue(data5, 0, 6, 7, value3);

        
        assertEquals(value1, reader.query(map,data3, 0, 6, 7));
        assertEquals(value2, reader.query(map,data4, 0, 6, 7));
        assertEquals(value3, reader.query(map,data5, 0, 6, 7));
        
        //swap values
        map.setValue(data3, 0, 6, 7, value3);
        map.setValue(data4, 0, 6, 7, value2);
        map.setValue(data5, 0, 6, 7, value1);
        
        assertEquals(value1, reader.query(map,data5, 0, 6, 7));
        assertEquals(value2, reader.query(map,data4, 0, 6, 7));
        assertEquals(value3, reader.query(map,data3, 0, 6, 7));        
        
    }
    
    @Test
    public void testInsertAfterBothBranchs() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data2,  1, 7, 7, value1);
        map.setValue(data3,  1, 7, 7, value2);
        map.setValue(data2b, 1, 7, 7, value3);
        map.setValue(data3b, 1, 7, 7, value4);
                
        assertEquals(value1, reader.query(map,data2,  1, 7, 7));
        assertEquals(value2, reader.query(map,data3,  1, 7, 7));
        assertEquals(value3, reader.query(map,data2b, 1, 7, 7));
        assertEquals(value4, reader.query(map,data3b, 1, 7, 7));
        
        //swap values
        map.setValue(data3b, 1, 7, 7, value1);
        map.setValue(data2b, 1, 7, 7, value2);
        map.setValue(data3,  1, 7, 7, value3);
        map.setValue(data2,  1, 7, 7, value4);
        
        assertEquals(value4, reader.query(map,data2,  1, 7, 7));
        assertEquals(value3, reader.query(map,data3,  1, 7, 7));
        assertEquals(value2, reader.query(map,data2b, 1, 7, 7));
        assertEquals(value1, reader.query(map,data3b, 1, 7, 7));       
        
    }

    
    @Test
    public void testLongInsertThenShortRootInsert() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data1, 0, 8, 7, value1);
        
      //  System.out.println(map);
       
        map.setValue(data1, 0, 3, 7, value2);
        
      //  System.out.println(map);
        
        assertEquals(value1, reader.query(map,data1, 0, 8, 7));
        assertEquals(value2, reader.query(map,data1, 0, 3, 7));
        
        //swap values
        map.setValue(data1, 0, 8, 7, value2);
        map.setValue(data1, 0, 3, 7, value1);
        
        assertEquals(value2, reader.query(map,data1, 0, 8, 7));
        assertEquals(value1, reader.query(map,data1, 0, 3, 7));        
        
    }

    
    @Test
    public void testShortRootInsertThenLongInsert() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data1, 0, 3, 7, value2);
        
     //   System.err.println(map);
        
        map.setValue(data1, 0, 8, 7, value1);
                
      //  System.err.println(map);
        
        assertEquals(value1, reader.query(map,data1, 0, 8, 7));
        assertEquals(value2, reader.query(map,data1, 0, 3, 7));
        
        //swap values
        map.setValue(data1, 0, 3, 7, value1);
        map.setValue(data1, 0, 8, 7, value2);
        
        assertEquals(value2, reader.query(map,data1, 0, 8, 7));
        assertEquals(value1, reader.query(map,data1, 0, 3, 7));        
        
    }
    
    //add tests for end stopping at the branch point?  double check the coverage
    
    @Test
    public void testToString() {
        
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data1, 0, 3, 7, value2);
        map.setValue(data1, 0, 8, 7, value1);
        
        map.setValue(data2,  1, 7, 7, value1);
        map.setValue(data3,  1, 7, 7, value2);
        map.setValue(data2b, 1, 7, 7, value3);
        map.setValue(data3b, 1, 7, 7, value4); 
        
        String actual = map.toString();
        
        String expected = "BRANCH_VALUE1[0], -248[1], 46[2], \n"+
                "RUN3[3], 3[4], 107[5], 108[6], 109[7], \n"+
                "BRANCH_VALUE1[8], -240[9], 19[10], \n"+
                "RUN3[11], 2[12], 120[13], 121[14], \n"+
                "BRANCH_VALUE1[15], -128[16], 6[17], \n"+
                "RUN3[18], 2[19], -128[20], -127[21], \n"+
                "END7[22], 57[23], \n"+
                "RUN3[24], 2[25], 122[26], 123[27], \n"+
                "END7[28], 23[29], \n"+
                "RUN3[30], 2[31], 110[32], 111[33], \n"+
                "BRANCH_VALUE1[34], -252[35], 6[36], \n"+
                "RUN3[37], 2[38], 118[39], 119[40], \n"+
                "END7[41], 41[42], \n"+
                "RUN3[43], 2[44], 112[45], 113[46], \n"+
                "END7[47], 10[48], \n"+
                "RUN3[49], 3[50], 101[51], 102[52], 103[53], \n"+
                "SAFE6[54], 23[55], \n"+
                "RUN3[56], 5[57], 104[58], 105[59], 106[60], 107[61], 108[62], \n"+
                "END7[63], 10[64], \n";
        
        if (!expected.equals(actual)) {
            System.out.println("String expected = \""+(actual.replace("\n", "\\n\"+\n\"")));
        }
        
        assertEquals(expected,actual);
        
        
        int actualLimit = map.getLimit();
        assertEquals(65, actualLimit);
        
    }
    
    
    
    public static void main(String[] args) {
        speedReadTest();
    }
    
    public static void speedReadTest() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        
        //Different values give very different results, for most small sets of URLS however it does look like the trie will be almost 2x faster than the hash.
        short testSize        = 21;//700;
        int baseSeqLen        = 40;//10;
        int maxSeqLenFromBase = 30;//180;
        int iterations        = 2000000;
        
        int[] testPos = new int[testSize];
        int[] testLen = new int[testSize];
        byte[] testData = buildTestData(testSize, baseSeqLen, maxSeqLenFromBase,testPos,testLen);
        
        //Build up the ByteSequenceMap
        int maxSize = 5*testSize*(baseSeqLen+maxSeqLenFromBase);
        SequentialTrieParser bsm = new SequentialTrieParser(maxSize);
        int i;
        
        i = testSize;
        int expectedSum = 0;
        while (--i >= 0) {
            System.out.println("ADD:"+Arrays.toString(Arrays.copyOfRange(testData,testPos[i],testPos[i]+testLen[i])));
            
            bsm.setValue(testData, testPos[i], testLen[i], 0x7FFF_FFFF, i);
            expectedSum += i;
            
//            System.out.println("");
//            System.out.println("");
//            System.out.println(bsm);
            
            int result = reader.query(bsm,testData, testPos[i], testLen[i], 0x7FFF_FFFF);
            if (i!=result) {
                System.err.println("unable to build expected "+i+" but got "+result);
                System.exit(0);
            }
        }
        System.out.println("done building trie limit:"+bsm.getLimit()+" max:"+maxSize);
        
        //Build up the classic Map
        Map<KeyBytesData, Integer> map = new HashMap<KeyBytesData, Integer>();
        i = testSize;
        while (--i >= 0) {
            map.put(new KeyBytesData(testData,testPos[i],testLen[i]), new Integer(i));
        }
        
        System.out.println("done with setup now run the test.");
        //ready for the read test.
                
        
        int j;
        
        System.out.println("exp:"+(expectedSum*iterations));
        
        long startTrie = System.currentTimeMillis();
        int sumTotalTrie = 0;
        j = iterations;
        while (--j>=0) {
            i = testSize;
            while (--i >= 0) {
                sumTotalTrie += reader.query(bsm,testData,testPos[i],testLen[i],0x7FFF_FFFF);
            }
        }
        long durationTrie = System.currentTimeMillis()-startTrie;
        System.out.println("Trie duration "+durationTrie);//+" sum "+sumTotalTrie);
        
        
        long startMap = System.currentTimeMillis();
        int sumTotalMap = 0;
        j = iterations;
        while (--j>=0) {
            i = testSize;
            while (--i >= 0) {
                sumTotalMap += map.get(new KeyBytesData(testData,testPos[i],testLen[i]));
            }
        }
        long durationMap = System.currentTimeMillis()-startMap;
        System.out.println("Map duration "+durationMap);//+" sum "+sumTotalMap);
       
       // System.out.println(testData);
        
        System.out.println("sum:"+sumTotalTrie);
        System.out.println("sum:"+sumTotalMap);
        
    }

    //test data looks similar to what we will find on the pipes
    private static byte[] buildTestData(int testSize, int baseSeqLen, int maxSeqLenFromBase, int[] targetPos, int[] targetLength) {
        byte[] testData = new byte[testSize*(baseSeqLen+maxSeqLenFromBase)];
        
        Random r = new Random(42);
        
        int runningPos = 0;
        int lastPos = 0;
        int lastLength = 0;
        
        for(int i = 0; i<testSize; i++) {
            
            int activePos = 0;
            int activeLength = baseSeqLen+((i*maxSeqLenFromBase) / testSize);
            
            if (lastPos>0) {
                int keep = lastLength/32;
                int copyCount =  keep+  r.nextInt(lastLength-keep);
                if (copyCount>0) {
                    System.arraycopy(testData, lastPos, testData, runningPos+activePos, copyCount);
                    activePos += copyCount;
                }
            }
            while (activePos<activeLength) {
                testData[runningPos + activePos++] = (byte)r.nextInt(125);
            }
           
            lastPos = runningPos;
            lastLength = activeLength;
                        
            targetPos[i] = lastPos;
            targetLength[i] = lastLength;
            
            runningPos+=activeLength;            
            
        }
        System.out.println("Total bytes of test data "+runningPos);
        return testData;
    }
    
    
}



