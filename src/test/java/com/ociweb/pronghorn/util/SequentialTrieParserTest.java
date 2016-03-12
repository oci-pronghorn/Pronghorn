package com.ociweb.pronghorn.util;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
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


    byte[] dataBytesExtractEnd = new byte[]{100,101,102,'%','b',127};
    byte[] dataBytesExtractMiddle = new byte[]{100,101,'%','b',127, 102};
    byte[] dataBytesExtractBeginning = new byte[]{'%','b',127,100,101,102,};

    byte[] toParseEnd       = new byte[]{100,101,102,10,11,12,13,127};
    byte[] toParseMiddle    = new byte[]{100,101,10,11,12,13,127,102};
    byte[] toParseBeginning = new byte[]{10,11,12,13,127,100,101,102};
    
    @Test 
    public void testExtractBytesEnd() {
        SequentialTrieParserReader reader = new SequentialTrieParserReader(3);
        SequentialTrieParser map = new SequentialTrieParser(1000);
        
        map.setValue(data1, 0, 3, 7, value1);
        
        map.setValue(dataBytesExtractEnd, 0, dataBytesExtractEnd.length, 7, value2);
        
        map.setValue(data1, 2, 3, 7, value3);
        
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data1, 0, 3, 7));
        assertEquals(value3, SequentialTrieParserReader.query(reader,map,data1, 2, 3, 7));
        
        
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,toParseEnd, 0, toParseEnd.length, 7));
                
        assertEquals(1, SequentialTrieParserReader.capturedFieldCount(reader));
                
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream(100);
        DataOutput out = new DataOutputStream(byteArray);
        SequentialTrieParserReader.capturedFieldBytes(reader, 0, out);
        assertEquals(Arrays.toString(new byte[]{10,11,12,13}),Arrays.toString(byteArray.toByteArray()) );
   
    }
    
    @Test 
    public void testExtractBytesMiddle() {
        SequentialTrieParserReader reader = new SequentialTrieParserReader(3);
        SequentialTrieParser map = new SequentialTrieParser(1000);
        
        map.setValue(data1, 0, 3, Integer.MAX_VALUE, value1);
        //System.out.println(map);
        
        map.setValue(dataBytesExtractMiddle, 0, dataBytesExtractMiddle.length, Integer.MAX_VALUE, value2);
        //System.out.println(map);
        
        map.setValue(data1, 2, 3, Integer.MAX_VALUE, value3);
        //System.out.println(map);
        
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data1, 0, 3, Integer.MAX_VALUE));
        assertEquals(value3, SequentialTrieParserReader.query(reader,map,data1, 2, 3, Integer.MAX_VALUE));
        
        
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,toParseMiddle, 0, toParseMiddle.length, Integer.MAX_VALUE));
                
        assertEquals(1, SequentialTrieParserReader.capturedFieldCount(reader));
                
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream(100);
        DataOutput out = new DataOutputStream(byteArray);
        SequentialTrieParserReader.capturedFieldBytes(reader, 0, out);
        assertEquals(Arrays.toString(new byte[]{10,11,12,13}),Arrays.toString(byteArray.toByteArray()) );
        
    }
    
    //TOOD: think about later as to how.
    //  abcdefg
    //  abc%bxfg
    //second one will modify the other 
    
    
//    @Test //TODO: fix this test.
//    public void testExtractBytesBeginning() {
//        SequentialTrieParserReader reader = new SequentialTrieParserReader(3);
//        SequentialTrieParser map = new SequentialTrieParser(1000);
//        
//        map.setValue(data1, 0, 3, 7, value1);
//        
//        map.setValue(dataBytesExtractBeginning, 0, dataBytesExtractBeginning.length, 7, value2);
//        
//        map.setValue(data1, 2, 3, 7, value3);
//        
//        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data1, 0, 3, 7));
//        assertEquals(value3, SequentialTrieParserReader.query(reader,map,data1, 2, 3, 7));
//        
//        
//        assertEquals(value2, SequentialTrieParserReader.query(reader,map,toParseBeginning, 0, toParseBeginning.length, 7));
//                
//        assertEquals(1, SequentialTrieParserReader.capturedFieldCount(reader));
//                
//        ByteArrayOutputStream byteArray = new ByteArrayOutputStream(100);
//        DataOutput out = new DataOutputStream(byteArray);
//        SequentialTrieParserReader.capturedFieldBytes(reader, 0, out);
//        assertEquals(Arrays.toString(new byte[]{10,11,12,13}),Arrays.toString(byteArray.toByteArray()) );
//        
//    }
    
    
    @Test
    public void testSimpleValueReplace() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);
                
        map.setValue(data1, 0, 3, 7, value1);        
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data1, 0, 3, 7));
        
        map.setValue(data1, 0, 3, 7, value2);        
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data1, 0, 3, 7));
                
    }
    
    @Test
    public void testSimpleValueReplaceWrapping() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);
        
        
        map.setValue(data1, 5, 5, 7, value1);        
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data1, 5, 5, 7));
        
        map.setValue(data1, 5, 5, 7, value2);        
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data1, 5, 5, 7));
                
    }
    
    @Test
    public void testTwoNonOverlapValuesWithReplace() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data1, 1, 3, 7, value1);
        map.setValue(data2, 1, 3, 7, value2);
                        
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data1, 1, 3, 7));
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data2, 1, 3, 7));
        
        //swap values
        map.setValue(data1, 1, 3, 7, value2);
        map.setValue(data2, 1, 3, 7, value1);
        
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data1, 1, 3, 7));
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data2, 1, 3, 7));        
        
    }
    
    @Test
    public void testTwoNonOverlapValuesWrappingWithReplace() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data1, 5, 5, 7, value1);
        map.setValue(data2, 5, 5, 7, value2);
         
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data1, 5, 5, 7));
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data2, 5, 5, 7));
        
        //swap values
        map.setValue(data1, 5, 5, 7, value2);
        map.setValue(data2, 5, 5, 7, value1);
        
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data1, 5, 5, 7));
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data2, 5, 5, 7));        
        
    }
    
    
    @Test
    public void testTwoOverlapValues() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data2, 2, 5, 7, value1);
        map.setValue(data3, 2, 5, 7, value2);
                        
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data2, 2, 5, 7));
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data3, 2, 5, 7));
        
        //swap values
        map.setValue(data2, 2, 5, 7, value2);
        map.setValue(data3, 2, 5, 7, value1);
        
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data2, 2, 5, 7));
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data3, 2, 5, 7));        
        
    }
    
    @Test
    public void testThreeOverlapValues() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data3, 2, 5, 7, value2);
        map.setValue(data4, 2, 5, 7, value3);
        map.setValue(data2, 2, 5, 7, value1);
                
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data2, 2, 5, 7));
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data3, 2, 5, 7));
        assertEquals(value3, SequentialTrieParserReader.query(reader,map,data4, 2, 5, 7));
        
        //swap values
        map.setValue(data2, 2, 5, 7, value3);
        map.setValue(data3, 2, 5, 7, value2);
        map.setValue(data4, 2, 5, 7, value1);
        
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data4, 2, 5, 7));
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data3, 2, 5, 7));
        assertEquals(value3, SequentialTrieParserReader.query(reader,map,data2, 2, 5, 7));        
        
    }
    
    @Test
    public void testInsertBeforeBranch() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data3, 0, 6, 7, value1);
        map.setValue(data4, 0, 6, 7, value2);
        map.setValue(data5, 0, 6, 7, value3);

        
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data3, 0, 6, 7));
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data4, 0, 6, 7));
        assertEquals(value3, SequentialTrieParserReader.query(reader,map,data5, 0, 6, 7));
        
        //swap values
        map.setValue(data3, 0, 6, 7, value3);
        map.setValue(data4, 0, 6, 7, value2);
        map.setValue(data5, 0, 6, 7, value1);
        
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data5, 0, 6, 7));
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data4, 0, 6, 7));
        assertEquals(value3, SequentialTrieParserReader.query(reader,map,data3, 0, 6, 7));        
        
    }
    
    @Test
    public void testInsertAfterBothBranchs() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data2,  1, 7, 7, value1);
        map.setValue(data3,  1, 7, 7, value2);
        map.setValue(data2b, 1, 7, 7, value3);
        map.setValue(data3b, 1, 7, 7, value4);
                
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data2,  1, 7, 7));
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data3,  1, 7, 7));
        assertEquals(value3, SequentialTrieParserReader.query(reader,map,data2b, 1, 7, 7));
        assertEquals(value4, SequentialTrieParserReader.query(reader,map,data3b, 1, 7, 7));
        
        //swap values
        map.setValue(data3b, 1, 7, 7, value1);
        map.setValue(data2b, 1, 7, 7, value2);
        map.setValue(data3,  1, 7, 7, value3);
        map.setValue(data2,  1, 7, 7, value4);
        
        assertEquals(value4, SequentialTrieParserReader.query(reader,map,data2,  1, 7, 7));
        assertEquals(value3, SequentialTrieParserReader.query(reader,map,data3,  1, 7, 7));
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data2b, 1, 7, 7));
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data3b, 1, 7, 7));       
        
    }

    
    @Test
    public void testLongInsertThenShortRootInsert() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data1, 0, 8, 7, value1);
        
      //  System.out.println(map);
       
        map.setValue(data1, 0, 3, 7, value2);
        
      //  System.out.println(map);
        
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data1, 0, 8, 7));
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data1, 0, 3, 7));
        
        //swap values
        map.setValue(data1, 0, 8, 7, value2);
        map.setValue(data1, 0, 3, 7, value1);
        
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data1, 0, 8, 7));
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data1, 0, 3, 7));        
        
    }

    
    @Test
    public void testShortRootInsertThenLongInsert() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader();
        SequentialTrieParser map = new SequentialTrieParser(1000);        
        
        map.setValue(data1, 0, 3, 7, value2);
        
     //   System.err.println(map);
        
        map.setValue(data1, 0, 8, 7, value1);
                
      //  System.err.println(map);
        
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data1, 0, 8, 7));
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data1, 0, 3, 7));
        
        //swap values
        map.setValue(data1, 0, 3, 7, value1);
        map.setValue(data1, 0, 8, 7, value2);
        
        assertEquals(value2, SequentialTrieParserReader.query(reader,map,data1, 0, 8, 7));
        assertEquals(value1, SequentialTrieParserReader.query(reader,map,data1, 0, 3, 7));        
        
    }
    
    //add tests for end stopping at the branch point?  double check the coverage
    
    @Test
    public void testByteExtractExample() {
        
        SequentialTrieParserReader reader = new SequentialTrieParserReader(10);
        SequentialTrieParser map = new SequentialTrieParser(1000);  
        
        byte[] b1 = "X-Wap-Profile:%b\n".getBytes();
        byte[] b2 = "X-ATT-DeviceId:%b\n".getBytes();
        byte[] b3 = "Front-End-Https:%b\n".getBytes();
        byte[] b4 = "X-Online-Host:%b\n".getBytes();
        byte[] b5 = "X-Forwarded-Host:%b\n".getBytes();
        byte[] b6 = "X-Forwarded-For:%b\n".getBytes();
        
        map.setValue(b1, 0, b1.length, Integer.MAX_VALUE, 1);
        //System.out.println(map);
        
        map.setValue(b2, 0, b2.length, Integer.MAX_VALUE, 2);
        //System.out.println(map);
        
        map.setValue(b3, 0, b3.length, Integer.MAX_VALUE, 3);
        //System.out.println(map);
        
        map.setValue(b4, 0, b4.length, Integer.MAX_VALUE, 4); 
        //System.out.println(map);
        
        map.setValue(b5, 0, b5.length, Integer.MAX_VALUE, 5);
        //System.out.println(map);
        
        map.setValue(b6, 0, b6.length, Integer.MAX_VALUE, 6);
        //System.out.println(map);
       
        byte[] example = "X-Wap-Profile:ABCD\nHello".getBytes();
        assertEquals(1, SequentialTrieParserReader.query(reader,  map, example, 0, example.length, Integer.MAX_VALUE));
        
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream(100);
        DataOutput out = new DataOutputStream(byteArray);
        SequentialTrieParserReader.capturedFieldBytes(reader, 0, out);
        
        byte[] results = byteArray.toByteArray();
        assertTrue(Arrays.toString(results), Arrays.equals("ABCD".getBytes(), results));
                
        
    }
    
    
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
                "RUN0[3], 3[4], 107'k'[5], 108'l'[6], 109'm'[7], \n"+
                "BRANCH_VALUE1[8], -240[9], 19[10], \n"+
                "RUN0[11], 2[12], 120'x'[13], 121'y'[14], \n"+
                "BRANCH_VALUE1[15], -128[16], 6[17], \n"+
                "RUN0[18], 2[19], -128[20], -127[21], \n"+
                "END7[22], 57[23], \n"+
                "RUN0[24], 2[25], 122'z'[26], 123'{'[27], \n"+
                "END7[28], 23[29], \n"+
                "RUN0[30], 2[31], 110'n'[32], 111'o'[33], \n"+
                "BRANCH_VALUE1[34], -252[35], 6[36], \n"+
                "RUN0[37], 2[38], 118'v'[39], 119'w'[40], \n"+
                "END7[41], 41[42], \n"+
                "RUN0[43], 2[44], 112'p'[45], 113'q'[46], \n"+
                "END7[47], 10[48], \n"+
                "RUN0[49], 3[50], 101'e'[51], 102'f'[52], 103'g'[53], \n"+
                "SAFE6[54], 23[55], \n"+
                "RUN0[56], 5[57], 104'h'[58], 105'i'[59], 106'j'[60], 107'k'[61], 108'l'[62], \n"+
                "END7[63], 10[64], \n"+
                "RUN0[65], 0[66], \n";
        
        if (!expected.equals(actual)) {
            System.out.println("String expected = \""+(actual.replace("\n", "\\n\"+\n\"")));
        }
        
        assertEquals(expected,actual);
        
        
        int actualLimit = map.getLimit();
        assertEquals(67, actualLimit);
        
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
             
            int result = SequentialTrieParserReader.query(reader,bsm,testData, testPos[i], testLen[i], 0x7FFF_FFFF);
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
                sumTotalTrie += SequentialTrieParserReader.query(reader,bsm,testData,testPos[i],testLen[i],0x7FFF_FFFF);
            }
        }
        long durationTrie = System.currentTimeMillis()-startTrie;
        long totalLookups = iterations*testSize;
        long lookupsPerMs = totalLookups/durationTrie;
        
        System.out.println("Trie duration "+durationTrie+" Lookups per MS "+lookupsPerMs);
        //header request may be between 200 and 2000 bytes, 800 is common
        
        int avgLookpsPerheader = 16;//this is a big guess
        long perSecond = 1000*lookupsPerMs;
        long headersPerSecond = perSecond/avgLookpsPerheader;
        long bytesPerSecond = 800 * headersPerSecond;
        System.out.println("guess of mbps read "+ (bytesPerSecond/(1024*1024)));
        
        
        
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
                int copyCount =  keep +  r.nextInt(lastLength-keep);
                if (copyCount>0) {
                    System.arraycopy(testData, lastPos, testData, runningPos+activePos, copyCount);
                    activePos += copyCount;
                }
            }
            while (activePos<activeLength) {
                
                byte v = 0;
                do {
                    v = (byte)r.nextInt(125);
                } while ('%' == v);//eliminate the escape byte
                
                testData[runningPos + activePos++] = v;
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



