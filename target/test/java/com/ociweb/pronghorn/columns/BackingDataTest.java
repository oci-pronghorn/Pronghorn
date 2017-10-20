package com.ociweb.pronghorn.columns;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.EnumSet;

import org.junit.Test;

import com.ociweb.pronghorn.util.columns.BackingData;
import com.ociweb.pronghorn.util.columns.FieldsOf16Bits;
import com.ociweb.pronghorn.util.columns.FieldsOf32Bits;
import com.ociweb.pronghorn.util.columns.FieldsOf64Bits;
import com.ociweb.pronghorn.util.columns.FieldsOf8Bits;

public class BackingDataTest {

    enum TestByteFields implements FieldsOf8Bits {
        AByteField;
    }
    
    enum TestShortFields implements FieldsOf16Bits {
        AShortField;
    }

    enum TestIntFields implements FieldsOf32Bits {
        AIntField;
    }

    enum TestLongFields implements FieldsOf64Bits {
        ALongField;
    }
    
    enum TestEnum {
        Item0,
        Item1,
        Item2,
        Item3,
        Item4,
        Item5,
        Item6;
    }
    
    
    
    @Test
    public void emptyDataTest() {
       
       int testSize = 0;
       
       BackingData<?> bd = new BackingData(null, null, null, null, testSize); 
       
       assertEquals(52, bd.memoryConsumed());
       
       try {
           BackingData.setByte(TestByteFields.AByteField, (byte)3, 0, bd);
           fail("should throw ArrayIndexOutOfBounds");
       } catch (ArrayIndexOutOfBoundsException aioobe) {
           //ok
       }
       try {
           BackingData.getByte(TestByteFields.AByteField, 0, bd);
           fail("should throw ArrayIndexOutOfBounds");
       } catch (ArrayIndexOutOfBoundsException aioobe) {
           //ok
       }
       try {
           BackingData.setShort(TestShortFields.AShortField, (short)3, 0, bd);
           fail("should throw ArrayIndexOutOfBounds");
       } catch (ArrayIndexOutOfBoundsException aioobe) {
           //ok
       }
       try {
           BackingData.getShort(TestShortFields.AShortField, 0, bd);
           fail("should throw ArrayIndexOutOfBounds");
       } catch (ArrayIndexOutOfBoundsException aioobe) {
           //ok
       }
       try {
           BackingData.setInt(TestIntFields.AIntField, (int)3, 0, bd);
           fail("should throw ArrayIndexOutOfBounds");
       } catch (ArrayIndexOutOfBoundsException aioobe) {
           //ok
       }
       try {
           BackingData.getInt(TestIntFields.AIntField, 0, bd);
           fail("should throw ArrayIndexOutOfBounds");
       } catch (ArrayIndexOutOfBoundsException aioobe) {
           //ok
       } 
       try {
           BackingData.setLong(TestLongFields.ALongField, (long)3, 0, bd);
           fail("should throw ArrayIndexOutOfBounds");
       } catch (ArrayIndexOutOfBoundsException aioobe) {
           //ok
       }
       try {
           BackingData.getLong(TestLongFields.ALongField, 0, bd);
           fail("should throw ArrayIndexOutOfBounds");
       } catch (ArrayIndexOutOfBoundsException aioobe) {
           //ok
       } 
       
       try {
           BackingData.setEnumBytes(TestByteFields.AByteField, 0, bd, TestEnum.Item1);
           fail("should throw ArrayIndexOutOfBounds");
       } catch (ArrayIndexOutOfBoundsException aioobe) {
           //ok
       }
       try {
           BackingData.getEnumBytes(TestByteFields.AByteField, 0, bd, TestEnum.class);
           fail("should throw ArrayIndexOutOfBounds");
       } catch (ArrayIndexOutOfBoundsException aioobe) {
           //ok
       }  
    }
    
    @Test
    public void singleDataTest() {
       
       int testSize = 1;
       
       BackingData<?> bd = new BackingData(TestLongFields.class, TestIntFields.class, TestShortFields.class, TestByteFields.class, testSize); 
       
       assertEquals(52+15, bd.memoryConsumed()); //15  8+4+2+1
       
       BackingData.setByte(TestByteFields.AByteField, (byte)3, 0, bd);
       assertEquals(3,BackingData.getByte(TestByteFields.AByteField, 0, bd));

       BackingData.setShort(TestShortFields.AShortField, (short)4, 0, bd);
       assertEquals(4,BackingData.getShort(TestShortFields.AShortField, 0, bd));

       BackingData.setInt(TestIntFields.AIntField, (int)5, 0, bd);
       assertEquals(5, BackingData.getInt(TestIntFields.AIntField, 0, bd));

       BackingData.setLong(TestLongFields.ALongField, (long)6, 0, bd);
       assertEquals(6,BackingData.getLong(TestLongFields.ALongField, 0, bd));

       BackingData.setEnumBytes(TestByteFields.AByteField, 0, bd, TestEnum.Item1);
       assertEquals(TestEnum.Item1, BackingData.getEnumBytes(TestByteFields.AByteField, 0, bd, TestEnum.class));
    
    }
    
    @Test
    public void loopDataTest() {
       
       int testSize = 10;
       int loops = 10;
       
       BackingData<?> bd = new BackingData(TestLongFields.class, TestIntFields.class, TestShortFields.class, TestByteFields.class, testSize); 
       
       assertEquals(52+(15*testSize), bd.memoryConsumed()); //15  8+4+2+1
       
       int i = loops;
       while (--i>=0) {
       
           int j = testSize;
           while (--j>=0) {
               if (0==(j&1)) {
                   BackingData.setByte(TestByteFields.AByteField, (byte)(3*i), j, bd);
               } else {
                   BackingData.setEnumBytes(TestByteFields.AByteField, j, bd, TestEnum.Item1);
               }
               BackingData.setShort(TestShortFields.AShortField, (short)(4*i), j, bd);
               BackingData.setInt(TestIntFields.AIntField, (int)(5*i), j, bd);
               BackingData.setLong(TestLongFields.ALongField, (long)(6*i), j, bd);
               
               if (0==(j&1)) {
                   assertEquals(3*i,BackingData.getByte(TestByteFields.AByteField, j, bd));    
               } else {
                   assertEquals(TestEnum.Item1, BackingData.getEnumBytes(TestByteFields.AByteField, j, bd, TestEnum.class));
               }
               assertEquals(4*i,BackingData.getShort(TestShortFields.AShortField, j, bd));    
               assertEquals(5*i, BackingData.getInt(TestIntFields.AIntField, j, bd));    
               assertEquals(6*i,BackingData.getLong(TestLongFields.ALongField, j, bd));    
           }
       }
    
    }
    
    @Test
    public void loopIncDataTest() {
       
       int testSize = 10;
       int loops = 10;
       
       BackingData<?> bd = new BackingData(TestLongFields.class, TestIntFields.class, TestShortFields.class, TestByteFields.class, testSize); 
       
       assertEquals(52+(15*testSize), bd.memoryConsumed()); //15  8+4+2+1
       
       int c =0;
       int i = loops;
       while (--i>=0) {
       
           c++;
           for(int j = 0;j<testSize;j++) {
 
               BackingData.incByte(TestByteFields.AByteField, (byte)(3), j, bd);
               BackingData.incShort(TestShortFields.AShortField, (short)(4), j, bd);
               BackingData.incInt(TestIntFields.AIntField, (int)(5), j, bd);
               BackingData.incLong(TestLongFields.ALongField, (long)(6), j, bd);               
                   
               assertEquals(3*c,BackingData.getByte(TestByteFields.AByteField, j, bd));
               assertEquals(4*c,BackingData.getShort(TestShortFields.AShortField, j, bd));    
               assertEquals(5*c, BackingData.getInt(TestIntFields.AIntField, j, bd));    
               assertEquals(6*c,BackingData.getLong(TestLongFields.ALongField, j, bd));    
           }
       }
    
    }
    
    @Test
    public void loopDecDataTest() {
       
       int testSize = 10;
       int loops = 10;
       
       BackingData<?> bd = new BackingData(TestLongFields.class, TestIntFields.class, TestShortFields.class, TestByteFields.class, testSize); 
       
       assertEquals(52+(15*testSize), bd.memoryConsumed()); //15  8+4+2+1
       
       int c =0;
       int i = loops;
       while (--i>=0) {
       
           c++;
           for(int j = 0;j<testSize;j++) {
 
               BackingData.decByte(TestByteFields.AByteField, (byte)(3), j, bd);
               BackingData.decShort(TestShortFields.AShortField, (short)(4), j, bd);
               BackingData.decInt(TestIntFields.AIntField, (int)(5), j, bd);
               BackingData.decLong(TestLongFields.ALongField, (long)(6), j, bd);               
                   
               assertEquals(-3*c,BackingData.getByte(TestByteFields.AByteField, j, bd));
               assertEquals(-4*c,BackingData.getShort(TestShortFields.AShortField, j, bd));    
               assertEquals(-5*c, BackingData.getInt(TestIntFields.AIntField, j, bd));    
               assertEquals(-6*c,BackingData.getLong(TestLongFields.ALongField, j, bd));    
           }
       }
    
    } 
    
    @Test
    public void enumDataTest() {
       
       int testSize = 10;
       
       BackingData<?> bd = new BackingData(TestLongFields.class, TestIntFields.class, TestShortFields.class, TestByteFields.class, testSize); 
       
       assertEquals(52+(15*testSize), bd.memoryConsumed()); //15  8+4+2+1
       
       BackingData.setEnumSetBytes(TestByteFields.AByteField, 0, bd, TestEnum.Item1);
       BackingData.setEnumSetBytes(TestByteFields.AByteField, 1, bd, TestEnum.Item1, TestEnum.Item2);
       BackingData.setEnumSetBytes(TestByteFields.AByteField, 2, bd, TestEnum.Item1, TestEnum.Item2, TestEnum.Item3);
       BackingData.setEnumSetBytes(TestByteFields.AByteField, 3, bd, TestEnum.Item1, TestEnum.Item2, TestEnum.Item3, TestEnum.Item4);
       BackingData.setEnumSetBytes(TestByteFields.AByteField, 4, bd, TestEnum.Item1, TestEnum.Item2, TestEnum.Item3, TestEnum.Item4, TestEnum.Item5);
       BackingData.setEnumSetBytes(TestByteFields.AByteField, 5, bd, EnumSet.of(TestEnum.Item0, TestEnum.Item2, TestEnum.Item4));
       
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 0, bd, TestEnum.Item1));
       assertFalse(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 0, bd, TestEnum.Item2));
       
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 1, bd, TestEnum.Item1));
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 1, bd, TestEnum.Item2));
       assertFalse(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 1, bd, TestEnum.Item3));
       
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 2, bd, TestEnum.Item1));
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 2, bd, TestEnum.Item2));
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 2, bd, TestEnum.Item3));
       assertFalse(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 2, bd, TestEnum.Item4));      
       
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 3, bd, TestEnum.Item1));
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 3, bd, TestEnum.Item2));
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 3, bd, TestEnum.Item3));
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 3, bd, TestEnum.Item4));
       assertFalse(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 3, bd, TestEnum.Item5));       
       
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 4, bd, TestEnum.Item1));
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 4, bd, TestEnum.Item2));
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 4, bd, TestEnum.Item3));
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 4, bd, TestEnum.Item4));
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 4, bd, TestEnum.Item5));   
       assertFalse(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 4, bd, TestEnum.Item6));     
       
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 5, bd, TestEnum.Item0));
       assertFalse(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 5, bd, TestEnum.Item1));
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 5, bd, TestEnum.Item2));
       assertFalse(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 5, bd, TestEnum.Item3));
       assertTrue(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 5, bd, TestEnum.Item4));   
       assertFalse(BackingData.isEnumBitSetByte(TestByteFields.AByteField, 5, bd, TestEnum.Item5));   
       
       
    }
    
    @Test
    public void writeReadDataTest() {
       
       final int testSize = 10;
       final int loops = 10;
       
       BackingData<?> bd = new BackingData(TestLongFields.class, TestIntFields.class, TestShortFields.class, TestByteFields.class, testSize); 
       
       assertEquals(52+(15*testSize), bd.memoryConsumed()); //15  8+4+2+1
       
       int c =0;
       int i = loops;
       while (--i>=0) {
       
           c++;
           for(int j = 0;j<testSize;j++) {
 
               BackingData.decByte(TestByteFields.AByteField, (byte)(3), j, bd);
               BackingData.decShort(TestShortFields.AShortField, (short)(4), j, bd);
               BackingData.decInt(TestIntFields.AIntField, (int)(5), j, bd);
               BackingData.decLong(TestLongFields.ALongField, (long)(6), j, bd);               
                   
               ByteArrayOutputStream baost = new ByteArrayOutputStream();
               DataOutputStream out = new DataOutputStream(baost); 
               
                try {
                   bd.write(0, testSize, out);
                } catch (IOException e) {
                    e.printStackTrace();
                    fail();
                }
               
                ByteArrayInputStream baist = new ByteArrayInputStream(baost.toByteArray());
                DataInputStream in = new DataInputStream(baist);
                BackingData<?> newbd = new BackingData(TestLongFields.class, TestIntFields.class, TestShortFields.class, TestByteFields.class, testSize); 
                try {
                    newbd.read(0, testSize, in);
                } catch (IOException e) {
                    e.printStackTrace();
                    fail();
                }
               assertEquals(-3*c,BackingData.getByte(TestByteFields.AByteField, j, bd));
               assertEquals(-4*c,BackingData.getShort(TestShortFields.AShortField, j, bd));    
               assertEquals(-5*c, BackingData.getInt(TestIntFields.AIntField, j, bd));    
               assertEquals(-6*c,BackingData.getLong(TestLongFields.ALongField, j, bd));    
           }
       }
    
    } 
    
}
