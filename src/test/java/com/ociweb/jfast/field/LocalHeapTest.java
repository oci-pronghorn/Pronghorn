//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.Test;

public class LocalHeapTest {

	
	@AfterClass
	public static void cleanup() {
		System.gc();
	}
	
	@Test
	public void simpleWriteReadTest() {
		
		String[] testData = new String[] {
			"a","b","c","hello","world"	
		};
		
		LocalHeap th = new LocalHeap(6,6, testData.length);
		
		int i = testData.length;
		while (--i>=0) {
			byte[] data = testData[i].getBytes();
			LocalHeap.set(i,data,0,data.length,0xFFFFFFFF,th);
		}
		
		localHeapMatchesTestData(testData, th);				
	}

    private void localHeapMatchesTestData(String[] testData, LocalHeap th) {
        int i;
        i = testData.length;
		while (--i>=0) {
		    int len = LocalHeap.length(i, th);
		    assertEquals(testData[i].length(),len); //only valid for ASCII		    
		    byte[] target = new byte[len];	    
		    LocalHeap.copyToRingBuffer(i, target, 0, 0xFFFFFFFF, th);		    
		    //Don't trust equals because that is part of what we are testing
		    assertTrue(LocalHeap.equals(i,testData[i].getBytes(),0,len,0xFFFFFFFF,th));	   
			assertTrue(Arrays.equals(testData[i].getBytes(), target));
		}
    }
	
	
	@Test
	public void simpleReplacementTest() {
		
		String[] testData = new String[] {
			"a","b","c","hello","world","string"	
		};
		
		LocalHeap th = new LocalHeap(6,6, testData.length);
		
		//write each test value into the heap
		int i = testData.length;
		while (--i>=0) {
			byte[] data = testData[i].getBytes();
			LocalHeap.set(testData.length-(i+1),data,0,data.length,0xFFFFFFFF,th);
		}
		//now replace each backwards so they have something shorter or longer
		i = testData.length;
		while (--i>=0) {
			byte[] data = testData[i].getBytes();
			LocalHeap.set(i,data,0,data.length,0xFFFFFFFF,th);
		}
				
		localHeapMatchesTestData(testData, th); 
		
	}
	
	@Test
	public void simpleReplacementStringTest() {
		
		String[] testData = new String[] {
			"a","b","c","hello","world","string"	
		};
		
		LocalHeap th = new LocalHeap(6,6, testData.length);
		
		//write each test value into the heap
		int i = testData.length;
		while (--i>=0) {
			LocalHeap.set(testData.length-(i+1),testData[i].getBytes(),0,testData[i].length(),0xFFFFFFFF,th);
		}
		//now replace each backwards so they have something shorter or longer
		i = testData.length;
		while (--i>=0) {
			LocalHeap.set(i,testData[i].getBytes(),0,testData[i].length(),0xFFFFFFFF,th);
		}
		
		
		localHeapMatchesTestData(testData, th); 
		
	}
	
	@Test
	public void modifyTailTest() {
		
		LocalHeap th = new LocalHeap(8,8, 10);
		String root = "hello";
		String suffix = "world";
		String replace = "everyone";
		
		byte[] data = (root+suffix).getBytes();
		LocalHeap.set(2,data,0,data.length,0xFFFFFFFF,th);
		//
		StringBuilder builder = new StringBuilder();
		LocalHeapTest.get(2,builder,th);
		assertEquals(root+suffix,builder.toString());
		//
		byte[] tail = replace.getBytes();
		LocalHeap.appendTail(2,suffix.length(),tail,0,tail.length,0xFFFFFFFF,th);
		//
		builder.setLength(0);
		LocalHeapTest.get(2,builder,th);
		assertEquals(root+replace,builder.toString());
		String temp = root+replace;
		assertTrue(LocalHeap.equals(2,temp.getBytes(),0,temp.length(),0xFFFFFFFF,th));
		assertEquals(root.length(), LocalHeap.countHeadMatch(2,root.getBytes(),0,root.length(),0xFFFFFFFF,th));
		assertEquals(replace.length(), LocalHeap.countTailMatch(2,replace.getBytes(),replace.length(),replace.length(),0xFFFFFFFF,th));
	}
	
	@Test
	public void modifyHeadTestString() {
		
		LocalHeap th = new LocalHeap(8,8, 10);
		String prefix = "hello";
		String replace = "goodbye";
		String root = "world";
		
		byte[] data = (prefix+root).getBytes();
		LocalHeap.set(2,data,0,data.length,0xFFFFFFFF,th);
		//
		StringBuilder builder = new StringBuilder();
		LocalHeapTest.get(2,builder,th);
		assertEquals(prefix+root,builder.toString());
		//
		byte[] tail = replace.getBytes();
		LocalHeap.appendHead(2,prefix.length(),tail,0,tail.length,0xFFFFFFFF,th);
		//
		builder.setLength(0);
		LocalHeapTest.get(2,builder,th);
		assertEquals(replace+root,builder.toString());
		String temp = replace+root;
		assertTrue(LocalHeap.equals(2,temp.getBytes(),0,temp.length(),0xFFFFFFFF,th));
		assertFalse(LocalHeap.equals(2,replace.getBytes(),0,replace.length(),0xFFFFFFFF,th));
		assertEquals(replace.length(), LocalHeap.countHeadMatch(2,replace.getBytes(),0,replace.length(),0xFFFFFFFF,th));
		assertEquals(root.length(), LocalHeap.countTailMatch(2,root.getBytes(),root.length(),root.length(),0xFFFFFFFF,th));
	}
	
	@Test
	public void modifyHeadTestChar() {
		
		LocalHeap th = new LocalHeap(8, 8, 10);
		String prefix = "hello";
		String replace = "goodbye";
		String root = "world";
		
		byte[] data = (prefix+root).getBytes();
		LocalHeap.set(2,data,0,data.length,0xFFFFFFFF,th);
		//
		StringBuilder builder = new StringBuilder();
		LocalHeapTest.get(2,builder,th);
		assertEquals(prefix+root,builder.toString());
		//
		LocalHeap.appendHead(2,prefix.length(),replace.getBytes(),0,replace.length(),0xFFFFFFFF,th);
		//
		builder.setLength(0);
		LocalHeapTest.get(2,builder,th);
		assertEquals(replace+root,builder.toString());
		String temp = replace+root;
		assertTrue(LocalHeap.equals(2,temp.getBytes(),0,temp.length(),0xFFFFFFFF,th));
		assertFalse(LocalHeap.equals(2,replace.getBytes(),0,replace.length(),0xFFFFFFFF,th));
		assertEquals(replace.length(), LocalHeap.countHeadMatch(2,replace.getBytes(),0,replace.length(),0xFFFFFFFF,th));
		assertEquals(root.length(), LocalHeap.countTailMatch(2,root.getBytes(),root.length(),root.length(),0xFFFFFFFF,th));
	}
	
	@Test
	public void modifyHeadTestSingleChar() {
		
		LocalHeap th = new LocalHeap(8,8, 10);
		String prefix = "hello";
		String replace = "g";
		String root = "world";
		
		byte[] data = (prefix+root).getBytes();
		LocalHeap.set(2,data,0,data.length,0xFFFFFFFF,th);
		//
		StringBuilder builder = new StringBuilder();
		LocalHeapTest.get(2,builder,th);
		assertEquals(prefix+root,builder.toString());
		//
		th.trimHead(2, prefix.length());
		LocalHeap.appendHead(2,(byte)replace.charAt(0),th);
		//
		builder.setLength(0);
		LocalHeapTest.get(2,builder,th);
		assertEquals(replace+root,builder.toString());
		String temp = replace+root;
		assertTrue(LocalHeap.equals(2,temp.getBytes(),0,temp.length(),0xFFFFFFFF,th));
		assertFalse(LocalHeap.equals(2,replace.getBytes(),0,replace.length(),0xFFFFFFFF,th));
		assertEquals(replace.length(), LocalHeap.countHeadMatch(2,replace.getBytes(),0,replace.length(),0xFFFFFFFF,th));
		assertEquals(root.length(), LocalHeap.countTailMatch(2,root.getBytes(),root.length(),root.length(),0xFFFFFFFF,th));
	}
	
	private static final String[] buildTestData = new String[] {"abcd","efgh","ijkl","mnop"};
	private LocalHeap buildTestHeap() {
		//each string is 4 bytes with 4 bytes between each
		LocalHeap th = new LocalHeap(4, 4, 4);
		
		int i = buildTestData.length;
		while (--i>=0) {
			byte[] temp = buildTestData[i].getBytes();
			LocalHeap.set(i,temp,0,temp.length,0xFFFFFFFF,th);
		}
		// test the data
		localHeapMatchesTestData(buildTestData, th); 
		return th;
	}
	
	@Test
	public void replaceAndMoveRightTest() {
		//Need to test move right and move left.
		LocalHeap th = buildTestHeap();
		//
		String bigString = "abcdefghijkl";
		byte[] bigData = bigString.getBytes();
		
		LocalHeap.set(0,bigData,0,bigData.length,0xFFFFFFFF,th);
		
		StringBuilder builder = new StringBuilder();
		builder.setLength(0);
		LocalHeapTest.get(0,builder,th);
		assertEquals(bigString,builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(1,builder,th);
		assertEquals(buildTestData[1],builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(2,builder,th);
		assertEquals(buildTestData[2],builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(3,builder,th);
		assertEquals(buildTestData[3],builder.toString());
	}
	
	@Test
	public void replaceAndMoveLeftTest() {
		//Need to test move right and move left.
		LocalHeap th = buildTestHeap();
		//
		String bigString = "abcdefghijkl";
		byte[] bigData = bigString.getBytes();
		
		LocalHeap.set(3,bigData,0,bigData.length,0xFFFFFFFF,th);
		
		StringBuilder builder = new StringBuilder();
		builder.setLength(0);
		LocalHeapTest.get(3,builder,th);
		assertEquals(bigString,builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(2,builder,th);
		assertEquals(buildTestData[2],builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(1,builder,th);
		assertEquals(buildTestData[1],builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(0,builder,th);
		assertEquals(buildTestData[0],builder.toString());
	}
	
	@Test
	public void replaceMiddleAndMoveBothTest() {
		//Need to test move right and move left.
		LocalHeap th = buildTestHeap();
		//
		String bigString = "abcdefghijklmo";
		byte[] bigData = bigString.getBytes();
		
		LocalHeap.set(1,bigData,0,bigData.length,0xFFFFFFFF,th);
		
		StringBuilder builder = new StringBuilder();
		builder.setLength(0);
		LocalHeapTest.get(1,builder,th);
		assertEquals(bigString,builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(0,builder,th);
		assertEquals(buildTestData[0],builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(2,builder,th);
		assertEquals(buildTestData[2],builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(3,builder,th);
		assertEquals(buildTestData[3],builder.toString());
	}
	
	@Test
	public void tailAndMoveRightTest() {
		//Need to test move right and move left.
		LocalHeap th = buildTestHeap();
		//
		String tail = "qrstuv";
		byte[] tailData = tail.getBytes();
		
		LocalHeap.appendTail(0,1,tailData,0,tailData.length,0xFFFFFFFF,th);
				
		StringBuilder builder = new StringBuilder();
		builder.setLength(0);
		LocalHeapTest.get(0,builder,th);
		assertEquals("abc"+tail,builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(1,builder,th);
		assertEquals(buildTestData[1],builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(2,builder,th);
		assertEquals(buildTestData[2],builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(3,builder,th);
		assertEquals(buildTestData[3],builder.toString());
	}
	
	@Test
	public void headAndMoveLeftTestChar() {
		//Need to test move right and move left.
		LocalHeap th = buildTestHeap();
		//
		String headString = "abcdef";
		byte[] headData = headString.getBytes();
		
		LocalHeap.appendHead(3,1,headData,0,headData.length,0xFFFFFFFF,th);
				
		StringBuilder builder = new StringBuilder();
		builder.setLength(0);
		LocalHeapTest.get(3,builder,th);
		assertEquals(headString+"nop",builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(2,builder,th);
		assertEquals(buildTestData[2],builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(1,builder,th);
		assertEquals(buildTestData[1],builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(0,builder,th);
		assertEquals(buildTestData[0],builder.toString());
	}
	
	@Test
	public void headAndMoveLeftTestString() {
		//Need to test move right and move left.
		LocalHeap th = buildTestHeap();
		//
		String headString = "abcdef";
		
		LocalHeap.appendHead(3,1,headString.getBytes(),0,headString.length(),0xFFFFFFFF,th);
				
		StringBuilder builder = new StringBuilder();
		builder.setLength(0);
		LocalHeapTest.get(3,builder,th);
		assertEquals(headString+"nop",builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(2,builder,th);
		assertEquals(buildTestData[2],builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(1,builder,th);
		assertEquals(buildTestData[1],builder.toString());
		
		builder.setLength(0);
		LocalHeapTest.get(0,builder,th);
		assertEquals(buildTestData[0],builder.toString());
	}
	//private static final String[] buildTestData = new String[] {"abcd","efgh","ijkl","mnop"};
	
	
	@Test
	public void fullTextTest() {
		
		String[] testData = new String[] {
			"abc","def","ghi","jkl","mno"	
		};
		
		LocalHeap th = new LocalHeap(3, 0, testData.length);
		
		int i = testData.length;
		while (--i>=0) {
			byte[] data = testData[i].getBytes();
			LocalHeap.set(i,data,0,data.length,0xFFFFFFFF,th);
		}
		
		localHeapMatchesTestData(testData, th); 
		
		try {
			byte[] biggerString = "abcd".getBytes();
			LocalHeap.set(0,biggerString,0,biggerString.length,0xFFFFFFFF,th);
			fail("expected exception no more room in heap.");
		} catch (Throwable t) {
			//expected
		}
		
		
	}
	
	@Test
	public void fullTextWithOneSpotOnRightTest() {
		
		String[] testData = new String[] {
			"abc","def","ghi","jkl","mn"	
		};
		
		LocalHeap th = new LocalHeap(3, 0, testData.length);
		
		int i = testData.length;
		while (--i>=0) {
			byte[] data = testData[i].getBytes();
			LocalHeap.set(i,data,0,data.length,0xFFFFFFFF,th);
		}
		
		localHeapMatchesTestData(testData, th); 
		
		//should be room enough for this.
		byte[] biggerString = "abcd".getBytes();
		LocalHeap.set(0,biggerString,0,biggerString.length,0xFFFFFFFF,th);	
		
	}
	
	@Test
	public void fullTextWithOneSpotOnLeftTest() {
		
		String[] testData = new String[] {
			"bc","def","ghi","jkl","mno"	
		};
		
		LocalHeap th = new LocalHeap(3, 0, testData.length);
		
		int i = testData.length;
		while (--i>=0) {
			byte[] data = testData[i].getBytes();
			LocalHeap.set(i,data,0,data.length,0xFFFFFFFF,th);
		}
		
		localHeapMatchesTestData(testData, th); 
		
		//should be room enough for this.
		byte[] biggerString = "abcd".getBytes();
		LocalHeap.set(4,biggerString,0,biggerString.length,0xFFFFFFFF,th);

	}

    public static int get(int idx, Appendable target, LocalHeap heap) {
        
        int textLen = (idx < 0 ? LocalHeap.initLength(idx, heap) : LocalHeap.valueLength(idx, heap));
        int i = 0;
        while (i<textLen) {
            try {
                target.append((char)LocalHeap.byteAt(idx, i++, heap));
            } catch (IOException e) {
                e.printStackTrace();
            }            
        }
        return textLen;
    
    }
	
}
