//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import static org.junit.Assert.*;

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
			th.set(i, data, 0, data.length);
		}
		
		i = testData.length;
		StringBuilder builder = new StringBuilder();
		while (--i>=0) {
			builder.setLength(0);
			th.get(i,builder);
			assertEquals(testData[i],builder.toString());
			assertTrue(th.equals(i, testData[i]));
		}
		
		i = testData.length;
		while (--i>=0) {
			byte[] data = testData[i].getBytes();
			byte[] target = new byte[data.length];
			th.get(i,target, 0);
			assertTrue(Arrays.equals(data,target));
			assertTrue(th.equals(i, testData[i]));
		}
		
	}
	
	@Test
	public void simpleStringWriteReadTest() {
		
		String[] testData = new String[] {
			"a","b","c","hello","world"	
		};
		
		LocalHeap th = new LocalHeap(6,6, testData.length);
		
		int i = testData.length;
		while (--i>=0) {
			CharSequence byteSequence = testData[i];
			th.set(i, byteSequence);
		}
		
		i = testData.length;
		StringBuilder builder = new StringBuilder();
		while (--i>=0) {
			builder.setLength(0);
			th.get(i,builder);
			assertEquals(testData[i],builder.toString());
			assertTrue(th.equals(i, testData[i]));
		}
		
		i = testData.length;
		while (--i>=0) {
			byte[] data = testData[i].getBytes();
			byte[] target = new byte[data.length];
			th.get(i,target, 0);
			assertTrue(Arrays.equals(data,target));
			assertTrue(th.equals(i, testData[i]));
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
			th.set(testData.length-(i+1), data, 0, data.length);
		}
		//now replace each backwards so they have something shorter or longer
		i = testData.length;
		while (--i>=0) {
			byte[] data = testData[i].getBytes();
			th.set(i, data, 0, data.length);
		}
		
		
		i = testData.length;
		StringBuilder builder = new StringBuilder();
		while (--i>=0) {
			builder.setLength(0);
			th.get(i,builder);
			assertEquals(testData[i],builder.toString());
			assertTrue(th.equals(i, testData[i]));
		}
		
		i = testData.length;
		while (--i>=0) {
			byte[] data = testData[i].getBytes();
			byte[] target = new byte[data.length];
			th.get(i,target, 0);
			assertTrue(Arrays.equals(data,target));
			assertTrue(th.equals(i, testData[i]));
		}
		
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
			CharSequence byteSequence = testData[i];
			th.set(testData.length-(i+1), byteSequence);
		}
		//now replace each backwards so they have something shorter or longer
		i = testData.length;
		while (--i>=0) {
			CharSequence byteSequence = testData[i];
			th.set(i, byteSequence);
		}
		
		
		i = testData.length;
		StringBuilder builder = new StringBuilder();
		while (--i>=0) {
			builder.setLength(0);
			th.get(i,builder);
			assertEquals(testData[i],builder.toString());
			assertTrue(th.equals(i, testData[i]));
		}
		
		i = testData.length;
		while (--i>=0) {
			byte[] data = testData[i].getBytes();
			byte[] target = new byte[data.length];
			th.get(i,target, 0);
			assertTrue(Arrays.equals(data,target));
			assertTrue(th.equals(i, testData[i]));
		}
		
	}
	
	@Test
	public void modifyTailTest() {
		
		LocalHeap th = new LocalHeap(8,8, 10);
		String root = "hello";
		String suffix = "world";
		String replace = "everyone";
		
		byte[] data = (root+suffix).getBytes();
		th.set(2, data, 0, data.length);
		//
		StringBuilder builder = new StringBuilder();
		th.get(2, builder);
		assertEquals(root+suffix,builder.toString());
		//
		byte[] tail = replace.getBytes();
		th.appendTail(2, suffix.length(), tail, 0, tail.length);
		//
		builder.setLength(0);
		th.get(2, builder);
		assertEquals(root+replace,builder.toString());
		assertTrue(th.equals(2, root+replace));
		assertEquals(root.length(), th.countHeadMatch(2, root));
		assertEquals(replace.length(), th.countTailMatch(2, replace));
	}
	
	@Test
	public void modifyHeadTestString() {
		
		LocalHeap th = new LocalHeap(8,8, 10);
		String prefix = "hello";
		String replace = "goodbye";
		String root = "world";
		
		byte[] data = (prefix+root).getBytes();
		th.set(2, data, 0, data.length);
		//
		StringBuilder builder = new StringBuilder();
		th.get(2, builder);
		assertEquals(prefix+root,builder.toString());
		//
		byte[] tail = replace.getBytes();
		th.appendHead(2, prefix.length(), tail, 0, tail.length);
		//
		builder.setLength(0);
		th.get(2, builder);
		assertEquals(replace+root,builder.toString());
		assertTrue(th.equals(2, replace+root));
		assertFalse(th.equals(2, replace));
		assertEquals(replace.length(), th.countHeadMatch(2, replace));
		assertEquals(root.length(), th.countTailMatch(2, root));
	}
	
	@Test
	public void modifyHeadTestChar() {
		
		LocalHeap th = new LocalHeap(8, 8, 10);
		String prefix = "hello";
		String replace = "goodbye";
		String root = "world";
		
		byte[] data = (prefix+root).getBytes();
		th.set(2, data, 0, data.length);
		//
		StringBuilder builder = new StringBuilder();
		th.get(2, builder);
		assertEquals(prefix+root,builder.toString());
		//
		th.appendHead(2, prefix.length(), replace, replace.length());
		//
		builder.setLength(0);
		th.get(2, builder);
		assertEquals(replace+root,builder.toString());
		assertTrue(th.equals(2, replace+root));
		assertFalse(th.equals(2, replace));
		assertEquals(replace.length(), th.countHeadMatch(2, replace));
		assertEquals(root.length(), th.countTailMatch(2, root));
	}
	
	@Test
	public void modifyHeadTestSingleChar() {
		
		LocalHeap th = new LocalHeap(8,8, 10);
		String prefix = "hello";
		String replace = "g";
		String root = "world";
		
		byte[] data = (prefix+root).getBytes();
		th.set(2, data, 0, data.length);
		//
		StringBuilder builder = new StringBuilder();
		th.get(2, builder);
		assertEquals(prefix+root,builder.toString());
		//
		th.trimHead(2, prefix.length());
		th.appendHead(2, (byte)replace.charAt(0));
		//
		builder.setLength(0);
		th.get(2, builder);
		assertEquals(replace+root,builder.toString());
		assertTrue(th.equals(2, replace+root));
		assertFalse(th.equals(2, replace));
		assertEquals(replace.length(), th.countHeadMatch(2, replace));
		assertEquals(root.length(), th.countTailMatch(2, root));
	}
	
	private static final String[] buildTestData = new String[] {"abcd","efgh","ijkl","mnop"};
	private LocalHeap buildTestHeap() {
		//each string is 4 bytes with 4 bytes between each
		LocalHeap th = new LocalHeap(4, 4, 4);
		
		int i = buildTestData.length;
		while (--i>=0) {
			byte[] temp = buildTestData[i].getBytes();
			th.set(i, temp, 0, temp.length);
		}
		// test the data
		StringBuilder builder = new StringBuilder();
		i = buildTestData.length;
		while (--i>=0) {
			builder.setLength(0);
			th.get(i,builder);
			assertEquals(buildTestData[i],builder.toString());
		}
		return th;
	}
	
	@Test
	public void replaceAndMoveRightTest() {
		//Need to test move right and move left.
		LocalHeap th = buildTestHeap();
		//
		String bigString = "abcdefghijkl";
		byte[] bigData = bigString.getBytes();
		
		th.set(0, bigData, 0, bigData.length);
		
		StringBuilder builder = new StringBuilder();
		builder.setLength(0);
		th.get(0, builder);
		assertEquals(bigString,builder.toString());
		
		builder.setLength(0);
		th.get(1, builder);
		assertEquals(buildTestData[1],builder.toString());
		
		builder.setLength(0);
		th.get(2, builder);
		assertEquals(buildTestData[2],builder.toString());
		
		builder.setLength(0);
		th.get(3, builder);
		assertEquals(buildTestData[3],builder.toString());
	}
	
	@Test
	public void replaceAndMoveLeftTest() {
		//Need to test move right and move left.
		LocalHeap th = buildTestHeap();
		//
		String bigString = "abcdefghijkl";
		byte[] bigData = bigString.getBytes();
		
		th.set(3, bigData, 0, bigData.length);
		
		StringBuilder builder = new StringBuilder();
		builder.setLength(0);
		th.get(3, builder);
		assertEquals(bigString,builder.toString());
		
		builder.setLength(0);
		th.get(2, builder);
		assertEquals(buildTestData[2],builder.toString());
		
		builder.setLength(0);
		th.get(1, builder);
		assertEquals(buildTestData[1],builder.toString());
		
		builder.setLength(0);
		th.get(0, builder);
		assertEquals(buildTestData[0],builder.toString());
	}
	
	@Test
	public void replaceMiddleAndMoveBothTest() {
		//Need to test move right and move left.
		LocalHeap th = buildTestHeap();
		//
		String bigString = "abcdefghijklmo";
		byte[] bigData = bigString.getBytes();
		
		th.set(1, bigData, 0, bigData.length);
		
		StringBuilder builder = new StringBuilder();
		builder.setLength(0);
		th.get(1, builder);
		assertEquals(bigString,builder.toString());
		
		builder.setLength(0);
		th.get(0, builder);
		assertEquals(buildTestData[0],builder.toString());
		
		builder.setLength(0);
		th.get(2, builder);
		assertEquals(buildTestData[2],builder.toString());
		
		builder.setLength(0);
		th.get(3, builder);
		assertEquals(buildTestData[3],builder.toString());
	}
	
	@Test
	public void tailAndMoveRightTest() {
		//Need to test move right and move left.
		LocalHeap th = buildTestHeap();
		//
		String tail = "qrstuv";
		byte[] tailData = tail.getBytes();
		
		th.appendTail(0, 1, tailData, 0, tailData.length);
				
		StringBuilder builder = new StringBuilder();
		builder.setLength(0);
		th.get(0, builder);
		assertEquals("abc"+tail,builder.toString());
		
		builder.setLength(0);
		th.get(1, builder);
		assertEquals(buildTestData[1],builder.toString());
		
		builder.setLength(0);
		th.get(2, builder);
		assertEquals(buildTestData[2],builder.toString());
		
		builder.setLength(0);
		th.get(3, builder);
		assertEquals(buildTestData[3],builder.toString());
	}
	
	@Test
	public void headAndMoveLeftTestChar() {
		//Need to test move right and move left.
		LocalHeap th = buildTestHeap();
		//
		String headString = "abcdef";
		byte[] headData = headString.getBytes();
		
		th.appendHead(3, 1, headData, 0, headData.length);
				
		StringBuilder builder = new StringBuilder();
		builder.setLength(0);
		th.get(3, builder);
		assertEquals(headString+"nop",builder.toString());
		
		builder.setLength(0);
		th.get(2, builder);
		assertEquals(buildTestData[2],builder.toString());
		
		builder.setLength(0);
		th.get(1, builder);
		assertEquals(buildTestData[1],builder.toString());
		
		builder.setLength(0);
		th.get(0, builder);
		assertEquals(buildTestData[0],builder.toString());
	}
	
	@Test
	public void headAndMoveLeftTestString() {
		//Need to test move right and move left.
		LocalHeap th = buildTestHeap();
		//
		String headString = "abcdef";
		
		th.appendHead(3, 1, headString, headString.length());
				
		StringBuilder builder = new StringBuilder();
		builder.setLength(0);
		th.get(3, builder);
		assertEquals(headString+"nop",builder.toString());
		
		builder.setLength(0);
		th.get(2, builder);
		assertEquals(buildTestData[2],builder.toString());
		
		builder.setLength(0);
		th.get(1, builder);
		assertEquals(buildTestData[1],builder.toString());
		
		builder.setLength(0);
		th.get(0, builder);
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
			th.set(i, data, 0, data.length);
		}
		
		i = testData.length;
		StringBuilder builder = new StringBuilder();
		while (--i>=0) {
			builder.setLength(0);
			th.get(i,builder);
			assertEquals(testData[i],builder.toString());
		}
		
		i = testData.length;
		while (--i>=0) {
			byte[] data = testData[i].getBytes();
			byte[] target = new byte[data.length];
			th.get(i,target, 0);
			assertTrue(Arrays.equals(data,target));
		}
		
		try {
			byte[] biggerString = "abcd".getBytes();
			th.set(0, biggerString, 0, biggerString.length);
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
			th.set(i, data, 0, data.length);
		}
		
		i = testData.length;
		StringBuilder builder = new StringBuilder();
		while (--i>=0) {
			builder.setLength(0);
			th.get(i,builder);
			assertEquals(testData[i],builder.toString());
		}
		
		i = testData.length;
		while (--i>=0) {
			byte[] data = testData[i].getBytes();
			byte[] target = new byte[data.length];
			th.get(i,target, 0);
			assertTrue(Arrays.equals(data,target));
		}
		
		//should be room enough for this.
		byte[] biggerString = "abcd".getBytes();
		th.set(0, biggerString, 0, biggerString.length);	
		
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
			th.set(i, data, 0, data.length);
		}
		
		i = testData.length;
		StringBuilder builder = new StringBuilder();
		while (--i>=0) {
			builder.setLength(0);
			th.get(i,builder);
			assertEquals(testData[i],builder.toString());
		}
		
		i = testData.length;
		while (--i>=0) {
			byte[] data = testData[i].getBytes();
			byte[] target = new byte[data.length];
			th.get(i,target, 0);
			assertTrue(Arrays.equals(data,target));
		}
		
		//should be room enough for this.
		byte[] biggerString = "abcd".getBytes();
		th.set(4, biggerString, 0, biggerString.length);

	}
	
}
