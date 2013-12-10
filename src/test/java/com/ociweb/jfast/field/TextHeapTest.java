package com.ociweb.jfast.field;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

public class TextHeapTest {

	
	@Test
	public void simpleWriteReadTest() {
		
		String[] testData = new String[] {
			"a","b","c","hello","world"	
		};
		
		TextHeap th = new TextHeap(6,6, testData.length);
		
		int i = testData.length;
		while (--i>=0) {
			char[] data = testData[i].toCharArray();
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
			char[] data = testData[i].toCharArray();
			char[] target = new char[data.length];
			th.get(i,target, 0);
			assertTrue(Arrays.equals(data,target));
		}
		
	}
	
	@Test
	public void simpleReplacementTest() {
		
		String[] testData = new String[] {
			"a","b","c","hello","world","string"	
		};
		
		TextHeap th = new TextHeap(6,6, testData.length);
		
		//write each test value into the heap
		int i = testData.length;
		while (--i>=0) {
			char[] data = testData[i].toCharArray();
			th.set(testData.length-(i+1), data, 0, data.length);
		}
		//now replace each backwards so they have something shorter or longer
		i = testData.length;
		while (--i>=0) {
			char[] data = testData[i].toCharArray();
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
			char[] data = testData[i].toCharArray();
			char[] target = new char[data.length];
			th.get(i,target, 0);
			assertTrue(Arrays.equals(data,target));
		}
		
	}
	
	@Test
	public void modifyTailTest() {
		
		TextHeap th = new TextHeap(8,8, 10);
		String root = "hello";
		String suffix = "world";
		String replace = "everyone";
		
		char[] data = (root+suffix).toCharArray();
		th.set(2, data, 0, data.length);
		//
		StringBuilder builder = new StringBuilder();
		th.get(2, builder);
		assertEquals(root+suffix,builder.toString());
		//
		char[] tail = replace.toCharArray();
		th.appendTail(2, suffix.length(), tail, 0, tail.length);
		//
		builder.setLength(0);
		th.get(2, builder);
		assertEquals(root+replace,builder.toString());
	}
	
	@Test
	public void modifyHeadTest() {
		
		TextHeap th = new TextHeap(8,8, 10);
		String prefix = "hello";
		String replace = "goodbye";
		String root = "world";
		
		char[] data = (prefix+root).toCharArray();
		th.set(2, data, 0, data.length);
		//
		StringBuilder builder = new StringBuilder();
		th.get(2, builder);
		assertEquals(prefix+root,builder.toString());
		//
		char[] tail = replace.toCharArray();
		th.appendHead(2, prefix.length(), tail, 0, tail.length);
		//
		builder.setLength(0);
		th.get(2, builder);
		assertEquals(replace+root,builder.toString());
	}
	
	private static final String[] buildTestData = new String[] {"abcd","efgh","ijkl","mnop"};
	private TextHeap buildTestHeap() {
		//each string is 4 chars with 4 chars between each
		TextHeap th = new TextHeap(4, 4, 4);
		
		int i = buildTestData.length;
		while (--i>=0) {
			char[] temp = buildTestData[i].toCharArray();
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
		TextHeap th = buildTestHeap();
		//
		String bigString = "abcdefghijkl";
		char[] bigData = bigString.toCharArray();
		
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
		TextHeap th = buildTestHeap();
		//
		String bigString = "abcdefghijkl";
		char[] bigData = bigString.toCharArray();
		
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
		TextHeap th = buildTestHeap();
		//
		String bigString = "abcdefghijklmo";
		char[] bigData = bigString.toCharArray();
		
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
		TextHeap th = buildTestHeap();
		//
		String tail = "qrstuv";
		char[] tailData = tail.toCharArray();
		
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
	public void headAndMoveLeftTest() {
		//Need to test move right and move left.
		TextHeap th = buildTestHeap();
		//
		String headString = "abcdef";
		char[] headData = headString.toCharArray();
		
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
	//private static final String[] buildTestData = new String[] {"abcd","efgh","ijkl","mnop"};
	
	
	@Test
	public void fullTextTest() {
		
		String[] testData = new String[] {
			"abc","def","ghi","jkl","mno"	
		};
		
		TextHeap th = new TextHeap(3, 0, testData.length);
		
		int i = testData.length;
		while (--i>=0) {
			char[] data = testData[i].toCharArray();
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
			char[] data = testData[i].toCharArray();
			char[] target = new char[data.length];
			th.get(i,target, 0);
			assertTrue(Arrays.equals(data,target));
		}
		
		try {
			char[] biggerString = "abcd".toCharArray();
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
		
		TextHeap th = new TextHeap(3, 0, testData.length);
		
		int i = testData.length;
		while (--i>=0) {
			char[] data = testData[i].toCharArray();
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
			char[] data = testData[i].toCharArray();
			char[] target = new char[data.length];
			th.get(i,target, 0);
			assertTrue(Arrays.equals(data,target));
		}
		
		//should be room enough for this.
		char[] biggerString = "abcd".toCharArray();
		th.set(0, biggerString, 0, biggerString.length);	
		
	}
	
	@Test
	public void fullTextWithOneSpotOnLeftTest() {
		
		String[] testData = new String[] {
			"bc","def","ghi","jkl","mno"	
		};
		
		TextHeap th = new TextHeap(3, 0, testData.length);
		
		int i = testData.length;
		while (--i>=0) {
			char[] data = testData[i].toCharArray();
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
			char[] data = testData[i].toCharArray();
			char[] target = new char[data.length];
			th.get(i,target, 0);
			assertTrue(Arrays.equals(data,target));
		}
		
		//should be room enough for this.
		char[] biggerString = "abcd".toCharArray();
		th.set(4, biggerString, 0, biggerString.length);

	}
	
}
