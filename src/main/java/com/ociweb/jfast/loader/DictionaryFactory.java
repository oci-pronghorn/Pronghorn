//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.loader;

import java.util.Arrays;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;



/**
 * Holds count of how many of each type of field is required and what the default values are.
 * Default(initial) values are sparse so only the index and the value are kept in this class.
 * Upon request this class will generate the needed dictionary arrays fully populated.
 * 
 * 
 * @author Nathan Tippy
 *
 */
public class DictionaryFactory {
	
	/*
	 * Dictionary:
	 * Max count of all fields for dictionary shared across templates. With default values.
	 * 
	 * Catalog:
	 * tokens lookup and IDs span across templates and dictionary and belong to the catalog.
	 * hold app opional return values for each field.
	 * 
	 * Template:
	 * templates hold ordered list of fields/tokens.
	 * 
	 * 
	 * catalog
	 * 		template
	 * 	    dictionary (shared between templates?)
	 * 
	 * 
	 * 
	 * 
	 */
	
	//TODO: must hold list of token id values for each dictionary.
	
	
	char[][] appTypes = null;//appType identifiers used in XML
	
	private static final int INIT_GROW_STEP = 16; 
		
	private int integerCount;
	private int longCount;
	private int charCount;
	private int decimalCount;
	private int bytesCount;
	
	private int integerInitCount;
	private int[] integerInitIndex;
	private int[] integerInitValue;
	
	private int longInitCount;
	private int[] longInitIndex;
	private long[] longInitValue;
		
	private int charInitCount;	
	private int[] charInitIndex;
	private char[][] charInitValue;
	private int charInitTotalLength;
	
	private int decimalExponentInitCount;
	private int[] decimalExponentInitIndex;
	private int[] decimalExponentInitValue;
	
	private int decimalMantissaInitCount;
	private int[] decimalMantissaInitIndex;
	private long[] decimalMantissaInitValue;
	
	private int byteInitCount;
	private int[] byteInitIndex;
	private byte[][] byteInitValue;
	
	private int byteInitTotalLength;
		
	public DictionaryFactory() {
		
		 
		 this.integerInitCount=0;
		 this.integerInitIndex = new int[INIT_GROW_STEP];
		 this.integerInitValue = new int[INIT_GROW_STEP];
		
		 this.longInitCount=0;
		 this.longInitIndex = new int[INIT_GROW_STEP];
		 this.longInitValue = new long[INIT_GROW_STEP];
			
		 this.charInitCount=0;
		 this.charInitIndex = new int[INIT_GROW_STEP];
		 this.charInitValue = new char[INIT_GROW_STEP][];
		
		 this.decimalExponentInitCount=0;
		 this.decimalExponentInitIndex = new int[INIT_GROW_STEP];
		 this.decimalExponentInitValue = new int[INIT_GROW_STEP];
		
		 this.decimalMantissaInitCount=0;
		 this.decimalMantissaInitIndex = new int[INIT_GROW_STEP];
		 this.decimalMantissaInitValue = new long[INIT_GROW_STEP];
		
		 this.byteInitCount=0;
		 this.byteInitIndex = new int[INIT_GROW_STEP];
		 this.byteInitValue = new byte[INIT_GROW_STEP][];
	}

	public void setTypeCounts(int integerCount, int longCount, int charCount, int decimalCount, int bytesCount) {
		this.integerCount=integerCount;
		 this.longCount=longCount;
		 this.charCount=charCount;
		 this.decimalCount=decimalCount;
		 this.bytesCount=bytesCount;
	}
	
	public DictionaryFactory(PrimitiveReader reader) {
		
		this.integerCount=reader.readIntegerUnsigned();
		this.longCount=reader.readIntegerUnsigned();
		this.charCount=reader.readIntegerUnsigned();
		this.decimalCount=reader.readIntegerUnsigned();
		this.bytesCount=reader.readIntegerUnsigned();
		
		this.integerInitCount = reader.readIntegerUnsigned();
		this.integerInitIndex = new int[integerInitCount];
		this.integerInitValue = new int[integerInitCount];
		int c = integerInitCount;
		while (--c>=0) {
			integerInitIndex[c] = reader.readIntegerUnsigned();
			integerInitValue[c] = reader.readIntegerSigned();
		}
		
		this.longInitCount = reader.readIntegerUnsigned();
		this.longInitIndex = new int[longInitCount];
		this.longInitValue = new long[longInitCount];
		c = longInitCount;
		while (--c>=0) {
			longInitIndex[c] = reader.readIntegerUnsigned();
			longInitValue[c] = reader.readLongSigned();
		}
		
		this.charInitCount = reader.readIntegerUnsigned();
		this.charInitIndex = new int[charInitCount];
		this.charInitValue = new char[charInitCount][];
		c = charInitCount;
		while (--c>=0) {
			charInitIndex[c] = reader.readIntegerUnsigned();
			int len = reader.readIntegerUnsigned();
			char[] value = new char[len];
			reader.readTextUTF8(value, 0 , len);
			charInitValue[c] = value;
		}
		this.charInitTotalLength = reader.readIntegerUnsigned();
		
		this.decimalExponentInitCount = reader.readIntegerUnsigned();
		this.decimalExponentInitIndex = new int[decimalExponentInitCount];
		this.decimalExponentInitValue = new int[decimalExponentInitCount];
		c = decimalExponentInitCount;
		while (--c>=0) {
			decimalExponentInitIndex[c] = reader.readIntegerUnsigned();
			decimalExponentInitValue[c] = reader.readIntegerSigned();
		}
		
		this.decimalMantissaInitCount = reader.readIntegerUnsigned();
		this.decimalMantissaInitIndex = new int[decimalMantissaInitCount];
		this.decimalMantissaInitValue = new long[decimalMantissaInitCount];
		c = decimalMantissaInitCount;
		while (--c>=0) {
			decimalMantissaInitIndex[c] = reader.readIntegerUnsigned();
			decimalMantissaInitValue[c] = reader.readLongSigned();
		}
		
		
		this.byteInitCount = reader.readIntegerUnsigned();
		this.byteInitIndex = new int[byteInitCount];
		this.byteInitValue = new byte[byteInitCount][];
		c = byteInitCount;
		while (--c>=0) {
			byteInitIndex[c] = reader.readIntegerUnsigned();
			int len = reader.readIntegerUnsigned();
			byte[] value = new byte[len];
			reader.readByteData(value, 0 , len);
			byteInitValue[c] = value;
		}
		byteInitTotalLength = reader.readIntegerUnsigned();

		
	}

	public void save(PrimitiveWriter pw) {
				
		pw.writeIntegerUnsigned(integerCount);
		pw.writeIntegerUnsigned(longCount);
		pw.writeIntegerUnsigned(charCount);
		pw.writeIntegerUnsigned(decimalCount);
		pw.writeIntegerUnsigned(bytesCount);
		
		pw.writeIntegerUnsigned(integerInitCount);
		int c = integerInitCount;
		while (--c>=0) {
			pw.writeIntegerUnsigned(integerInitIndex[c]);
			pw.writeIntegerSigned(integerInitValue[c]);
		}
		
		pw.writeIntegerUnsigned(longInitCount);
		c = longInitCount;
		while (--c>=0) {
			pw.writeIntegerUnsigned(longInitIndex[c]);
			pw.writeLongSigned(longInitValue[c]);
		}
		
		pw.writeIntegerUnsigned(charInitCount);
		c = charInitCount;
		while (--c>=0) {
			pw.writeIntegerUnsigned(charInitIndex[c]);
			char[] value = charInitValue[c];
			pw.writeIntegerUnsigned(value.length);
			pw.writeTextUTF(value, 0, value.length);
		}
		pw.writeIntegerUnsigned(charInitTotalLength);
		
		pw.writeIntegerUnsigned(decimalExponentInitCount);
		c = decimalExponentInitCount;
		while (--c>=0) {
			pw.writeIntegerUnsigned(decimalExponentInitIndex[c]);
			pw.writeIntegerSigned(decimalExponentInitValue[c]);
		}		
		
		pw.writeIntegerUnsigned(decimalMantissaInitCount);
		c = decimalMantissaInitCount;
		while (--c>=0) {
			pw.writeIntegerUnsigned(decimalMantissaInitIndex[c]);
			pw.writeLongSigned(decimalMantissaInitValue[c]);
		}
				
		pw.writeIntegerUnsigned(byteInitCount);
		c = byteInitCount;
		while (--c>=0) {
			pw.writeIntegerUnsigned(byteInitIndex[c]);
			byte[] value = byteInitValue[c];
			pw.writeIntegerUnsigned(value.length);
			pw.writeByteArrayData(value,0,value.length);
		}
		pw.writeIntegerUnsigned(byteInitTotalLength);
				
		
		
		/*
		Fastest searialize deserialize however its more verbose and there is no 
		object dectection and construction.
		
		These files can be deleted and modified but those changes are only refelected on startup.
		New templates can be added but an explicit call must be made to load them.
		The new templates will be loaded dynamicaly on first use but this is not recommended.
		


		 */
		
		
		
	}
	
	
	
	public void addInitInteger(int idx, int value) {
		
		integerInitIndex[integerInitCount] = idx;
		integerInitValue[integerInitCount] = value;
		if (++integerInitCount>=integerInitValue.length) {
			int newLength = integerInitValue.length+INIT_GROW_STEP;
			int[] temp1 = new int[newLength];
			int[] temp2 = new int[newLength];
			System.arraycopy(integerInitIndex, 0, temp1, 0, integerInitValue.length);
			System.arraycopy(integerInitValue, 0, temp2, 0, integerInitValue.length);
			integerInitIndex = temp1;
			integerInitValue = temp2;			
		}
		
	}
	
	public void addInitLong(int idx, long value) {
		
		longInitIndex[longInitCount] = idx;
		longInitValue[longInitCount] = value;
		if (++longInitCount>=longInitValue.length) {
			int newLength = longInitValue.length+INIT_GROW_STEP;
			int[] temp1 = new int[newLength];
			long[] temp2 = new long[newLength];
			System.arraycopy(longInitIndex, 0, temp1, 0, longInitIndex.length);
			System.arraycopy(longInitValue, 0, temp2, 0, longInitIndex.length);
			longInitIndex = temp1;
			longInitValue = temp2;			
		}
		
	}
	
	public void addInitDecimalExponent(int idx, int exponent) {
		
		decimalExponentInitIndex[decimalExponentInitCount] = idx;
		decimalExponentInitValue[decimalExponentInitCount] = exponent;
		if (++decimalExponentInitCount>=decimalExponentInitValue.length) {
			int newLength = decimalExponentInitValue.length+INIT_GROW_STEP;
			int[] temp1 = new int[newLength];
			int[] temp2 = new int[newLength];
			System.arraycopy(decimalExponentInitIndex, 0, temp1, 0, decimalExponentInitValue.length);
			System.arraycopy(decimalExponentInitValue, 0, temp2, 0, decimalExponentInitValue.length);
			decimalExponentInitIndex = temp1;
			decimalExponentInitValue = temp2;			
		}
		
	}
	
	public void addInitDecimalMantissa(int idx, long mantissa) {
		
		decimalMantissaInitIndex[decimalMantissaInitCount] = idx;
		decimalMantissaInitValue[decimalMantissaInitCount] = mantissa;
		if (++decimalMantissaInitCount>=decimalMantissaInitValue.length) {
			int newLength = decimalMantissaInitValue.length+INIT_GROW_STEP;
			int[] temp1 = new int[newLength];
			long[] temp2 = new long[newLength];
			System.arraycopy(decimalMantissaInitIndex, 0, temp1, 0, decimalMantissaInitIndex.length);
			System.arraycopy(decimalMantissaInitValue, 0, temp2, 0, decimalMantissaInitIndex.length);
			decimalMantissaInitIndex = temp1;
			decimalMantissaInitValue = temp2;			
		}
		
	}
	
	
	public void addInit(int idx, char[] value) {
		
		charInitIndex[charInitCount] = idx;
		charInitValue[charInitCount] = value;
		charInitTotalLength+=value.length;
		if (++charInitCount>=charInitValue.length) {
			int newLength = charInitValue.length+INIT_GROW_STEP;
			int[] temp1 = new int[newLength];
			char[][] temp2 = new char[newLength][];
			System.arraycopy(charInitIndex, 0, temp1, 0, charInitValue.length);
			System.arraycopy(charInitValue, 0, temp2, 0, charInitValue.length);
			charInitIndex = temp1;
			charInitValue = temp2;			
		}
		
	}	
	
	private char[] ZERO_LENGTH_CHARS = new char[0];
	
	public char[] getInitChars(int idx) {
		int i = charInitCount;
		while (--i>=0) {
			if (idx == charInitIndex[i]) {
				return charInitValue[i];
			}
		}
		return ZERO_LENGTH_CHARS; 
	}
	
	
	public void addInit(int idx, byte[] value) {
		
		byteInitIndex[byteInitCount] = idx;
		byteInitValue[byteInitCount] = value;
		byteInitTotalLength+=value.length;
		if (++byteInitCount>=byteInitValue.length) {
			int newLength = byteInitValue.length+INIT_GROW_STEP;
			int[] temp1 = new int[newLength];
			byte[][] temp2 = new byte[newLength][];
			System.arraycopy(byteInitIndex, 0, temp1, 0, byteInitValue.length);
			System.arraycopy(byteInitValue, 0, temp2, 0, byteInitValue.length);
			byteInitIndex = temp1;
			byteInitValue = temp2;			
		}
		
	}	
	
	public static int nextPowerOfTwo(int value) {
		int temp = value;
		int result = 0;
		while (0!=temp) {
			temp = temp>>1;
			result++;
		}
		//System.err.println(value+" -> "+(1<<result));
		
		return 1<<result;
	}
	
	
	public int[] integerDictionary() {
		int[] array = new int[nextPowerOfTwo(integerCount)];
		int i = integerInitCount;
		while (--i>=0) {
			array[integerInitIndex[i]] = integerInitValue[i];
		}
		return array;
	}
	
	public long[] longDictionary() {
		long[] array = new long[nextPowerOfTwo(longCount)];
		int i = longInitCount;
		while (--i>=0) {
			array[longInitIndex[i]] = longInitValue[i];
		}
		return array;
	}
	
	public int[] decimalExponentDictionary() {
		int[] array = new int[nextPowerOfTwo(decimalCount)];
		int i = decimalExponentInitCount;
		while (--i>=0) {
			array[decimalExponentInitIndex[i]] = decimalExponentInitValue[i];
		}
		return array;
	}
	
	public long[] decimalMantissaDictionary() {
		long[] array = new long[nextPowerOfTwo(decimalCount)];
		int i = decimalMantissaInitCount;
		while (--i>=0) {
			array[decimalMantissaInitIndex[i]] = decimalMantissaInitValue[i];
		}
		return array;
	}
	
	public TextHeap charDictionary(int singleTextSize, int gapSize) {
		if (charCount==0) {
			return null;
		}
		TextHeap heap = new TextHeap(singleTextSize, gapSize, nextPowerOfTwo(charCount),
				                     charInitTotalLength, charInitIndex, charInitValue);
		heap.reset();	
		return heap;
	}
	
	public ByteHeap byteDictionary(int singleBytesSize, int gapSize) {
		if (bytesCount==0) {
			return null;
		}
		ByteHeap heap = new ByteHeap(singleBytesSize, gapSize, nextPowerOfTwo(bytesCount),
                                     byteInitTotalLength, byteInitIndex, byteInitValue);
		heap.reset();
		return heap;
	}
	

	public void reset(int[] values) {
		int i = integerCount;
		while (--i>=0) {
			values[i] = 0;
		}
		i = integerInitCount;
		while (--i>=0) {
			values[integerInitIndex[i]] = integerInitValue[i];
		}
	}

	public void reset(long[] values) {
		int i = longCount;
		while (--i>=0) {
			values[i] = 0;
		}
		i = longInitCount;
		while (--i>=0) {
			values[longInitIndex[i]] = longInitValue[i];
		}
	}
	
	public void reset(int[] exponents, long[] mantissa) {
		int i = decimalCount;
		while (--i>=0) {
			exponents[i] = 0;
			mantissa[i] = 0;
		}
		i = decimalExponentInitCount;
		while (--i>=0) {
			exponents[decimalExponentInitIndex[i]] = decimalExponentInitValue[i];
		}

		i = decimalMantissaInitCount;
		while (--i>=0) {
			mantissa[decimalMantissaInitIndex[i]] = decimalMantissaInitValue[i];
		}
	}
	
	public void reset(TextHeap heap) {
		if (null!=heap) {
			heap.reset();
		}
	}
	
	public void reset(ByteHeap heap) {
		if (null!=heap) {
			heap.reset();
		}
	}


	
	
}
