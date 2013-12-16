package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.TextHeap;


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
	
	private static final int MAX_FIELDS = 1<<20; //1M
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
	
	private int decimalExponentInitCount;
	private int[] decimalExponentInitIndex;
	private int[] decimalExponentInitValue;
	
	private int decimalMantissaInitCount;
	private int[] decimalMantissaInitIndex;
	private long[] decimalMantissaInitValue;
	
	private int byteInitCount;
	private int[] byteInitIndex;
	private byte[][] byteInitValue;
	

	
//	public DictionaryConstructionRules(FASTStaticReader reader) {
//		
//		//TODO: read all the values back in.
//		
//	}
	public DictionaryFactory() {
		this(MAX_FIELDS,MAX_FIELDS,MAX_FIELDS,MAX_FIELDS,MAX_FIELDS);
	}
	
	public DictionaryFactory(int integerCount, int longCount, int charCount, int decimalCount, int bytesCount) {
		 this.integerCount=integerCount;
		 this.longCount=longCount;
		 this.charCount=charCount;
		 this.decimalCount=decimalCount;
		 this.bytesCount=bytesCount;
		
		 integerInitCount=0;
		 integerInitIndex = new int[INIT_GROW_STEP];
		 integerInitValue = new int[INIT_GROW_STEP];
		
		 longInitCount=0;
		 longInitIndex = new int[INIT_GROW_STEP];
		 longInitValue = new long[INIT_GROW_STEP];
			
		 charInitCount=0;
		 charInitIndex = new int[INIT_GROW_STEP];
		 charInitValue = new char[INIT_GROW_STEP][];
		
		 decimalExponentInitCount=0;
		 decimalExponentInitIndex = new int[INIT_GROW_STEP];
		 decimalExponentInitValue = new int[INIT_GROW_STEP];
		
		 decimalMantissaInitCount=0;
		 decimalMantissaInitIndex = new int[INIT_GROW_STEP];
		 decimalMantissaInitValue = new long[INIT_GROW_STEP];
		
		 byteInitCount=0;
		 byteInitIndex = new int[INIT_GROW_STEP];
		 byteInitValue = new byte[INIT_GROW_STEP][];
	}
	
	
	public void setTypeCounts(int integerCount, int longCount, int charCount, int decimalCount, int bytesCount) {
		 this.integerCount=integerCount;
		 this.longCount=longCount;
		 this.charCount=charCount;
		 this.decimalCount=decimalCount;
		 this.bytesCount=bytesCount;
	}
	
	public void addInit(int idx, int value) {
		
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
	
	public void addInit(int idx, long value) {
		
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
	
	public void addInitDecimal(int idx, int exponent) {
		
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
	
	public void addInitDecimal(int idx, long mantissa) {
		
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
	
	public void addInit(int idx, byte[] value) {
		
		byteInitIndex[byteInitCount] = idx;
		byteInitValue[byteInitCount] = value;
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
	
	public int nextPowerOfTwo(int value) {
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
	
	public TextHeap charDictionary() {
		
		int singleTextSize = 0; //TODO: how is this set?
		int singleGapSize = 0; //TODO: how is this set?
		
		TextHeap heap = new TextHeap(singleTextSize, singleGapSize, nextPowerOfTwo(charCount));
		
		int i = charInitCount;
		while (--i>=0) {
			heap.set(charInitIndex[i], charInitValue[i], 0, charInitValue[i].length);
		}
		return heap;
	}
	
	public byte[][] byteDictionary() {
		byte[][] array = new byte[nextPowerOfTwo(bytesCount)][];
		int i = byteInitCount;
		while (--i>=0) {
			array[byteInitIndex[i]] = byteInitValue[i];
		}
		return array;
	}
	

	public void reset(int[] values) {
		int i;
		i = integerInitCount;
		while (--i>=0) {
			int j = integerInitIndex[i];
			values[j] = integerInitValue[i];
		}
	}

	public void reset(long[] values) {
		int i;
		i = longInitCount;
		while (--i>=0) {
			int j = longInitIndex[i];
			values[j] = longInitValue[i];
		}
	}
	
	public void reset(int[] exponents, long[] mantissa) {

		int i = decimalExponentInitCount;
		while (--i>=0) {
			int j = decimalExponentInitIndex[i];
			exponents[j] = decimalExponentInitValue[i];
		}

		i = decimalMantissaInitCount;
		while (--i>=0) {
			int j = decimalMantissaInitIndex[i];
			mantissa[j] = decimalMantissaInitValue[i];
		}
	}
	
	public void reset(char[][] values) {
		int i = charInitCount;
		while (--i>=0) {
			int j = charInitIndex[i];
			values[j] = charInitValue[i];
		}
	}
	
	public void reset(byte[][] values) {
		int i = byteInitCount;
		while (--i>=0) {
			int j = byteInitIndex[i];
			values[j] = byteInitValue[i];
		}
	}
	
	
}
