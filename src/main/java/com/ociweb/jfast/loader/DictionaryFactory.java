//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.loader;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.TextHeap;
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
	
	
	
	
	/**
	 * Class hold everything needed to define the dictionary
	 * 
	 * There is a many-to-many relationship between templates and applicationType.
	 * applicationTypes are fully abstract types
	 * 
	 * SharedDictionary:
	 * 		None/Template: default we just use the structures produced here.
	 *      Type: application type, other structures sharing this field&appType must share value.
	 *      Global: ignore template/type and just use field id gobally
	 *      <custom>: use custom named structure.
	 *      
	 * 
	 * 
	 * 
	 * 
	 * needs:   
	 *    token must hold extra data for dictionary!!      
	 *    2 bits
	 *       00 none/template
	 *       01 type
	 *       10 global
	 *       11 use inernal looup of custom by field id. 
	 * 
	 */
	
	char[][] appTypes = null;//appType identifiers used in XML
	
	private static final int MAX_FIELDS = 1<<20; //1M
	private static final int DEFAULT_TEXT_LENGTH = 64;
	private static final int INIT_GROW_STEP = 16;
	private final int singleGapSize = 64; //default to avoid false cache sharing. 
		
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
		
	private int singleTextSize; //TODO: need an independent value for byteValues?
	private int[] tokenLookup;

	//need to hold groups and field ids;
	//values <0 are possible stop nodes boardering the group.
	//all other values are ids
	private int[] structure;
	private int structureCount;
	
	
	public DictionaryFactory(int[] tokenLookup) {
		this(MAX_FIELDS,MAX_FIELDS,MAX_FIELDS,DEFAULT_TEXT_LENGTH,MAX_FIELDS,MAX_FIELDS, tokenLookup);
	}
		
	public DictionaryFactory(int integerCount, int longCount, int charCount, int singleCharLength, int decimalCount, int bytesCount, int[] tokenLookup) {
		 this.integerCount=integerCount;
		 this.longCount=longCount;
		 this.charCount=charCount;
		 this.decimalCount=decimalCount;
		 this.bytesCount=bytesCount;
		 this.tokenLookup = tokenLookup;
		
		 this.structureCount = 0;
		 this.structure = new int[INIT_GROW_STEP];
		 
		 this.integerInitCount=0;
		 this.integerInitIndex = new int[INIT_GROW_STEP];
		 this.integerInitValue = new int[INIT_GROW_STEP];
		
		 this.longInitCount=0;
		 this.longInitIndex = new int[INIT_GROW_STEP];
		 this.longInitValue = new long[INIT_GROW_STEP];
			
		 this.charInitCount=0;
		 this.charInitIndex = new int[INIT_GROW_STEP];
		 this.charInitValue = new char[INIT_GROW_STEP][];
		 this.singleTextSize = singleCharLength;
		
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
	
//	public DictionaryFactory(PrimitiveReader pr) {
//		
//		
//		
//	}
	
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

		pw.writeIntegerUnsigned(singleTextSize);
		
		pw.writeIntegerUnsigned(tokenLookup.length);
		c = tokenLookup.length;
		while (--c>=0) {
			pw.writeIntegerSigned(tokenLookup[c]);
		}
		
		pw.writeIntegerUnsigned(structureCount);
		c = structure.length;
		while (--c>=0) {
			pw.writeIntegerSigned(structure[c]);
		}
		
		
		/*
		Fastest searialize deserialize however its more verbose and there is no 
		object dectection and construction.
		
		These files can be deleted and modified but those changes are only refelected on startup.
		New templates can be added but an explicit call must be made to load them.
		The new templates will be loaded dynamicaly on first use but this is not recommended.
		


		 */
		
		
		
	}
	
	public void setTypeCounts(int integerCount, int longCount, int charCount, int decimalCount, int bytesCount) {
		 this.integerCount=integerCount;
		 this.longCount=longCount;
		 this.charCount=charCount;
		 this.decimalCount=decimalCount;
		 this.bytesCount=bytesCount;
	}
	
	public void addFieldId(int id) {
		structure[structureCount] = id;
		if (++structureCount>=structure.length) {
			int newLength = structureCount+INIT_GROW_STEP;
			int[] temp = new int[newLength];
			System.arraycopy(structure, 0, temp, 0, structure.length);
			structure = temp;
		}
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
	
	public TextHeap charDictionary() {
		TextHeap heap = new TextHeap(singleTextSize, singleGapSize, nextPowerOfTwo(charCount),
				                     charInitTotalLength, charInitIndex, charInitValue);
		heap.reset();	
		return heap;
	}
	
	public ByteHeap byteDictionary() {
		ByteHeap heap = new ByteHeap(singleTextSize, singleGapSize, nextPowerOfTwo(bytesCount),
                                     byteInitTotalLength, byteInitIndex, byteInitValue);
		heap.reset();
		return heap;
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

	public int[] getTokenLookup() {
		return tokenLookup;
	}
	
	
}
