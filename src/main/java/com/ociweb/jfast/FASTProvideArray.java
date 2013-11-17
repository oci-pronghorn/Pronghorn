package com.ociweb.jfast;

public class FASTProvideArray implements FASTProvide {

	private int idx;
	
	private final boolean[]		 isNull;	
	private final long[]    		 longValue;
	private final int[]     		 intValue;
	private final byte[][]  		 bytesValue;
	private final CharSequence[] 	charSequenceValue;
	private final long[] 			mantissaValue;
	private final int[] 			exponentValue;
	
	public FASTProvideArray(boolean[] nullArray) {
		isNull = nullArray;
		longValue = new long[nullArray.length];
		intValue = new int[nullArray.length];
		bytesValue = new byte[0][nullArray.length];
		charSequenceValue = new CharSequence[nullArray.length];
		mantissaValue = new long[nullArray.length];
		exponentValue = new int[nullArray.length];
	}
	
	public FASTProvideArray(long[] longArray) {
		isNull = new boolean[longArray.length];
		longValue = longArray;
		intValue = new int[longArray.length];
		bytesValue = new byte[0][longArray.length];
		charSequenceValue = new CharSequence[longArray.length];
		mantissaValue = new long[longArray.length];
		exponentValue = new int[longArray.length];
	}
	
	public FASTProvideArray(int[] intArray) {
		isNull = new boolean[intArray.length];
		longValue = new long[intArray.length];
		intValue = intArray;
		bytesValue = new byte[0][intArray.length];
		charSequenceValue = new CharSequence[intArray.length];
		mantissaValue = new long[intArray.length];
		exponentValue = new int[intArray.length];
	}
	
	public FASTProvideArray(byte[][] bytesArray) {
		isNull = new boolean[bytesArray.length];
		longValue = new long[bytesArray.length];
		intValue = new int[bytesArray.length];
		bytesValue = bytesArray;
		charSequenceValue = new CharSequence[bytesArray.length];
		mantissaValue = new long[bytesArray.length];
		exponentValue = new int[bytesArray.length];
	}
	
	public FASTProvideArray(long[] mantissaArray, int[] exponentArray) {
		if (mantissaArray.length != exponentArray.length) {
			throw new UnsupportedOperationException(mantissaArray.length+" != "+exponentArray.length);
		}
		isNull = new boolean[mantissaArray.length];
		longValue = new long[mantissaArray.length];
		intValue = new int[mantissaArray.length];
		bytesValue = new byte[0][mantissaArray.length];
		charSequenceValue = new CharSequence[mantissaArray.length];
		mantissaValue = mantissaArray;
		exponentValue = exponentArray;
	}
	
	public FASTProvideArray(CharSequence[] charSeqArray) {
		isNull = new boolean[charSeqArray.length];
		longValue = new long[charSeqArray.length];
		intValue = new int[charSeqArray.length];
		bytesValue = new byte[0][charSeqArray.length];
		charSequenceValue = charSeqArray;
		mantissaValue = new long[charSeqArray.length];
		exponentValue = new int[charSeqArray.length];
	}
	
	///
	
	public FASTProvideArray(boolean[] nullArray, long[] longArray) {
		isNull = nullArray;
		longValue = longArray;
		intValue = new int[longArray.length];
		bytesValue = new byte[0][longArray.length];
		charSequenceValue = new CharSequence[longArray.length];
		mantissaValue = new long[longArray.length];
		exponentValue = new int[longArray.length];
	}
	
	public FASTProvideArray(boolean[] nullArray, int[] intArray) {
		isNull = nullArray;
		longValue = new long[intArray.length];
		intValue = intArray;
		bytesValue = new byte[0][intArray.length];
		charSequenceValue = new CharSequence[intArray.length];
		mantissaValue = new long[intArray.length];
		exponentValue = new int[intArray.length];
	}
	
	public FASTProvideArray(boolean[] nullArray, byte[][] bytesArray) {
		isNull = nullArray;
		longValue = new long[bytesArray.length];
		intValue = new int[bytesArray.length];
		bytesValue = bytesArray;
		charSequenceValue = new CharSequence[bytesArray.length];
		mantissaValue = new long[bytesArray.length];
		exponentValue = new int[bytesArray.length];
	}
	
	public FASTProvideArray(boolean[] nullArray, long[] mantissaArray, int[] exponentArray) {
		if (mantissaArray.length != exponentArray.length) {
			throw new UnsupportedOperationException(mantissaArray.length+" != "+exponentArray.length);
		}
		isNull = nullArray;
		longValue = new long[mantissaArray.length];
		intValue = new int[mantissaArray.length];
		bytesValue = new byte[0][mantissaArray.length];
		charSequenceValue = new CharSequence[mantissaArray.length];
		mantissaValue = mantissaArray;
		exponentValue = exponentArray;
	}
	
	public FASTProvideArray(boolean[] nullArray, CharSequence[] charSeqArray) {
		isNull = nullArray;
		longValue = new long[charSeqArray.length];
		intValue = new int[charSeqArray.length];
		bytesValue = new byte[0][charSeqArray.length];
		charSequenceValue = charSeqArray;
		mantissaValue = new long[charSeqArray.length];
		exponentValue = new int[charSeqArray.length];
	}
	
	
	
	@Override
	public boolean provideNull(int id) {
		if (isNull[idx]) { //only increment when we hit the null
			idx++;
			return true;
		}
		return false;
	}

	@Override
	public long provideLong(int id) {
		return longValue[idx++];
	}

	@Override
	public int provideInt(int id) {
		return intValue[idx++];
	}

	@Override
	public byte[] provideBytes(int id) {
		return bytesValue[idx++];
	}

	@Override
	public CharSequence provideCharSequence(int id) {
		return charSequenceValue[idx++];
	}

	@Override
	public void provideDecimal(int id, DecimalDTO target) {
		target.exponent = exponentValue[idx];
		target.mantissa = mantissaValue[idx++];
	}

	@Override
	public void openGroup(int maxPMapBytes) {
	}

	@Override
	public void closeGroup() {
	}

}
