package com.ociweb.jfast;


//TODO: testing for use of boxed values in place of null if this works
//       we can develop a general purpose java serialization that can restore boxed values.

public class FASTProvideArrayBoxed implements FASTProvide {

	public static final int IDX_LONG     = 1;
	public static final int IDX_INTEGER  = 2;
	public static final int IDX_BYTES    = 3;
	public static final int IDX_STRING   = 4;
	public static final int IDX_DECIMAL  = 5;
	
	
	private int idx;
		
	private final Long[]    		 longValue;
	private final Integer[]     		 intValue;
	private final byte[][]  		 bytesValue;
	private final CharSequence[] 	charSequenceValue;
	private final Long[] 			mantissaValue;
	private final Integer[] 			exponentValue;
	
	
	public FASTProvideArrayBoxed(Long[] longArray) {

		longValue = longArray;
		intValue = new Integer[longArray.length];
		bytesValue = new byte[0][longArray.length];
		charSequenceValue = new CharSequence[longArray.length];
		mantissaValue = new Long[longArray.length];
		exponentValue = new Integer[longArray.length];
	}
	
	public FASTProvideArrayBoxed(Integer[] intArray) {

		longValue = new Long[intArray.length];
		intValue = intArray;
		bytesValue = new byte[0][intArray.length];
		charSequenceValue = new CharSequence[intArray.length];
		mantissaValue = new Long[intArray.length];
		exponentValue = new Integer[intArray.length];
	}
	
	public FASTProvideArrayBoxed(byte[][] bytesArray) {

		longValue = new Long[bytesArray.length];
		intValue = new Integer[bytesArray.length];
		bytesValue = bytesArray;
		charSequenceValue = new CharSequence[bytesArray.length];
		mantissaValue = new Long[bytesArray.length];
		exponentValue = new Integer[bytesArray.length];
	}
	
	public FASTProvideArrayBoxed(Long[] mantissaArray, Integer[] exponentArray) {
		if (mantissaArray.length != exponentArray.length) {
			throw new UnsupportedOperationException(mantissaArray.length+" != "+exponentArray.length);
		}

		longValue = new Long[mantissaArray.length];
		intValue = new Integer[mantissaArray.length];
		bytesValue = new byte[0][mantissaArray.length];
		charSequenceValue = new CharSequence[mantissaArray.length];
		mantissaValue = mantissaArray;
		exponentValue = exponentArray;
	}
	
	public FASTProvideArrayBoxed(CharSequence[] charSeqArray) {

		longValue = new Long[charSeqArray.length];
		intValue = new Integer[charSeqArray.length];
		bytesValue = new byte[0][charSeqArray.length];
		charSequenceValue = charSeqArray;
		mantissaValue = new Long[charSeqArray.length];
		exponentValue = new Integer[charSeqArray.length];
	}
	
	//TODO: for now implement provide null but if the performance is good
	//this will be integrated into the methods which will each come in 2 forms.
	
	@Override
	public boolean provideNull(int id) {
		//only increment when we hit the null
		switch(id) {
			case IDX_LONG:
				if (null == longValue[idx]) {
					idx++;
					return true;
				}
				return false;
		 	case IDX_INTEGER:
		 		if (null == intValue[idx]) {
		 			idx++;
		 			return true;
		 		}
		 		return false;
		 	case IDX_BYTES:
		 		if (null == bytesValue[idx]) {
		 			idx++;
		 			return true;
		 		}
		 		return false;
		 	case IDX_STRING:
		 		if (null == charSequenceValue[idx]) {
		 			idx++;
		 			return true;
		 		}
		 		return false;
		 	case IDX_DECIMAL:
		 		if (null == exponentValue[idx]) {
		 			idx++;
		 			return true;
		 		}
		 		return false;
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
	public void beginGroup() {
	}

	@Override
	public void endGroup() {
	}

}
