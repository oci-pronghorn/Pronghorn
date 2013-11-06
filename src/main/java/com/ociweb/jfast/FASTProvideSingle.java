package com.ociweb.jfast;

public class FASTProvideSingle implements FASTProvide {

	final boolean		 isNull;
	final long    		 longValue;
	final int     		 intValue;
	final byte[]  		 bytesValue;
	final CharSequence charSequenceValue;
	final long manissaValue;
	final int exponentValue;
	
	public FASTProvideSingle() {
		isNull = true;
		longValue = 0;
		intValue = 0;
		bytesValue = null;
		charSequenceValue = null;
		manissaValue = 0;
		exponentValue =0;
	}
	
	public FASTProvideSingle(long value) {
		isNull = false;
		longValue = value;
		intValue = 0;
		bytesValue = null;
		charSequenceValue = null;
		manissaValue = 0;
		exponentValue =0;
	}
	
	public FASTProvideSingle(int value) {
		isNull = false;
		longValue = value;
		intValue = value;
		bytesValue = null;
		charSequenceValue = null;
		manissaValue = 0;
		exponentValue =0;
	}
	
	public FASTProvideSingle(byte[] value) {
		isNull = false;
		longValue = 0;
		intValue = 0;
		bytesValue = value;
		charSequenceValue = null;
		manissaValue = 0;
		exponentValue =0;
	}
	
	public FASTProvideSingle(long manissa, int exponent) {
		isNull = false;
		longValue = 0;
		intValue = 0;
		bytesValue = null;
		charSequenceValue = null;
		manissaValue = manissa;
		exponentValue =exponent;
	}
	
	public FASTProvideSingle(CharSequence value) {
		isNull = false;
		longValue = 0;
		intValue = 0;
		bytesValue = null;
		charSequenceValue = value;
		manissaValue = 0;
		exponentValue =0;
	}
	
	public final boolean provideNull(int id) {
		return isNull;
	}

	public final long provideLong(int id) {
		return longValue;
	}

	public final int provideInt(int id) {
		return intValue;
	}

	public final byte[] provideBytes(int id) {
		return bytesValue;
	}

	public final CharSequence provideCharSequence(int id) {
		return charSequenceValue;
	}

	public final void provideDecimal(int id, DecimalDTO target) {
		target.mantissa = manissaValue;
		target.exponent = exponentValue;
	}

	@Override
	public void beginGroup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void endGroup() {
		// TODO Auto-generated method stub
		
	}

}
