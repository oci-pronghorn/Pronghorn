//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveReader;

public final class FieldReaderInteger {

	public final int MAX_INT_INSTANCE_MASK;
	public final PrimitiveReader reader;	
	public final int[]  dictionary;
	public final int[]  init;
   
	public FieldReaderInteger(PrimitiveReader reader, int[] values, int[] init) {

		assert(values.length<TokenBuilder.MAX_INSTANCE);
		assert(TokenBuilder.isPowerOfTwo(values.length));
		
		this.MAX_INT_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (values.length-1));
		this.reader = reader;
		this.dictionary = values;
		this.init = init;
	}



}
