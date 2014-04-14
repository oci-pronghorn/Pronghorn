//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldWriterInteger {
	
	
	//for optional fields it is still in the optional format so 
	//zero represents null for those fields.  
	public final int[] dictionary;
	public final int[] init;
	final PrimitiveWriter writer;
	public final int INSTANCE_MASK;
	
	public FieldWriterInteger(PrimitiveWriter writer, int[] values, int[] init) {
		assert(values.length<TokenBuilder.MAX_INSTANCE);
		assert(TokenBuilder.isPowerOfTwo(values.length));
		
		this.INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (values.length-1));
		this.writer = writer;
		this.dictionary = values;
		this.init = init;
	}
	
	
	
	
	public void writeNull(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
			if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
				//None and Delta (both do not use pmap)
				dictionary[idx] = 0;
				writer.writeNull();              //no pmap, yes change to last value
			} else {
				//Copy and Increment
				
				if (dictionary[idx]==0) { //stored value was null;
					writer.writePMapBit((byte)0);
				} else {
					dictionary[idx] =0;
					writer.writePMapBit((byte)1);
					writer.writeNull();
				}  //yes pmap, yes change to last value	
			}
		} else {
			if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
				assert(0!=(token&(1<<TokenBuilder.SHIFT_TYPE))) :"Sending a null constant is not supported";
				//const optional
				writer.writePMapBit((byte)0);       //pmap only
			} else {	
				//default
				
				if (dictionary[idx]==0) { //stored value was null;
					writer.writePMapBit((byte)0);
				} else {
					writer.writePMapBit((byte)1);
					writer.writeNull();
				}  //yes pmap,  no change to last value
			}	
		}
		
	}
	
}
