//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FieldWriterDecimal {

	public final FieldWriterInteger writerDecimalExponent;
	public final FieldWriterLong writerDecimalMantissa;
	
	public FieldWriterDecimal(PrimitiveWriter writer, int[] exponentValues, int[] exponentInit, long[] mantissaValues, long[] mantissaInit) {
		writerDecimalExponent = new FieldWriterInteger(writer, exponentValues, exponentInit);
		writerDecimalMantissa = new FieldWriterLong(writer, mantissaValues, mantissaInit);
	}

	public void writeDecimalNone(int token, int exponent, long mantissa) {
		
		int idx = token & writerDecimalExponent.INSTANCE_MASK;
		
		writerDecimalExponent.writer.writeIntegerSigned(writerDecimalExponent.dictionary[idx] = exponent);
		int idx1 = token & writerDecimalMantissa.INSTANCE_MASK;
		
		writerDecimalMantissa.writer.writeLongSigned(writerDecimalMantissa.dictionary[idx1] = mantissa);
		
	}

	public void reset(DictionaryFactory df) {
		df.reset(writerDecimalExponent.dictionary,writerDecimalMantissa.dictionary);
	}	
	public void copyExponent(int sourceToken, int targetToken) {
		writerDecimalExponent.dictionary[targetToken & writerDecimalExponent.INSTANCE_MASK] = writerDecimalExponent.dictionary[sourceToken & writerDecimalExponent.INSTANCE_MASK];
	}
	public void copyMantissa(int sourceToken, int targetToken) {
		writerDecimalMantissa.dictionary[targetToken & writerDecimalMantissa.INSTANCE_MASK] = writerDecimalMantissa.dictionary[sourceToken & writerDecimalMantissa.INSTANCE_MASK];
	}
	
	public void writeMantissaOptional(int token, long mantissa) {
		
		//oppMaint
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					int idx = token & writerDecimalMantissa.INSTANCE_MASK;
					
					writerDecimalMantissa.writer.writeLongSigned(writerDecimalMantissa.dictionary[idx] = 1+mantissa);
				} else {
					//delta
					writerDecimalMantissa.writeLongSignedDeltaOptional(mantissa, token);
				}	
			} else {
				//constant
				assert(writerDecimalMantissa.dictionary[ token & writerDecimalMantissa.INSTANCE_MASK]==mantissa) : "Only the constant value from the template may be sent";
				writerDecimalMantissa.writer.writePMapBit((byte)1);
				//the writeNull will take care of the rest.
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					writerDecimalMantissa.writeLongSignedCopyOptional(mantissa, token);
				} else {
					//increment
					writerDecimalMantissa.writeLongSignedIncrementOptional(mantissa, token);
				}	
			} else {
				// default
				writerDecimalMantissa.writeLongSignedDefaultOptional(mantissa, token);
			}		
		}
	}


	public void writeExponentOptional(int token, int exponent) {
		//oppExp
		if (0==(token&(1<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
			//none, constant, delta
			if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
				//none, delta
				if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
					//none
					int idx = token & writerDecimalExponent.INSTANCE_MASK;
					
					writerDecimalExponent.writer.writeIntegerSigned(writerDecimalExponent.dictionary[idx] = exponent>=0?1+exponent:exponent);
				} else {
					//delta
					int idx = token & writerDecimalExponent.INSTANCE_MASK;
					
					writerDecimalExponent.writer.writeIntegerSignedDeltaOptional(exponent,idx,writerDecimalExponent.dictionary);
				}	
			} else {
				//constant
				assert(writerDecimalExponent.dictionary[ token & writerDecimalExponent.INSTANCE_MASK]==exponent) : "Only the constant value from the template may be sent";
				writerDecimalExponent.writer.writePMapBit((byte)1);
				//the writeNull will take care of the rest.
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
				//copy, increment
				if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
					//copy
					int idx = token & writerDecimalExponent.INSTANCE_MASK;
					
					writerDecimalExponent.writer.writeIntegerSignedCopyOptional(exponent, idx, writerDecimalExponent.dictionary);
				} else {
					//increment
					int idx = token & writerDecimalExponent.INSTANCE_MASK;
					
					writerDecimalExponent.writer.writeIntegerSignedIncrementOptional(exponent, idx, writerDecimalExponent.dictionary);
				}	
			} else {
				// default
				int idx = token & writerDecimalExponent.INSTANCE_MASK;
				int constDefault = writerDecimalExponent.dictionary[idx];
				
				writerDecimalExponent.writer.writeIntegerSignedDefaultOptional(exponent, idx, constDefault);
			}		
		}
	}

	public void writeMantissa(int token, long mantissa) {
		//oppMaint
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					int idx = token & writerDecimalMantissa.INSTANCE_MASK;
					
					writerDecimalMantissa.writer.writeLongSigned(writerDecimalMantissa.dictionary[idx] = mantissa);
				} else {
					//delta
					writerDecimalMantissa.writeLongSignedDelta(mantissa, token);
				}	
			} else {
				//constant
				assert(writerDecimalMantissa.dictionary[ token & writerDecimalMantissa.INSTANCE_MASK]==mantissa) : "Only the constant value from the template may be sent";
				//nothing need be sent because constant does not use pmap and the template
				//on the other receiver side will inject this value from the template
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					writerDecimalMantissa.writeLongSignedCopy(mantissa, token);
				} else {
					//increment
					writerDecimalMantissa.writeLongSignedIncrement(mantissa, token);
				}	
			} else {
				// default
				writerDecimalMantissa.writeLongSignedDefault(mantissa, token);
			}		
		}
	}


	public void writeExponent(int token, int exponent) {
		//oppExp
		if (0==(token&(1<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
			//none, constant, delta
			if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
				//none, delta
				if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
					//none
					int idx = token & writerDecimalExponent.INSTANCE_MASK;
					
					writerDecimalExponent.writer.writeIntegerSigned(writerDecimalExponent.dictionary[idx] = exponent);
				} else {
					//delta
					//Delta opp never uses PMAP
					int idx = token & writerDecimalExponent.INSTANCE_MASK;
					
					writerDecimalExponent.writer.writeIntegerSignedDelta(exponent,idx,writerDecimalExponent.dictionary);
				}	
			} else {
				//constant
				assert(writerDecimalExponent.dictionary[ token & writerDecimalExponent.INSTANCE_MASK]==exponent) : "Only the constant value "+writerDecimalExponent.dictionary[ token & writerDecimalExponent.INSTANCE_MASK]+" from the template may be sent";
				//nothing need be sent because constant does not use pmap and the template
				//on the other receiver side will inject this value from the template
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
				//copy, increment
				if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
					//copy
					int idx = token & writerDecimalExponent.INSTANCE_MASK;
					
					writerDecimalExponent.writer.writeIntegerSignedCopy(exponent, idx, writerDecimalExponent.dictionary);
				} else {
					//increment
					int idx = token & writerDecimalExponent.INSTANCE_MASK;
					
					writerDecimalExponent.writer.writeIntegerSignedIncrement(exponent, idx, writerDecimalExponent.dictionary);
				}	
			} else {
				// default
				int idx = token & writerDecimalExponent.INSTANCE_MASK;
				int constDefault = writerDecimalExponent.dictionary[idx];
				
				writerDecimalExponent.writer.writeIntegerSignedDefault(exponent, idx, constDefault);
			}		
		}
	}

	public void writeNull(int token) {
		writerDecimalExponent.writeNull(token); //TODO: A, must implement null for decimals, this is not done yet
		writerDecimalMantissa.writeNull(token);
	}


	public void reset(int idx) {
		writerDecimalExponent.dictionary[idx] = writerDecimalExponent.init[idx];
		writerDecimalMantissa.dictionary[idx] = writerDecimalMantissa.init[idx];	
	}
}
