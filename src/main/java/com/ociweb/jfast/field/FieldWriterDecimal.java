//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FieldWriterDecimal {

	private final FieldWriterInteger writerDecimalExponent;
	private final FieldWriterLong writerDecimalMantissa;
	
	public FieldWriterDecimal(PrimitiveWriter writer, int[] exponentValues, long[] mantissaValues) {
		writerDecimalExponent = new FieldWriterInteger(writer, exponentValues);
		writerDecimalMantissa = new FieldWriterLong(writer, mantissaValues);
	}


	public void writeDecimalNone(int token, int exponent, long mantissa) {
		
		writerDecimalExponent.writeIntegerSigned(exponent, token);
		writerDecimalMantissa.writeLongSigned(mantissa, token);
		
	}

	public void reset(DictionaryFactory df) {
		df.reset(writerDecimalExponent.lastValue,writerDecimalMantissa.lastValue);
	}	
	public void copyExponent(int sourceToken, int targetToken) {
		writerDecimalExponent.copy(sourceToken, targetToken);
	}
	public void copyMantissa(int sourceToken, int targetToken) {
		writerDecimalMantissa.copy(sourceToken, targetToken);
	}
	
	public void writeDecimalOptional(int token, int exponent, long mantissa) {
		
		writeExponentOptional(token, exponent);
		writeMantissaOptional(token, mantissa);
		
	}


	private void writeMantissaOptional(int token, long mantissa) {
		//oppMaint
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					writerDecimalMantissa.writeLongSigned(1+mantissa, token);
				} else {
					//delta
					writerDecimalMantissa.writeLongSignedDeltaOptional(mantissa, token);
				}	
			} else {
				//constant
				writerDecimalMantissa.writeLongSignedConstantOptional(mantissa, token);
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


	private void writeExponentOptional(int token, int exponent) {
		//oppExp
		if (0==(token&(1<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
			//none, constant, delta
			if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
				//none, delta
				if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
					//none
					writerDecimalExponent.writeIntegerSigned(1+exponent, token);
				} else {
					//delta
					writerDecimalExponent.writeIntegerSignedDeltaOptional(exponent, token);
				}	
			} else {
				//constant
				writerDecimalExponent.writeIntegerSignedConstantOptional(exponent, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
				//copy, increment
				if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
					//copy
					writerDecimalExponent.writeIntegerSignedCopyOptional(exponent, token);
				} else {
					//increment
					writerDecimalExponent.writeIntegerSignedIncrementOptional(exponent, token);
				}	
			} else {
				// default
				writerDecimalExponent.writeIntegerSignedDefaultOptional(exponent, token);
			}		
		}
	}

	//remove two opp arguments!
	public void writeDecimal(int token, int exponent, long mantissa) {
		
		writeExponent(token, exponent);
		writeMantissa(token, mantissa);
			
	}


	private void writeMantissa(int token, long mantissa) {
		//oppMaint
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					writerDecimalMantissa.writeLongSigned(mantissa, token);
				} else {
					//delta
					writerDecimalMantissa.writeLongSignedDelta(mantissa, token);
				}	
			} else {
				//constant
				writerDecimalMantissa.writeLongSignedConstant(mantissa, token);
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


	private void writeExponent(int token, int exponent) {
		//oppExp
		if (0==(token&(1<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
			//none, constant, delta
			if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
				//none, delta
				if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
					//none
					writerDecimalExponent.writeIntegerSigned(exponent, token);
				} else {
					//delta
					writerDecimalExponent.writeIntegerSignedDelta(exponent, token);
				}	
			} else {
				//constant
				writerDecimalExponent.writeIntegerSignedConstant(exponent, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
				//copy, increment
				if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
					//copy
					writerDecimalExponent.writeIntegerSignedCopy(exponent, token);
				} else {
					//increment
					writerDecimalExponent.writeIntegerSignedIncrement(exponent, token);
				}	
			} else {
				// default
				writerDecimalExponent.writeIntegerSignedDefault(exponent, token);
			}		
		}
	}

	public void writeNull(int token) {
		writerDecimalExponent.writeNull(token); //TODO: this is not done yet
		writerDecimalMantissa.writeNull(token);
	}
}
