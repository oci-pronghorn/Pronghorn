//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;

public class FieldReaderDecimal {
	
	private PrimitiveReader reader;
	private final FieldReaderInteger exponent;
	private final FieldReaderLong mantissa;
	private int readFromIdx = -1;	
	
	public FieldReaderDecimal(PrimitiveReader reader, int[] decimalExponentDictionary, long[] decimalMantissaDictionary) {
		this.reader = reader;
		this.exponent = new FieldReaderInteger(reader, decimalExponentDictionary);
		this.mantissa = new FieldReaderLong(reader, decimalMantissaDictionary);
	}

	//TODO: Optional absent null is not implemented yet for Decimal type.
	
	public void reset(DictionaryFactory df) {
		df.reset(exponent.lastValue,mantissa.lastValue);
	}	
	public void copyExponent(int sourceToken, int targetToken) {
		exponent.copy(sourceToken, targetToken);
	}
	public void copyMantissa(int sourceToken, int targetToken) {
		mantissa.copy(sourceToken, targetToken);
	}
	
	public int readDecimalExponentOptional(int token) {
		
		//oppExp
				if (0==(token&(1<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
					//none, constant, delta
					if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
						//none, delta
						if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
							//none
							return exponent.readIntegerSignedOptional(token,readFromIdx );
						} else {
							//delta
							return exponent.readIntegerSignedDeltaOptional(token,readFromIdx );
						}	
					} else {
						//constant
						return exponent.readIntegerSignedConstantOptional(token,readFromIdx );
					}
					
				} else {
					//copy, default, increment
					if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
						//copy, increment
						if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
							//copy
							return exponent.readIntegerSignedCopyOptional(token,readFromIdx );
						} else {
							//increment
							return exponent.readIntegerSignedIncrementOptional(token,readFromIdx );
						}	
					} else {
						// default
						return exponent.readIntegerSignedDefaultOptional(token,readFromIdx );
					}		
				}
				
	}

	public int readDecimalExponent(int token) {
		
		//oppExp
		if (0==(token&(1<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
			//none, constant, delta
			if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
				//none, delta
				if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
					//none
					return exponent.readIntegerSigned(token,readFromIdx);
				} else {
					//delta
					return exponent.readIntegerSignedDelta(token,readFromIdx);
				}	
			} else {
				//constant
				return exponent.readIntegerSignedConstant(token,readFromIdx);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
				//copy, increment
				if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
					//copy
					return exponent.readIntegerSignedCopy(token,readFromIdx);
				} else {
					//increment
					return exponent.readIntegerSignedIncrement(token,readFromIdx);
				}	
			} else {
				// default
				return exponent.readIntegerSignedDefault(token,readFromIdx);
			}		
		}
		
	}

	public long readDecimalMantissaOptional(int token) {
		//oppMaint
				if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
					//none, constant, delta
					if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
						//none, delta
						if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
							//none
							return mantissa.readLongSignedOptional(token );
						} else {
							//delta
							return mantissa.readLongSignedDeltaOptional(token );
						}	
					} else {
						//constant
						return mantissa.readLongSignedConstantOptional(token );
					}
					
				} else {
					//copy, default, increment
					if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
						//copy, increment
						if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
							//copy
							return mantissa.readLongSignedCopyOptional(token );
						} else {
							//increment
							return mantissa.readLongSignedIncrementOptional(token );
						}	
					} else {
						// default
						return mantissa.readLongSignedDefaultOptional(token );
					}		
				}
		
	}

	public long readDecimalMantissa(int token) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return mantissa.readLongSigned(token);
				} else {
					//delta
					return mantissa.readLongSignedDelta(token);
				}	
			} else {
				//constant
				return mantissa.readLongSignedConstant(token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return mantissa.readLongSignedCopy(token);
				} else {
					//increment
					return mantissa.readLongSignedIncrement(token);
				}	
			} else {
				// default
				return mantissa.readLongSignedDefault(token);
			}		
		}
		
		
	}

	public void setReadFrom(int readFromIdx) {
		// TODO Auto-generated method stub
		
	}



}
