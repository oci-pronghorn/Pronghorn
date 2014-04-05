//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;

public final class FieldReaderDecimal {
	
	private PrimitiveReader reader;
	public final FieldReaderInteger exponent;
	public final FieldReaderLong mantissa;
	private int readFromIdx = -1;	
	
	public FieldReaderDecimal(PrimitiveReader reader, 
			                  int[] decimalExponentDictionary, 
			                  int[] decimalExponentInit,
			                  long[] decimalMantissaDictionary,
			                  long[] decimalMantissaInit) {
		this.reader = reader;
		this.exponent = new FieldReaderInteger(reader, decimalExponentDictionary, decimalExponentInit);
		this.mantissa = new FieldReaderLong(reader, decimalMantissaDictionary, decimalMantissaInit);
	}

	//TODO: Optional absent null is not implemented yet for Decimal type.
	
	public void reset(DictionaryFactory df) {
		df.reset(exponent.dictionary,mantissa.lastValue);
	}	

	
	public int readDecimalExponentOptional(int token, int readFromIdx2) {
		
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

	public final int readDecimalExponent(int token, int readFromIdx2) {
		
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

	public long readDecimalMantissaOptional(int token, int readFromIdx2) {
		//oppMaint
				if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
					//none, constant, delta
					if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
						//none, delta
						if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
							//none
							return mantissa.readLongSignedOptional(token, readFromIdx);
						} else {
							//delta
							return mantissa.readLongSignedDeltaOptional(token, readFromIdx);
						}	
					} else {
						//constant
						return mantissa.readLongSignedConstantOptional(token, readFromIdx);
					}
					
				} else {
					//copy, default, increment
					if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
						//copy, increment
						if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
							//copy
							return mantissa.readLongSignedCopyOptional(token, readFromIdx);
						} else {
							//increment
							return mantissa.readLongSignedIncrementOptional(token, readFromIdx);
						}	
					} else {
						// default
						return mantissa.readLongSignedDefaultOptional(token, readFromIdx);
					}		
				}
		
	}

	public long readDecimalMantissa(int token, int readFromIdx2) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return mantissa.readLongSigned(token, readFromIdx);
				} else {
					//delta
					return mantissa.readLongSignedDelta(token, readFromIdx);
				}	
			} else {
				//constant
				return mantissa.readLongSignedConstant(token, readFromIdx);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return mantissa.readLongSignedCopy(token, readFromIdx);
				} else {
					//increment
					return mantissa.readLongSignedIncrement(token, readFromIdx);
				}	
			} else {
				// default
				return mantissa.readLongSignedDefault(token, readFromIdx);
			}		
		}
		
		
	}

	public void reset(int idx) {
		exponent.reset(idx);
		mantissa.reset(idx);		
	}



}
