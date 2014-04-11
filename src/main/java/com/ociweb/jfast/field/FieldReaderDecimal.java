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

	//TODO: C, Optional absent null is not implemented yet for Decimal type.
	
	public void reset(DictionaryFactory df) {
		df.reset(exponent.dictionary,mantissa.dictionary);
	}	

	
	public int readDecimalExponentOptional(int token, int readFromIdx2) {
		
		//oppExp
				if (0==(token&(1<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
					//none, constant, delta
					if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
						//none, delta
						if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
							//none
							int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
							
							return exponent.reader.readIntegerSignedOptional(constAbsent);
						} else {
							//delta
							int target = token&exponent.MAX_INT_INSTANCE_MASK;
							int source = readFromIdx>0? readFromIdx&exponent.MAX_INT_INSTANCE_MASK : target;
							int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
							
							return exponent.reader.readIntegerSignedDeltaOptional(target, source, exponent.dictionary, constAbsent);
						}	
					} else {
						//constant
						int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
						int constConst = exponent.dictionary[token & exponent.MAX_INT_INSTANCE_MASK];
						
						return exponent.reader.readIntegerSignedConstantOptional(constAbsent, constConst);
					}
					
				} else {
					//copy, default, increment
					if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
						//copy, increment
						if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
							//copy
							int target = token&exponent.MAX_INT_INSTANCE_MASK;
							int source = readFromIdx>0? readFromIdx&exponent.MAX_INT_INSTANCE_MASK : target;
							int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
							
							int value = exponent.reader.readIntegerSignedCopy(target, source, exponent.dictionary);
							return (0 == value ? constAbsent: (value>0 ? value-1 : value));
						} else {
							//increment
							int target = token&exponent.MAX_INT_INSTANCE_MASK;
							int source = readFromIdx>0? readFromIdx&exponent.MAX_INT_INSTANCE_MASK : target;
							int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
							
							return exponent.reader.readIntegerSignedIncrementOptional(target, source, exponent.dictionary, constAbsent);
						}	
					} else {
						// default
						int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
						int constDefault = exponent.dictionary[token & exponent.MAX_INT_INSTANCE_MASK]==0?constAbsent:exponent.dictionary[token & exponent.MAX_INT_INSTANCE_MASK];
								
						return exponent.reader.readIntegerSignedDefaultOptional(constDefault, constAbsent);
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
					//no need to set initValueFlags for field that can never be null
					return exponent.reader.readIntegerSigned();
				} else {
					//delta
					int target = token&exponent.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&exponent.MAX_INT_INSTANCE_MASK : target;
					
					return exponent.reader.readIntegerSignedDelta(target, source, exponent.dictionary);
				}	
			} else {
				//constant
				//always return this required value.
				return exponent.dictionary[token & exponent.MAX_INT_INSTANCE_MASK];
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
				//copy, increment
				if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
					//copy
					int target = token&exponent.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&exponent.MAX_INT_INSTANCE_MASK : target;
					
					return exponent.reader.readIntegerSignedCopy(target, source, exponent.dictionary);
				} else {
					//increment
					int target = token&exponent.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&exponent.MAX_INT_INSTANCE_MASK : target;
					
					return exponent.reader.readIntegerSignedIncrement(target, source, exponent.dictionary);
				}	
			} else {
				// default
				int constDefault = exponent.dictionary[token & exponent.MAX_INT_INSTANCE_MASK];	
				
				return exponent.reader.readIntegerSignedDefault(constDefault);
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
							long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
							
							return mantissa.reader.readLongSignedOptional(constAbsent);
						} else {
							//delta
							int target = token&mantissa.MAX_LONG_INSTANCE_MASK;
							int source = readFromIdx>0? readFromIdx&mantissa.MAX_LONG_INSTANCE_MASK : target;
							long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
							
							return mantissa.reader.readLongSignedDeltaOptional(target, source, mantissa.dictionary, constAbsent);
						}	
					} else {
						//constant
						long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
						long constConst = mantissa.dictionary[token & mantissa.MAX_LONG_INSTANCE_MASK];
						
						return mantissa.reader.readLongSignedConstantOptional(constAbsent, constConst);
					}
					
				} else {
					//copy, default, increment
					if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
						//copy, increment
						if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
							//copy
							int target = token&mantissa.MAX_LONG_INSTANCE_MASK;
							int source = readFromIdx>0? readFromIdx&mantissa.MAX_LONG_INSTANCE_MASK : target;
							long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
									
							long value = mantissa.reader.readLongSignedCopy(target, source, mantissa.dictionary);
							return (0 == value ? constAbsent: value-1);
						} else {
							//increment
							int target = token&mantissa.MAX_LONG_INSTANCE_MASK;
							int source = readFromIdx>0? readFromIdx&mantissa.MAX_LONG_INSTANCE_MASK : target;
							long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
							
							return mantissa.reader.readLongSignedIncrementOptional(target, source, mantissa.dictionary, constAbsent);
						}	
					} else {
						// default
						long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
						long constDefault = mantissa.dictionary[token & mantissa.MAX_LONG_INSTANCE_MASK]==0?constAbsent:mantissa.dictionary[token & mantissa.MAX_LONG_INSTANCE_MASK];
						
						return mantissa.reader.readLongSignedDefaultOptional(constDefault, constAbsent);
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
					int target = token&mantissa.MAX_LONG_INSTANCE_MASK;
					
					return mantissa.reader.readLongSigned(target, mantissa.dictionary);
				} else {
					//delta
					int target = token&mantissa.MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&mantissa.MAX_LONG_INSTANCE_MASK : target;
					
					
					return mantissa.reader.readLongSignedDelta(target, source, mantissa.dictionary);
				}	
			} else {
				//constant
				//always return this required value.
				return mantissa.dictionary[token & mantissa.MAX_LONG_INSTANCE_MASK];
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token&mantissa.MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&mantissa.MAX_LONG_INSTANCE_MASK : target;
					
					return mantissa.reader.readLongSignedCopy(target, source, mantissa.dictionary);
				} else {
					//increment
					int target = token&mantissa.MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&mantissa.MAX_LONG_INSTANCE_MASK : target;
					
					
					return mantissa.reader.readLongSignedIncrement(target, source, mantissa.dictionary);
				}	
			} else {
				// default
				long constDefault = mantissa.dictionary[token & mantissa.MAX_LONG_INSTANCE_MASK];
				
				return mantissa.reader.readLongSignedDefault(constDefault);
			}		
		}
		
		
	}



}
