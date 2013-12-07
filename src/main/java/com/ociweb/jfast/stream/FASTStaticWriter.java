package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.FieldWriterBytes;
import com.ociweb.jfast.field.FieldWriterChar;
import com.ociweb.jfast.field.FieldWriterDecimal;
import com.ociweb.jfast.field.FieldWriterInteger;
import com.ociweb.jfast.field.FieldWriterLong;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveWriter;

//May drop interface if this causes a performance problem from virtual table 
public final class FASTStaticWriter implements FASTWriter {
	
	//TODO: add assert at beginning of every method that calls a FASTAccept
	//       object which is only created from the template when assert is on
	//       this will validate each FieldId/Token is the expected one in that order.
	
	private final PrimitiveWriter writer;
	
	
	//TODO: each of these instances represent a specific dictionary.
	
	
	private final FieldWriterInteger writerInteger;
	private final FieldWriterLong writerLong;
	
	private final FieldWriterInteger writerDecimalExponent;
	private final FieldWriterLong writerDecimalMantissa;
	
	private final FieldWriterDecimal writerDecimal;
	private final FieldWriterChar writerChar;
	private final FieldWriterBytes writerBytes;
		
	
	private final int[] tokenLookup; //array of tokens as field id locations
	
	
	private final int MASK_TYPE = 0x7F;
	private final int SHIFT_TYPE = 24;
	
	private final int MASK_OPER = 0x0F;
	private final int SHIFT_OPER = 20;
	
	private final int MASK_PMAP_MAX = 0x7FF;
	private final int SHIFT_PMAP_MASK = 20;
	
	//TODO: need to add dictionary id
	
	//TODO: these can all be run together with variable lengths
	//each field has a unique string id and the class for generating the ids will
	//have the scopes needed to know if a new instance or old one should be used.
	//TODO: What is the unqie id is it name or id?
	//TODO: How is this converted into simple field id of int type?
	
	// 
	//
	
	//////////////// 32 bits total ////////////////////////////////////////////
	//  1 bit, High bit is always set to denote this as a token vs fieldId   //
	//    Regular fields               #    Field Groups (need maxPMap bytes)
	//  6 bit type                     #               
	//  1 bit optional field **        #      
	//  4 bit operation                #      11 bit PMap max bytes (2048 max or zero)       )
	// 20 bit instance(1M fields/type) #      20 bit sequence length (1M max or reference)  
	//
	// ** this is read as the odd flag inside the type so all 7 bits are read
	//    together, this same mapping applies to the group.

	
	public FASTStaticWriter(PrimitiveWriter writer, DictionaryFactory dcr, int[] tokenLookup) {
		//TODO: must set the initial values for default/constants from the template here.
		//TODO: perhaps the arrays should be allocated external so template parser can manage it?
		
		this.writer = writer;
		this.tokenLookup = tokenLookup;
		
		this.writerInteger 			= new FieldWriterInteger(writer, dcr.integerDictionary());
		this.writerLong    			= new FieldWriterLong(writer,dcr.longDictionary());
		//decimal does the same as above but both parts work together for each whole value
		//TODO: it may be a better design to only use this for the singles and put the twins in the above structure?
		this.writerDecimalExponent = new FieldWriterInteger(writer, dcr.decimalExponentDictionary());
		this.writerDecimalMantissa = new FieldWriterLong(writer,dcr.decimalMantissaDictionary());
		//
		this.writerChar = null;
		this.writerBytes = null;
		this.writerDecimal = null;
		//TODO: add the Text and Bytes
		
		
	}
	
	/**
	 * Write null value, must only be used if the field id is one
	 * of optional type.
	 */
	@Override
	public void write(int id) {
		
		
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.IntegerUnsignedOptional:
				writeIntegerUnsignedOptional(token);				
			break;
			case TypeMask.IntegerSignedOptional:
				writeIntegerSignedOptional(token);				
			break;
			case TypeMask.LongUnsignedOptional:
				writeLongUnsignedOptional(token);				
			break;	
			case TypeMask.LongSignedOptional:
			    writeLongUnsignedOptional(token);				
			break;		
			case TypeMask.DecimalSingleOptional:
			case TypeMask.DecimalTwinOptional:
			case TypeMask.TextASCIIOptional:
			case TypeMask.TextUTF8Optional:
				writer.writeNull();
				break;
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
		
		
		

	}

	private void writeLongUnsignedOptional(int token) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
		case OperatorMask.Increment:
		case OperatorMask.Copy:
			writerLong.writeLongNullPMap(token,(byte)1);//sets 1
			break;
		case OperatorMask.None: //no pmap
			writerLong.writeLongNull(token);
			break;
		case OperatorMask.Delta:			
			writer.writePMapBit((byte)0);
			break;
		case OperatorMask.Default: //does not change dictionary	
		case OperatorMask.Constant:
			writer.writeNull();
			break;
		default:
			//constant can not be optional and will throw
			throw new UnsupportedOperationException();
}
	}

	private void writeIntegerSignedOptional(int token) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.Increment:
				writerInteger.writeIntegerSignedIncrementOptional(token);
				break;
			case OperatorMask.Copy:
				writerInteger.writeIntegerNullPMap(token,(byte)1);//sets 1
				break;
			case OperatorMask.None: //no pmap
				writerInteger.writeIntegerNull(token);
				break;
			case OperatorMask.Delta:
				writer.writePMapBit((byte)0);
				break;
			case OperatorMask.Default: //does not change dictionary	
				writerInteger.writeIntegerUnsignedDefaultOptional(token);
				break;
			case OperatorMask.Constant:
				writer.writeNull();
				break;
			default:
				//constant can not be optional and will throw
				throw new UnsupportedOperationException();
		}
	}

	private void writeIntegerUnsignedOptional(int token) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None: //no pmap
				writerInteger.writeIntegerNull(token);
				break;
			case OperatorMask.Copy:
				writerInteger.writeIntegerNullPMap(token,(byte)1);//sets 1
				break;
			case OperatorMask.Constant:
				writer.writeNull();
				break;
			case OperatorMask.Default: //does not change dictionary	
				writerInteger.writeIntegerSignedDefaultOptional(token);
				break;
			case OperatorMask.Delta:
				writerInteger.writeIntegerUnsignedDeltaOptional(token);
				break;
			case OperatorMask.Increment:
				writerInteger.writeIntegerUnsignedIncrementOptional(token);
				break;
			default:
				//constant can not be optional and will throw
				throw new UnsupportedOperationException();
		}
	}
	
	/**
	 * Method for writing signed unsigned and/or optional longs.
	 * To write the "null" or absence of a value use 
	 *    void write(int id) 
	 */
	@Override
	public void write(int id, long value) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.LongUnsigned:
				acceptLongUnsigned(token, value);
				break;
			case TypeMask.LongUnsignedOptional:
				acceptLongUnsignedOptional(token, value);
				break;
			case TypeMask.LongSigned:
				acceptLongSigned(token, value);
				break;
			case TypeMask.LongSignedOptional:
				acceptLongSignedOptional(token, value);
				break;
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}

	private void acceptLongSignedOptional(int token, long value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeLongSignedOptional(value);
				break;
			case OperatorMask.Copy:
				writerLong.writeLongSignedCopyOptional(value, token);
				break;
			case OperatorMask.Delta:
				writerLong.writeLongSignedDeltaOptional(value, token);
				break;	
			case OperatorMask.Increment:
				writerLong.writeLongSignedIncrementOptional(value, token);
				break;
			case OperatorMask.Default:
				writerLong.writeLongSignedDefaultOptional(value, token);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptLongSigned(int token, long value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeLongSigned(value);
				break;
			case OperatorMask.Constant:
				writerLong.writeLongSignedConstant(value, token);
				break;
			case OperatorMask.Copy:
				writerLong.writeLongSignedCopy(value, token);
				break;
			case OperatorMask.Delta:
				writerLong.writeLongSignedDelta(value, token);
				break;	
			case OperatorMask.Increment:
				writerLong.writeLongSignedIncrement(value, token);
				break;
			case OperatorMask.Default:
				writerLong.writeLongSignedDefault(value, token);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptLongUnsignedOptional(int token, long value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeLongUnsigned(value+1);//should be in writerLong
				break;
			case OperatorMask.Copy:
				writerLong.writeLongUnsignedCopyOptional(value, token);
				break;
			case OperatorMask.Delta:
				writerLong.writeLongUnsignedDeltaOptional(value, token);
				break;	
			case OperatorMask.Increment:
				writerLong.writeLongUnsignedIncrementOptional(value, token);
				break;
			case OperatorMask.Default:
				writerLong.writeLongUnsignedDefaultOptional(value, token);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptLongUnsigned(int token, long value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeLongUnsigned(value);
				break;
			case OperatorMask.Constant:
				writerLong.writeLongUnsignedConstant(value, token);
				break;
			case OperatorMask.Copy:
				writerLong.writeLongUnsignedCopy(value, token);
				break;
			case OperatorMask.Delta:
				writerLong.writeLongUnsignedDelta(value, token);
				break;	
			case OperatorMask.Increment:
				writerLong.writeLongUnsignedIncrement(value, token);
				break;
			case OperatorMask.Default:
				writerLong.writeLongUnsignedDefault(value, token);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	/**
	 * Method for writing signed unsigned and/or optional integers.
	 * To write the "null" or absence of an integer use 
	 *    void write(int id) 
	 */
	@Override
	public void write(int id, int value) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.IntegerUnsigned:
				acceptIntegerUnsigned(token, value);
				break;
			case TypeMask.IntegerUnsignedOptional:
				acceptIntegerUnsignedOptional(token, value);
				break;
			case TypeMask.IntegerSigned:
				acceptIntegerSigned(token, value);
				break;
			case TypeMask.IntegerSignedOptional:
				acceptIntegerSignedOptional(token, value);
				break;
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}
	
	private void acceptIntegerSigned(int token, int value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
		case OperatorMask.None:
			writer.writeIntegerSigned(value);
			break;
		case OperatorMask.Constant:
			writerInteger.writeIntegerSignedConstant(value, token);
			break;
		case OperatorMask.Copy:
			writerInteger.writeIntegerSignedCopy(value, token);
			break;
		case OperatorMask.Delta:
			writerInteger.writeIntegerSignedDelta(value, token);
			break;	
		case OperatorMask.Increment:
			writerInteger.writeIntegerSignedIncrement(value, token);
			break;
		case OperatorMask.Default:
			writerInteger.writeIntegerSignedDefault(value, token);
			break;
		default:
			throw new UnsupportedOperationException();
		}
	}
	
	private void acceptIntegerUnsigned(int token, int value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeIntegerUnsigned(value);
				break;
			case OperatorMask.Constant:
				writerInteger.writeIntegerUnsignedConstant(value, token);
				break;
			case OperatorMask.Copy:
				writerInteger.writeIntegerUnsignedCopy(value, token);
				break;
			case OperatorMask.Delta:
				writerInteger.writeIntegerUnsignedDelta(value, token);
				break;	
			case OperatorMask.Increment:
				writerInteger.writeIntegerUnsignedIncrement(value, token);
				break;
			case OperatorMask.Default:
				writerInteger.writeIntegerUnsignedDefault(value, token);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptIntegerSignedOptional(int token, int value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeIntegerSignedOptional(value);
				break;
			case OperatorMask.Copy:
				writerInteger.writeIntegerSignedCopyOptional(value, token);
				break;
			case OperatorMask.Delta:
				writerInteger.writeIntegerSignedDeltaOptional(value, token);
				break;	
			case OperatorMask.Increment:
				writerInteger.writeIntegerSignedIncrementOptional(value, token);
				break;
			case OperatorMask.Default:
				writerInteger.writeIntegerSignedDefaultOptional(value, token);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}
	
	private void acceptIntegerUnsignedOptional(int token, int value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeIntegerUnsigned(value+1);//should be in writerInteger
				break;
			case OperatorMask.Copy:
				writerInteger.writeIntegerUnsignedCopyOptional(value, token);
				break;
			case OperatorMask.Delta:
				writerInteger.writeIntegerUnsignedDeltaOptional(value, token);
				break;	
			case OperatorMask.Increment:
				writerInteger.writeIntegerUnsignedIncrementOptional(value, token);
				break;
			case OperatorMask.Default:
				writerInteger.writeIntegerUnsignedDefaultOptional(value, token);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}
	
	/**
	 * Method for writing decimals required or optional.
	 * To write the "null" or absence of a value use 
	 *    void write(int id) 
	 */
	@Override
	public void write(int id, int exponent, long mantissa) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			
		    case TypeMask.DecimalSingle:
				acceptDecimal(token, exponent, mantissa);
				break;
			case TypeMask.DecimalSingleOptional:
				acceptDecimalOptional(token, exponent, mantissa);
				break;
				
				
			case TypeMask.DecimalTwin:
				acceptDecimal(token, exponent, mantissa);
				break;
			case TypeMask.DecimalTwinOptional:
				acceptDecimalOptional(token, exponent, mantissa);
				break;
				
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}

	private void acceptDecimalOptional(int token, int exponent, long mantissa) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
	//			break;
			case OperatorMask.Constant:
				throw new UnsupportedOperationException();
	//			break;
			case OperatorMask.Copy:
				throw new UnsupportedOperationException();
	//			break;
			case OperatorMask.Delta:
				throw new UnsupportedOperationException();
	//			break;	
			case OperatorMask.Increment:
				throw new UnsupportedOperationException();
	//			break;
			case OperatorMask.Default:
				throw new UnsupportedOperationException();
	//			break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptDecimal(int token, int exponent, long mantissa) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				
				//this.writerDecimalExponent.write
				
				
				throw new UnsupportedOperationException();
	//			break;
			case OperatorMask.Constant:
				
				//possible way to  deal with single operation?
				this.writerDecimalExponent.writeIntegerSignedConstant(exponent, token);
				this.writerDecimalMantissa.writeLongSignedConstant(mantissa, token);
			
				
	//			throw new UnsupportedOperationException();
				break;
			case OperatorMask.Copy:
				throw new UnsupportedOperationException();
	//			break;
			case OperatorMask.Delta:
				throw new UnsupportedOperationException();
	//			break;	
			case OperatorMask.Increment:
				throw new UnsupportedOperationException();
	//			break;
			case OperatorMask.Default:
				throw new UnsupportedOperationException();
	//			break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void write(int id, byte[] value, int offset, int length) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.ByteArray:
				acceptByteArray(token, value, offset, length);
				break;
			case TypeMask.ByteArrayOptional:
				acceptByteArrayOptional(token, value, offset, length);
				break;
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}

	private void acceptByteArrayOptional(int token, byte[] value, int offset, int length) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptByteArray(int token, byte[] value, int offset, int length) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void write(int id, ByteBuffer buffer) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.ByteArray: 
				acceptByteBuffer(token, buffer);
				break;
			case TypeMask.ByteArrayOptional:
				acceptByteBufferOptional(token, buffer);
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}


	private void acceptByteBufferOptional(int token, ByteBuffer buffer) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptByteBuffer(int token, ByteBuffer buffer) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void write(int id, CharSequence value) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.TextASCII: 
				acceptCharSequenceASCII(token, value);
				break;
			case TypeMask.TextASCIIOptional:
				acceptCharSequenceASCIIOptional(token, value);
				break;
			case TypeMask.TextUTF8: 
				acceptCharSequenceUTF8(token, value);
				break;
			case TypeMask.TextUTF8Optional:
				acceptCharSequenceUTF8Optional(token, value);
				break;
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}
	

	private void acceptCharSequenceUTF8Optional(int token, CharSequence value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeIntegerUnsigned(value.length()+1);
				writer.writeTextUTF(value);
				break;
			case OperatorMask.Copy:
				break;
			case OperatorMask.Constant:
				break;
			case OperatorMask.Default:
				break;
			case OperatorMask.Delta:
				break;	
			case OperatorMask.Increment:
				break;
			case OperatorMask.Tail:
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharSequenceUTF8(int token, CharSequence value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeIntegerUnsigned(value.length());
				writer.writeTextUTF(value);
				break;
			case OperatorMask.Copy:
				break;
			case OperatorMask.Constant:
				break;
			case OperatorMask.Default:
				break;
			case OperatorMask.Delta:
				break;	
			case OperatorMask.Increment:
				break;
			case OperatorMask.Tail:
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharSequenceASCIIOptional(int token, CharSequence value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeTextASCII(value);
				break;
			case OperatorMask.Copy:
				break;
			case OperatorMask.Constant:
				break;
			case OperatorMask.Default:
				break;
			case OperatorMask.Delta:
				break;	
			case OperatorMask.Increment:
				break;
			case OperatorMask.Tail:
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharSequenceASCII(int token, CharSequence value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeTextASCII(value);
				break;
			case OperatorMask.Copy:
				break;
			case OperatorMask.Constant:
				break;
			case OperatorMask.Default:
				break;
			case OperatorMask.Delta:
				break;	
			case OperatorMask.Increment:
				break;
			case OperatorMask.Tail:
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void write(int id, char[] value, int offset, int length) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.TextASCII: 
				acceptCharArrayASCII(token,value,offset,length);
				break;
			case TypeMask.TextASCIIOptional:
				acceptCharArrayASCIIOptional(token,value,offset,length);
				break;
			case TypeMask.TextUTF8: 
				acceptCharArrayUTF8(token,value,offset,length);
				break;
			case TypeMask.TextUTF8Optional:
				acceptCharArrayUTF8Optional(token,value,offset,length);
				break;
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}



	private void acceptCharArrayUTF8Optional(int token, char[] value, int offset, int length) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeIntegerUnsigned(length+1);
				writer.writeTextUTF(value,offset,length);
				break;
			case OperatorMask.Copy:
				break;
			case OperatorMask.Constant:
				break;
			case OperatorMask.Default:
				break;
			case OperatorMask.Delta:
				break;	
			case OperatorMask.Increment:
				break;
			case OperatorMask.Tail:
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharArrayUTF8(int token, char[] value, int offset, int length) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeIntegerUnsigned(length);
				writer.writeTextUTF(value,offset,length);
				break;
			case OperatorMask.Copy:
				break;
			case OperatorMask.Constant:
				break;
			case OperatorMask.Default:
				break;
			case OperatorMask.Delta:
				break;	
			case OperatorMask.Increment:
				break;
			case OperatorMask.Tail:
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharArrayASCIIOptional(int token, char[] value, int offset, int length) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeTextASCII(value,offset,length);
				break;
			case OperatorMask.Copy:
				break;
			case OperatorMask.Constant:
				break;
			case OperatorMask.Default:
				break;
			case OperatorMask.Delta:
				break;	
			case OperatorMask.Increment:
				break;
			case OperatorMask.Tail:
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharArrayASCII(int token, char[] value, int offset, int length) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeTextASCII(value,offset,length);
				break;
			case OperatorMask.Copy:
				break;
			case OperatorMask.Constant:
				break;
			case OperatorMask.Default:
				break;
			case OperatorMask.Delta:
				break;	
			case OperatorMask.Increment:
				break;
			case OperatorMask.Tail:
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void openGroup(int id) {	
		int token = id>=0 ? tokenLookup[id] : id;
		
		//TODO: do we need a two more open group methods for dynamic template ids?
		
		
		//int repeat = 
		//if sequence is not set by writer must use sequence provided
	    //0 equals 1	
		
		writer.openPMap(MASK_PMAP_MAX&(token>>SHIFT_PMAP_MASK));
	}
	
	@Override
	public void openGroup(int id, int repeat) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		//repeat count provided
		
		writer.openPMap(MASK_PMAP_MAX&(token>>SHIFT_PMAP_MASK));
				
		//TODO: is this the point when we write the repeat?
		
	}

	@Override
	public void closeGroup() {
		writer.closePMap();
	}

	@Override
	public void flush() {
		writer.flush();
	}

	public boolean isGroupOpen() {//TODO: is this feature really needed?
		return writer.isPMapOpen();
	}

	
	public void reset(DictionaryFactory df) {
		//reset all values to unset
		writerInteger.reset(df);
		writerLong.reset(df);
	}




}
