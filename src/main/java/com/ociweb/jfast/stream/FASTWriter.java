package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import com.ociweb.jfast.FASTxmiter;
import com.ociweb.jfast.field.FieldWriterInteger;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveWriter;

//May drop interface if this causes a performance problem from virtual table 
public final class FASTWriter implements FASTxmiter {
	
	//TODO: add assert at beginning of every method that calls a FASTAccept
	//       object which is only created from the template when assert is on
	//       this will validate each FieldId/Token is the expected one in that order.
	
	private final PrimitiveWriter writer;
	private final FieldWriterInteger writerInteger;
	//writerLong
	//writerText
	//writerByteArray
	//writerDecimal
	
	
	private final int[] tokenLookup; //array of tokens as field id locations
	
	
	private final int MASK_TYPE = 0x7F;
	private final int SHIFT_TYPE = 24;
	
	private final int MASK_OPER = 0x0F;
	private final int SHIFT_OPER = 20;
	
	private final int MASK_PMAP_MAX = 0x7FF;
	private final int SHIFT_PMAP_MASK = 20;
	
	
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

	
	public FASTWriter(PrimitiveWriter writer, int intFields, int[] tokenLookup) {
		this.writer = writer;
		this.writerInteger = new FieldWriterInteger(writer, intFields);
		this.tokenLookup = tokenLookup;
	}
	
	/**
	 * Write null value, must only be used if the field id is one
	 * of optional type.
	 */
	@Override
	public void write(int id) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
		case OperatorMask.None:
			writer.writeNull();
			break;
		default:
			//TODO: other operators may have different pmap assignments for null
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
			case TypeMask.LongUnSigned:
				acceptLongUnsigned(token, value);
				break;
			case TypeMask.LongUnSignedOptional:
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
				writer.writeSignedLongNullable(value);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptLongSigned(int token, long value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeSignedLong(value);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptLongUnsignedOptional(int token, long value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeUnsignedLongNullable(value);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptLongUnsigned(int token, long value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeUnsignedLong(value);
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
			case TypeMask.IntegerUnSigned:
				acceptIntegerUnsigned(token, value);
				break;
			case TypeMask.IntegerUnSignedOptional:
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
				writer.writeSignedInteger(value);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}
	
	private void acceptIntegerUnsigned(int token, int value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeUnsignedInteger(value);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptIntegerSignedOptional(int token, int value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeSignedIntegerNullable(value);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}
	
	private void acceptIntegerUnsignedOptional(int token, int value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeUnsignedIntegerNullable(value);
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
			case TypeMask.Decimal:
				acceptDecimal(token, exponent, mantissa);
				break;
			case TypeMask.DecimalOptional:
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
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptDecimal(int token, int exponent, long mantissa) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
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
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharSequenceUTF8(int token, CharSequence value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharSequenceASCIIOptional(int token, CharSequence value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharSequenceASCII(int token, CharSequence value) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void write(int id, CharBuffer buffer) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.TextASCII: 
				acceptCharBufferASCII(token,buffer);
				break;
			case TypeMask.TextASCIIOptional:
				acceptCharBufferASCIIOptional(token,buffer);
				break;
			case TypeMask.TextUTF8: 
				acceptCharBufferUTF8(token,buffer);
				break;
			case TypeMask.TextUTF8Optional:
				acceptCharBufferUTF8Optional(token,buffer);
				break;
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharBufferUTF8Optional(int token, CharBuffer buffer) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharBufferUTF8(int token, CharBuffer buffer) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharBufferASCIIOptional(int token, CharBuffer buffer) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeASCII(buffer);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharBufferASCII(int token, CharBuffer buffer) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				writer.writeASCII(buffer);
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
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharArrayUTF8(int token, char[] value, int offset, int length) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharArrayASCIIOptional(int token, char[] value, int offset, int length) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharArrayASCII(int token, char[] value, int offset, int length) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
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




}
