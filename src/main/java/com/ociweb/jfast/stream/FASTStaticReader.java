package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.FieldReaderInteger;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;

//May drop interface if this causes a performance problem from virtual table
public class FASTStaticReader implements FASTReader {

	private final PrimitiveReader reader;
	private final int[] tokenLookup; //array of tokens as field id locations
	
	//package protected so DynamicReader can use these instances
	final FieldReaderInteger readerInteger;
		
	//See fast writer for details and mask sizes
	private final int MASK_TYPE = 0x3F;
	private final int SHIFT_TYPE = 24;
	
	private final int MASK_OPER = 0x0F;
	private final int SHIFT_OPER = 20;
	
	private final int MASK_PMAP_MAX = 0x7FF;
	private final int SHIFT_PMAP_MASK = 20;
	
		
	public FASTStaticReader(PrimitiveReader reader, int fields, int[] tokenLookup) {
		this.reader=reader;
		this.tokenLookup = tokenLookup;
		
		this.readerInteger = new FieldReaderInteger(reader,fields);
	}
	
	//package protected, unless we find a need to expose it?
	void readToken(int token) {
		//used by groups which hold list of tokens
		//at end of each group call back may be done and FASTDynamicReader used.
		
		//TODO: must also detect null.
		
	    switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.IntegerUnsigned:
				readIntegerUnsigned(token,0);
				break;
			case TypeMask.IntegerUnsignedOptional:
				readIntegerUnsignedOptional(token,0);
				break;
			case TypeMask.IntegerSigned:
				readIntegerSigned(token);
				break;
			case TypeMask.IntegerSignedOptional:
				readIntegerSignedOptional(token,0);
				break;
			case TypeMask.LongUnSigned:
				readLongUnsigned(token);
				break;
			case TypeMask.LongUnSignedOptional:
				readLongUnsignedOptional(token,0);
				break;
			case TypeMask.LongSigned:
				readLongSigned(token);
				break;
			case TypeMask.LongSignedOptional:
				readLongSignedOptional(token,0);
				break;
			case TypeMask.TextASCII:
				readTextASCII(token, null); //TODO: these nulls are not corect but we do not need the result.
			    break;
		    case TypeMask.TextASCIIOptional:
				readTextASCIIOptional(token, null);
			    break;
			case TypeMask.TextUTF8:
				readTextUTF8(token,null);
				break;
			case TypeMask.TextUTF8Optional:
				readTextUTF8Optional(token, null);
				break;
			case TypeMask.DecimalSingle:
				break;
			case TypeMask.DecimalSingleOptional:
				break;
			case TypeMask.DecimalTwin:
				break;
			case TypeMask.DecimalTwinOptional:
				break;
			case TypeMask.ByteArray:
				break;
			case TypeMask.ByteArrayOptional:
				break;
			default:
				throw new UnsupportedOperationException();
			}
	}
	
	
	@Override
	public long readLong(int id, long valueOfOptional) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.LongUnSigned:
				return readLongUnsigned(token);
			case TypeMask.LongUnSignedOptional:
				return readLongUnsignedOptional(token, valueOfOptional);
			case TypeMask.LongSigned:
				return readLongSigned(token);
			case TypeMask.LongSignedOptional:
				return readLongSignedOptional(token, valueOfOptional);
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}
	
	private long readLongSignedOptional(int token, long valueOfOptional) {
		// TODO Auto-generated method stub
		return 0;
	}

	private long readLongSigned(int token) {
		// TODO Auto-generated method stub
		return 0;
	}

	private long readLongUnsignedOptional(int token, long valueOfOptional) {
		// TODO Auto-generated method stub
		return 0;
	}

	private long readLongUnsigned(int token) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int readInt(int id, int valueOfOptional) {
		
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.IntegerUnsigned:
				return readIntegerUnsigned(token, valueOfOptional);
			case TypeMask.IntegerUnsignedOptional:
				return readIntegerUnsignedOptional(token, valueOfOptional);
			case TypeMask.IntegerSigned:
				return readIntegerSigned(token);
			case TypeMask.IntegerSignedOptional:
				return readIntegerSignedOptional(token, valueOfOptional);
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
		
	}

	private int readIntegerSignedOptional(int token, int valueOfOptional) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				return readerInteger.readIntegerSignedOptional(token, valueOfOptional);
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readIntegerSigned(int token) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				return readerInteger.readIntegerSigned(token);
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readIntegerUnsignedOptional(int token, int valueOfOptional) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				return readerInteger.readIntegerUnsignedOptional(token,valueOfOptional);
			case OperatorMask.Constant:
				//down grade to non optional rather than fail
				return readerInteger.readIntegerUnsignedConstant(token,valueOfOptional);
			case OperatorMask.Copy:
				return readerInteger.readIntegerUnsignedCopyOptional(token,valueOfOptional);
			case OperatorMask.Default:
				return readerInteger.readIntegerUnsignedDefaultOptional(token,valueOfOptional);
			case OperatorMask.Delta:
				return readerInteger.readIntegerUnsignedDeltaOptional(token,valueOfOptional);
			case OperatorMask.Increment:
				return readerInteger.readIntegerUnsignedIncrementOptional(token,valueOfOptional);	
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readIntegerUnsigned(int token, int valueOfOptional) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				return readerInteger.readIntegerUnsigned(token);
			case OperatorMask.Constant:
				return readerInteger.readIntegerUnsignedConstant(token, valueOfOptional);
			case OperatorMask.Copy:
				return readerInteger.readIntegerUnsignedCopy(token);
			case OperatorMask.Default:
				return readerInteger.readIntegerUnsignedDefault(token);
			case OperatorMask.Delta:
				return readerInteger.readIntegerUnsignedDelta(token);
			case OperatorMask.Increment:
				return readerInteger.readIntegerUnsignedIncrement(token);		
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void readBytes(int id, ByteBuffer target) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.ByteArray:
				throw new UnsupportedOperationException();
			case TypeMask.ByteArrayOptional:
				throw new UnsupportedOperationException();
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public int readBytes(int id, byte[] target, int offset) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.ByteArray:
				throw new UnsupportedOperationException();
			case TypeMask.ByteArrayOptional:
				throw new UnsupportedOperationException();
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void openGroup(int id) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		reader.readPMap(MASK_PMAP_MAX&(token>>SHIFT_PMAP_MASK));
		
	}

	@Override
	public void closeGroup() {
		reader.popPMap();
	}

	public boolean isGroupOpen() {
		return reader.isPMapOpen();
	}

	@Override
	public int readDecimalExponent(int id, int valueOfOptional) {
		
		if (reader.peekNull()) {
			reader.incPosition();
			return valueOfOptional;
		}
		
		return reader.readIntegerSignedOptional();
		
	}
	

	@Override
	public long readDecimalMantissa(int id, long valueOfOptional) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void readChars(int id, Appendable target) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.TextASCII:
					readTextASCII(token, target);
				break;
			case TypeMask.TextASCIIOptional:
					readTextASCIIOptional(token, target);
				break;
			case TypeMask.TextUTF8:
					readTextUTF8(token,target);
				break;
			case TypeMask.TextUTF8Optional:
					readTextUTF8Optional(token, target);
			    break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void readTextUTF8Optional(int token, Appendable target) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				int length = reader.readIntegerUnsigned()-1;
				reader.readTextUTF8(length, target);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void readTextUTF8(int token, Appendable target) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				int length = reader.readIntegerUnsigned();
				reader.readTextUTF8(length, target);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void readTextASCIIOptional(int token, Appendable target) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				reader.readTextASCII(target);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void readTextASCII(int token, Appendable target) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				reader.readTextASCII(target);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public int readChars(int id, char[] target, int offset) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.TextASCII:
				return readTextASCII(token, target, offset);
			case TypeMask.TextASCIIOptional:
				return readTextASCIIOptional(token, target, offset);
			case TypeMask.TextUTF8:
				return readTextUTF8(token, target, offset);
			case TypeMask.TextUTF8Optional:
				return readTextUTF8Optional(token, target, offset);
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readTextUTF8Optional(int token, char[] target, int offset) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				int length = reader.readIntegerUnsigned()-1;
				reader.readTextUTF8(target,offset,length);
				return length;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readTextUTF8(int token, char[] target, int offset) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				int length = reader.readIntegerUnsigned();
				reader.readTextUTF8(target,offset,length);
				return length;
			default:
				throw new UnsupportedOperationException();
	}
	}

	private int readTextASCIIOptional(int token, char[] target, int offset) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				return reader.readTextASCII(target,offset);
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readTextASCII(int token, char[] target, int offset) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				return reader.readTextASCII(target,offset);
			default:
				throw new UnsupportedOperationException();
		}
	}

	public void reset() {
		//clear all previous values to unset
		readerInteger.reset();
		
	}



}
