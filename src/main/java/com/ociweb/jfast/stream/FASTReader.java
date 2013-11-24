package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.field.FieldReaderInteger;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;

//May drop interface if this causes a performance problem from virtual table
public class FASTReader implements FASTProvide {

	private final PrimitiveReader reader;
	private final int[] tokenLookup; //array of tokens as field id locations
	
	private final FieldReaderInteger readerInteger;
		
	//See fast writer for details and mask sizes
	private final int MASK_TYPE = 0x3F;
	private final int SHIFT_TYPE = 24;
	
	private final int MASK_OPER = 0x0F;
	private final int SHIFT_OPER = 20;
	
	private final int MASK_PMAP_MAX = 0x7FF;
	private final int SHIFT_PMAP_MASK = 20;
	
		
	public FASTReader(PrimitiveReader reader, int fields, int[] tokenLookup) {
		this.reader=reader;
		this.tokenLookup = tokenLookup;
		readerInteger = new FieldReaderInteger(reader,fields);
	}
	
	@Override
	public long readLong(int id, long valueOfOptional) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public long readLong(int id) {
		// TODO Auto-generated method stub
		return 0;
	}

	
	
	@Override
	public int readInt(int id, int valueOfOptional) {
		
		//TODO: need operation specific implementations.s
		if (reader.peekNull()) {
			reader.incPosition();
			return valueOfOptional;
		}
		
		
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.IntegerUnSigned:
				return readIntegerUnsigned(token);
			case TypeMask.IntegerUnSignedOptional:
				return readIntegerUnsignedOptional(token);
			case TypeMask.IntegerSigned:
				return readIntegerSigned(token);
			case TypeMask.IntegerSignedOptional:
				return readIntegerSignedOptional(token);
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
		
	}

	@Override
	public int readInt(int id) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>SHIFT_TYPE)&MASK_TYPE) {
			case TypeMask.IntegerUnSigned:
				return readIntegerUnsigned(token);
			case TypeMask.IntegerUnSignedOptional:
				return readIntegerUnsignedOptional(token);
			case TypeMask.IntegerSigned:
				return readIntegerSigned(token);
			case TypeMask.IntegerSignedOptional:
				return readIntegerSignedOptional(token);
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}

	private int readIntegerSignedOptional(int token) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				return reader.readSignedIntegerNullable();
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readIntegerSigned(int token) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				return reader.readSignedInteger();
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readIntegerUnsignedOptional(int token) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				return reader.readUnsignedIntegerNullable();
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readIntegerUnsigned(int token) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				return reader.readUnsignedInteger();
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void readBytes(int id, ByteBuffer target) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int readBytes(int id, byte[] target, int offset) {
		// TODO Auto-generated method stub
		
		
		return 0;
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
	public int readDecimalExponent(int id) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long readDecimalMantissa(int id) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int readDecimalExponent(int id, int valueOfOptional) {
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
				int length = reader.readUnsignedInteger()-1;
				reader.readTextUTF8(length, target);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void readTextUTF8(int token, Appendable target) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				int length = reader.readUnsignedInteger();
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
				int length = reader.readUnsignedInteger()-1;
				reader.readTextUTF8(target,offset,length);
				return length;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readTextUTF8(int token, char[] target, int offset) {
		switch ((token>>SHIFT_OPER)&MASK_OPER) {
			case OperatorMask.None:
				int length = reader.readUnsignedInteger();
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


}
