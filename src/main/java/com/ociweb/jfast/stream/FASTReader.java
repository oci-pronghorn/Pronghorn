package com.ociweb.jfast.stream;

import static com.ociweb.jfast.field.OperatorMask.None;
import static com.ociweb.jfast.field.TypeMask.IntegerSigned;
import static com.ociweb.jfast.field.TypeMask.IntegerSignedOptional;
import static com.ociweb.jfast.field.TypeMask.IntegerUnSigned;
import static com.ociweb.jfast.field.TypeMask.IntegerUnSignedOptional;

import com.ociweb.jfast.DecimalDTO;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.field.FieldReader;
import com.ociweb.jfast.field.FieldReaderInteger;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;

//May drop interface if this causes a performance problem from virtual table
public class FASTReader implements FASTProvide {

	private final PrimitiveReader reader;
	private final int[] tokenLookup; //array of tokens as field id locations
	
	private final FieldReaderInteger readerInteger;
	
	private final int MASK = 0x3FF;
	private final int INST = 20;
	
	private final int MASK_TYPE = 0x3F;
	private final int SHIFT_TYPE = 24;
	
	private final int MASK_OPER = 0x0F;
	private final int SHIFT_OPER = 20;
	
	//32 bits total
	//two high bits set
	//  6 bit type (must match method)
	//  4 bit operation (must match method)
	// 20 bit instance (MUST be lowest for easy mask and frequent use)
	
	public FASTReader(PrimitiveReader reader, int fields, int[] tokenLookup) {
		this.reader=reader;
		this.tokenLookup = tokenLookup;
		readerInteger = new FieldReaderInteger(reader,fields);
	}
	
	@Override
	public boolean provideNull(int id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long provideLong(int id) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int provideInt(int id) {
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
	public byte[] provideBytes(int id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CharSequence provideCharSequence(int id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void provideDecimal(int id, DecimalDTO target) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void openGroup(int maxPMapBytes) {
		reader.readPMap(maxPMapBytes);
	}

	@Override
	public void closeGroup() {
		reader.popPMap();
	}

	public boolean isGroupOpen() {
		return reader.isPMapOpen();
	}

}
