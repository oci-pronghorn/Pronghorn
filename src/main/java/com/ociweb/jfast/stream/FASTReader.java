package com.ociweb.jfast.stream;

import static com.ociweb.jfast.field.OperatorMask.None;
import static com.ociweb.jfast.field.TypeMask.IntegerSigned;
import static com.ociweb.jfast.field.TypeMask.IntegerSignedOptional;
import static com.ociweb.jfast.field.TypeMask.IntegerUnSigned;
import static com.ociweb.jfast.field.TypeMask.IntegerUnSignedOptional;

import com.ociweb.jfast.DecimalDTO;
import com.ociweb.jfast.FASTProvide;

//May drop interface if this causes a performance problem from virtual table
public class FASTReader implements FASTProvide {

	private final int[] tokenLookup; //array of tokens as field id locations
	
	private final int MASK = 0x3FF;
	private final int INST = 20;
	//32 bits total
	//two high bits set
	//  6 bit type (must match method)
	//  4 bit operation (must match method)
	// 20 bit instance (MUST be lowest for easy mask and frequent use)
	
	public FASTReader(int[] tokenLookup) {
		//this.readerInteger = readerInteger;
		this.tokenLookup = tokenLookup;
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
		
		switch ((token>>INST)&MASK) {
			case (IntegerUnSigned<<4)|None:
				//writer.writeIntegerUnsigned
				break;
			case (IntegerSigned<<4)|None:
			//	writerInteger.writeIntegerSigned(value, token);
				break;
			case (IntegerUnSignedOptional<<4)|None:
				//writer.writer
				break;
			case (IntegerSignedOptional<<4)|None:
			//	writerInteger.writeIntegerSignedOptional(value, token);
				break;
						
			default:
				break;
		}
		//int type = token>>20
		
		// TODO Auto-generated method stub
		return 0;
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
	public void beginGroup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void endGroup() {
		// TODO Auto-generated method stub
		
	}

}
