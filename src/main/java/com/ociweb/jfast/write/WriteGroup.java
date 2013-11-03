package com.ociweb.jfast.write;

import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.NullAdjuster;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;

public class WriteGroup implements WriteEntry {

	private final WriteEntry[] members;

	private PrimitiveWriter header;
	private PrimitiveWriter body;
	
	private int c;
	private int bitFlag;
	private int bitCount;
	private int bitMaxByte;
	private int bitBytes;
	
	
	/**
	 * For each field must decide optimal way to send and set all bits before sending.
	 * 
	 * Then send all the fields using specific writers.
	 * 
	 * TODO: this probably needs its own serializer in kryo?
	 */
	public WriteGroup(int id, WriteEntry[] members) {
		this.members = members;
				
		int headerCapacity = (int)Math.ceil(members.length/(double)7); 
		int bodyInitialCapacity = members.length*15;
		
		//TODO: this is really using 2 buffers each (for 4) perhaps we only need one.
		byte[] headBuf = new byte[headerCapacity];
		this.header = new PrimitiveWriter(headerCapacity, new FASTOutputByteArray(headBuf));
		
		byte[] bodyBuf = new byte[bodyInitialCapacity];
		this.body   = new PrimitiveWriter(bodyInitialCapacity, new FASTOutputByteArray(bodyBuf));
		
		reset();
	}

	public void reset() {
		this.bitFlag = 0;
		this.bitCount = 0;
		this.bitMaxByte = -1;
		this.bitBytes = 0;
		this.c = 0;
	}

	
	public int writeLong(PrimitiveWriter writer, ValueDictionaryEntry entry,
						int id,	NullAdjuster nullOff, FASTProvide value) {
	    return updatePMap(writer, members[c++].writeLong(body, entry, id, nullOff, value));
	}

	public int writeInt(PrimitiveWriter writer, ValueDictionaryEntry entry, int id,
			NullAdjuster nullOff, FASTProvide value) {
		return updatePMap(writer, members[c++].writeInt(body, entry, id, nullOff, value));
	}

	public int writeBytes(PrimitiveWriter writer, ValueDictionaryEntry entry, int id,
			NullAdjuster nullOff, FASTProvide value) {
		return updatePMap(writer, members[c++].writeBytes(body, entry, id, nullOff, value));
	}

	public int writeASCII(PrimitiveWriter writer, ValueDictionaryEntry entry, int id,
			NullAdjuster nullOff, FASTProvide value) {
		return updatePMap(writer, members[c++].writeASCII(body, entry, id, nullOff, value));
	}

	public int writeDecimal(PrimitiveWriter writer, ValueDictionaryEntry entry, int id,
			NullAdjuster nullOff, FASTProvide value) {
		return updatePMap(writer, members[c++].writeDecimal(body, entry, id, nullOff, value));
	}


	private final int updatePMap(PrimitiveWriter writer, int bit) {
		return 0;
//		//TODO: this block is the same for all, need to make common.
//		if (bit>=0) {
//			if (bitCount==7) {
//				if (bitFlag!=0) {
//					bitMaxByte = bitBytes;
//				}
//				
//				header.writeByte((byte)bitFlag);
//				bitFlag = 0;
//				bitCount = 0;
//				bitBytes++;
//			}
//			//
//			bitFlag |= (bit&1);
//			bitCount++;
//			
//		} else {
//			//don't write bits because this is required
//		}
//	    if (c==members.length) {
//	    	//cleanup and reset for next write pass
//	    	if (bitFlag!=0) {
//				bitMaxByte = bitBytes;
//			}
//			header.writeByte((byte)bitFlag);
//			reset();
//	    	
//	    	//do we have non zero values?
//	    	if (bitMaxByte>=0) {
//	    		//only take the pmap up to bitMaxByte and that one needs a stopper.
//	    		header.getBuffer()[bitMaxByte] = (byte)(header.getBuffer()[bitMaxByte]|0x80);
//
//	    		//gather write both byteBuffers into target
//	    		target.write(header.getBuffer(), 0, bitMaxByte+1);
//	    		target.write(body.getBuffer(), 0, body.position());
//	    		
//	    		bit = 1;
//	    	} else {
//	    		//only write body into target there is no pmap.
//	    		target.write(body.getBuffer(), 0, body.position());
//	    		bit = 0;
//	    	}
//	    	
//	    } else {
//	    	bit = -1;
//	    }
//		return bit;
	}

	public int writeUTF8(PrimitiveWriter writer, ValueDictionaryEntry entry, int id,
			NullAdjuster nullOff, FASTProvide value) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int writeGroup(PrimitiveWriter writer, ValueDictionaryEntry entry,
			int id, FASTProvide value) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int writeUnsignedLong(PrimitiveWriter writer,
			ValueDictionaryEntry entry, int id, NullAdjuster nullOff,
			FASTProvide value) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int writeUnsignedInt(PrimitiveWriter writer,
			ValueDictionaryEntry entry, int id, NullAdjuster nullOff,
			FASTProvide value) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
}
