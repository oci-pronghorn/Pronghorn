package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructTypes;

public class StructuredWriter {

	private final DataOutputBlobWriter<?> channelWriter;
	
	public StructuredWriter(DataOutputBlobWriter<?> channelWriter) {
		this.channelWriter = channelWriter;
	}
	
	//////////////////////////
	//writes using associated object
	//////////////////////////
	
	private int pos = 0;
	private int[] positions = new int[4];
	private Object[] associations = new Object[4];	
	
	public void writeInt(Object assoc, int value) {
		assert(DataOutputBlobWriter.getStructType(channelWriter)<=0) :  "call selectStruct(id) only after setting all the object fields.";
		storeAssocAndPosition(assoc);
		channelWriter.writePackedInt(value);
	}
	
	public void writeShort(Object assoc, short value) {
		assert(DataOutputBlobWriter.getStructType(channelWriter)<=0) :  "call selectStruct(id) only after setting all the object fields.";
		storeAssocAndPosition(assoc);
		channelWriter.writePackedInt(value);
	}
	
	public void writeByte(Object assoc, byte value) {
		assert(DataOutputBlobWriter.getStructType(channelWriter)<=0) :  "call selectStruct(id) only after setting all the object fields.";
		storeAssocAndPosition(assoc);
		channelWriter.write(value);
	}

	public void writeText(Object assoc, CharSequence text) {
		assert(DataOutputBlobWriter.getStructType(channelWriter)<=0) :  "call selectStruct(id) only after setting all the object fields.";
		storeAssocAndPosition(assoc);
		channelWriter.writeUTF(text);
	}
	
	public <A extends Appendable> A writeText(Object assoc) {
		assert(DataOutputBlobWriter.getStructType(channelWriter)<=0) :  "call selectStruct(id) only after setting all the object fields.";
		storeAssocAndPosition(assoc);
		return (A)channelWriter;
	}
	
	public void selectStruct(int structId) {
		
		StructRegistry structRegistry = Pipe.structRegistry(channelWriter.backingPipe);
		assert(DataOutputBlobWriter.getStructType(channelWriter)<=0) :  "call selectStruct(id) only after setting all the object fields.";
		DataOutputBlobWriter.commitBackData(channelWriter, structId);		
		int p = pos;
		while (--p>=0) {
			
			DataOutputBlobWriter.setIntBackData(channelWriter,
								positions[p],
								StructRegistry.lookupIndexOffset(structRegistry,
										              associations[p], structId, 
										              associations[p].hashCode()) & StructRegistry.FIELD_MASK					
							);
		}
		pos = 0;//cleared for next time;
	}

	
	
	///////////////////////

	private void storeAssocAndPosition(Object assoc) {
		if (null==assoc) {
			throw new NullPointerException("associated object must not be null");
		}
		grow(pos);

		int positionToKeep = channelWriter.position();
		//keep object
		positions[pos]=positionToKeep;
		associations[pos]=assoc;
		pos++;
	}
	
	
	private void grow(int pos) {
		if (pos==positions.length) {
			positions = grow(positions);
			associations = grow(associations);
		}
	}
	
	
	////////////////////////
	//writes using fieldId
	////////////////////////


	private Object[] grow(Object[] source) {
		Object[] result = new Object[source.length*2];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private int[] grow(int[] source) {
		int[] result = new int[source.length*2];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}


	public ChannelWriter writeBlob(long fieldId) {
		
		assert(Pipe.structRegistry(channelWriter.backingPipe).fieldType(fieldId) == StructTypes.Blob);
		DataOutputBlobWriter.commitBackData(channelWriter, StructRegistry.extractStructId(fieldId));
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		return channelWriter;
		
	}
	
	public Appendable writeText(long fieldId) {
		
		assert(Pipe.structRegistry(channelWriter.backingPipe).fieldType(fieldId) == StructTypes.Text);
		DataOutputBlobWriter.commitBackData(channelWriter, StructRegistry.extractStructId(fieldId));
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		return channelWriter;
		
	}
	
	public void writeBoolean(boolean value, long fieldId) {
		
		assert(Pipe.structRegistry(channelWriter.backingPipe).fieldType(fieldId) == StructTypes.Boolean);
		DataOutputBlobWriter.commitBackData(channelWriter, StructRegistry.extractStructId(fieldId));
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writeBoolean(value);
		
		assert confirmDataDoesNotWriteOverIndex(fieldId) : "Data has witten over index data";
	}
	
	public void writeBoolean(Object assoc, boolean value) {
		assert(DataOutputBlobWriter.getStructType(channelWriter)<=0) :  "call selectStruct(id) only after setting all the object fields.";
		storeAssocAndPosition(assoc);
		channelWriter.writeBoolean(value);
	}
	
	public void writeLong(long value, long fieldId) {
		
		assert(Pipe.structRegistry(channelWriter.backingPipe).fieldType(fieldId) == StructTypes.Long);
		DataOutputBlobWriter.commitBackData(channelWriter, StructRegistry.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writePackedLong(value);
		
		assert confirmDataDoesNotWriteOverIndex(fieldId) : "Data has witten over index data";
	}
	
	public void writeLong(Object assoc, long value) {
		assert(DataOutputBlobWriter.getStructType(channelWriter)<=0) :  "call selectStruct(id) only after setting all the object fields.";
		storeAssocAndPosition(assoc);
		channelWriter.writePackedLong(value);
	}
	
	public void writeInt(int value, long fieldId) {
		
		assert(Pipe.structRegistry(channelWriter.backingPipe).fieldType(fieldId) == StructTypes.Integer);		
		DataOutputBlobWriter.commitBackData(channelWriter, StructRegistry.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writePackedInt(value);
		
		assert confirmDataDoesNotWriteOverIndex(fieldId) : "Data has witten over index data";
	}

	//TODO: this is fine for asserts but we need to check this before it happens like the old pub sub struct did.
	
	private boolean confirmDataDoesNotWriteOverIndex(long fieldId) {
		return channelWriter.position()< (Pipe.blobIndexBasePosition(channelWriter.backingPipe)-(4*Pipe.structRegistry(channelWriter.backingPipe)
				.totalSizeOfIndexes((int)(fieldId>>StructRegistry.STRUCT_OFFSET))));
	}	
	
	public void writeShort(short value, long fieldId) {
		
		assert(Pipe.structRegistry(channelWriter.backingPipe).fieldType(fieldId) == StructTypes.Short);
		DataOutputBlobWriter.commitBackData(channelWriter, StructRegistry.extractStructId(fieldId));
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writePackedShort(value);
		
		assert confirmDataDoesNotWriteOverIndex(fieldId) : "Data has witten over index data";
	}	
	
	
	public void writeByte(int value, long fieldId) {
		
		assert(Pipe.structRegistry(channelWriter.backingPipe).fieldType(fieldId) == StructTypes.Byte);
		DataOutputBlobWriter.commitBackData(channelWriter, StructRegistry.extractStructId(fieldId));
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writeByte(value);
		
		assert confirmDataDoesNotWriteOverIndex(fieldId) : "Data has witten over index data";
	}	
	
	public void writeDouble(double value, long fieldId) {
		
		assert(Pipe.structRegistry(channelWriter.backingPipe).fieldType(fieldId) == StructTypes.Double);
		DataOutputBlobWriter.commitBackData(channelWriter, StructRegistry.extractStructId(fieldId));
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writeDouble(value);
		
		assert confirmDataDoesNotWriteOverIndex(fieldId) : "Data has witten over index data";
	}
	
	public void writeFloat(float value, long fieldId) {
		
		assert(Pipe.structRegistry(channelWriter.backingPipe).fieldType(fieldId) == StructTypes.Float);
		DataOutputBlobWriter.commitBackData(channelWriter, StructRegistry.extractStructId(fieldId));
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writeFloat(value);
		
		assert confirmDataDoesNotWriteOverIndex(fieldId) : "Data has witten over index data";
	}
	
	public void writeRational(long numerator, long denominator, long fieldId) {
		
		assert(Pipe.structRegistry(channelWriter.backingPipe).fieldType(fieldId) == StructTypes.Rational);
		DataOutputBlobWriter.commitBackData(channelWriter, StructRegistry.extractStructId(fieldId));
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writeRational(numerator, denominator);
		
		assert confirmDataDoesNotWriteOverIndex(fieldId) : "Data has witten over index data";
	}
	
	public void writeDecimal(long m, byte e, long fieldId) {
		
		assert(Pipe.structRegistry(channelWriter.backingPipe).fieldType(fieldId) == StructTypes.Decimal);
		DataOutputBlobWriter.commitBackData(channelWriter, StructRegistry.extractStructId(fieldId));
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writeDecimal(m, e);
		
		assert confirmDataDoesNotWriteOverIndex(fieldId) : "Data has witten over index data";
	}

	public void fullIndexWriteFrom(int indexSizeInBytes, DataInputBlobReader<RawDataSchema> reader) {
		DataOutputBlobWriter.writeToEndFrom(channelWriter,indexSizeInBytes,reader);
	}
	
}
