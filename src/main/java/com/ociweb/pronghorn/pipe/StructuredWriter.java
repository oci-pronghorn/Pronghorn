package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.struct.BStructSchema;
import com.ociweb.pronghorn.struct.BStructTypes;

public class StructuredWriter {

	private final DataOutputBlobWriter<?> channelWriter;
	private final BStructSchema typeData;
	
	public StructuredWriter(DataOutputBlobWriter<?> channelWriter,
			                BStructSchema typeData) {
		this.channelWriter = channelWriter;
		this.typeData = typeData;
	}
	
	//////////////////////////
	//writes using associated object
	//////////////////////////
	
	public void writeInt(Object assoc, int value) {
		int positionToKeep = channelWriter.position();
		//keep object
		
		//where to store array?
		
		//typeData.maxFieldsPerStrct
		//created..
		
		//write value...
		//need array to store
		
	}
	
	
	
	
	/////////////////////////
	//writes using fieldId
	////////////////////////
	
	public ChannelWriter writeBlob(long fieldId) {
		
		assert(typeData.fieldType(fieldId) == BStructTypes.Blob);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, BStructSchema.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				BStructSchema.extractFieldPosition(fieldId));
		
		return channelWriter;
		
	}
	
	public Appendable writeText(long fieldId) {
		
		assert(typeData.fieldType(fieldId) == BStructTypes.Text);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, BStructSchema.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				BStructSchema.extractFieldPosition(fieldId));
		
		return channelWriter;
		
	}
	
	public void writeBoolean(boolean value, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == BStructTypes.Boolean);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, BStructSchema.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				BStructSchema.extractFieldPosition(fieldId));
		
		channelWriter.writeBoolean(value);
		
	}
	
	public void writeLong(long value, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == BStructTypes.Long);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, BStructSchema.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				BStructSchema.extractFieldPosition(fieldId));
		
		channelWriter.writePackedLong(value);
		
	}
	
	public void writeInt(int value, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == BStructTypes.Integer);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, BStructSchema.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				BStructSchema.extractFieldPosition(fieldId));
		
		channelWriter.writePackedInt(value);
		
	}	
	
	public void writeShort(short value, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == BStructTypes.Short);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, BStructSchema.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				BStructSchema.extractFieldPosition(fieldId));
		
		channelWriter.writePackedShort(value);
		
	}	
	
	
	public void writeByte(int value, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == BStructTypes.Byte);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, BStructSchema.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				BStructSchema.extractFieldPosition(fieldId));
		
		channelWriter.writeByte(value);
		
	}	
	
	public void writeDouble(double value, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == BStructTypes.Double);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, BStructSchema.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				BStructSchema.extractFieldPosition(fieldId));
		
		channelWriter.writeDouble(value);
		
	}
	
	public void writeFloat(float value, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == BStructTypes.Float);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, BStructSchema.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				BStructSchema.extractFieldPosition(fieldId));
		
		channelWriter.writeFloat(value);
		
	}
	
	public void writeRational(long numerator, long denominator, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == BStructTypes.Rational);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, BStructSchema.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				BStructSchema.extractFieldPosition(fieldId));
		
		channelWriter.writeRational(numerator, denominator);
		
	}
	
	public void writeDecimal(long m, byte e, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == BStructTypes.Decimal);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, BStructSchema.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				BStructSchema.extractFieldPosition(fieldId));
		
		channelWriter.writeDecimal(m, e);
		
	}
	
}
