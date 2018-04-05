package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructTypes;

public class StructuredWriter {

	private final DataOutputBlobWriter<?> channelWriter;
	private final StructRegistry typeData;
	
	public StructuredWriter(DataOutputBlobWriter<?> channelWriter,
			                StructRegistry typeData) {
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
		
		assert(typeData.fieldType(fieldId) == StructTypes.Blob);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, StructRegistry.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		return channelWriter;
		
	}
	
	public Appendable writeText(long fieldId) {
		
		assert(typeData.fieldType(fieldId) == StructTypes.Text);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, StructRegistry.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		return channelWriter;
		
	}
	
	public void writeBoolean(boolean value, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == StructTypes.Boolean);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, StructRegistry.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writeBoolean(value);
		
	}
	
	public void writeLong(long value, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == StructTypes.Long);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, StructRegistry.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writePackedLong(value);
		
	}
	
	public void writeInt(int value, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == StructTypes.Integer);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, StructRegistry.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writePackedInt(value);
		
	}	
	
	public void writeShort(short value, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == StructTypes.Short);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, StructRegistry.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writePackedShort(value);
		
	}	
	
	
	public void writeByte(int value, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == StructTypes.Byte);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, StructRegistry.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writeByte(value);
		
	}	
	
	public void writeDouble(double value, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == StructTypes.Double);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, StructRegistry.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writeDouble(value);
		
	}
	
	public void writeFloat(float value, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == StructTypes.Float);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, StructRegistry.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writeFloat(value);
		
	}
	
	public void writeRational(long numerator, long denominator, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == StructTypes.Rational);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, StructRegistry.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writeRational(numerator, denominator);
		
	}
	
	public void writeDecimal(long m, byte e, long fieldId) {
		
		assert(typeData.fieldType(fieldId) == StructTypes.Decimal);
		
		DataOutputBlobWriter.structTypeValidation(channelWriter, StructRegistry.extractStructId(fieldId));
		
		DataOutputBlobWriter.setIntBackData(
				channelWriter, 
				channelWriter.position(), 
				StructRegistry.extractFieldPosition(fieldId));
		
		channelWriter.writeDecimal(m, e);
		
	}
	
}
