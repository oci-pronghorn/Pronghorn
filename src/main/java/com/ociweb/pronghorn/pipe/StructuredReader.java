package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.struct.BStructDimIntListener;
import com.ociweb.pronghorn.struct.BStructFieldVisitor;
import com.ociweb.pronghorn.struct.BStructIntListener;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.Appendables;

public final class StructuredReader {

	public static final int PAYLOAD_INDEX_LOCATION = 0;
	private final DataInputBlobReader<?> channelReader; 
	private final StructRegistry typeData;
	
	//TODO: add is absent, was absent methods..
	//TODO: add direct copy over methods.
	
	public StructuredReader(DataInputBlobReader<?> reader, StructRegistry typeData) {
		this.channelReader = reader;
		this.typeData = typeData;
	}

	public <T> void visit(Class<T> attachedInstanceOf, BStructFieldVisitor<T> visitor) {		
		typeData.visit(channelReader, attachedInstanceOf, visitor);
	}
	
	public <T,B extends T> boolean identityVisit(B attachedInstance, BStructFieldVisitor<T> visitor) {		
		return typeData.identityVisit(channelReader, attachedInstance, visitor);
	}

	int indexCopyLenInBytes(int type) {
		return typeData.totalSizeOfIndexes(type)*4;
	}
	
	public ChannelReader read(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return channelReader;
	}
	
	public ChannelReader read(Object association) {
		long fieldId = typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader));		
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return channelReader;
	}
	
	public String readText(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return channelReader.readUTF();
	}
	
	public String readText(Object association) {
		long fieldId = typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader));		
		
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return channelReader.readUTF();
	}

	public boolean isEqual(long fieldId, byte[] value) {
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		if (channelReader.available()>=2) {
			int length = channelReader.readShort(); 
			return value.length==length && channelReader.equalBytes(value);
		} else {
			return false;
		}
	}

	public boolean isEqual(Object association, byte[] value) {
		long fieldId = typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader));		
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		if (channelReader.available()>=2) {
			int length = channelReader.readShort(); 
			return value.length==length && channelReader.equalBytes(value);
		} else {
			return false;
		}
	}
	
	public long readTextAsLong(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return DataInputBlobReader.readUTFAsLong(channelReader);
	}
	
	public long readTextAsLong(Object association) {
		long fieldId = typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader));		
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return DataInputBlobReader.readUTFAsLong(channelReader);
	}
	
	public double readTextAsDouble(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return DataInputBlobReader.readUTFAsDecimal(channelReader);
	}
	
	public double readTextAsDouble(Object association) {
		long fieldId = typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader));		
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return DataInputBlobReader.readUTFAsDecimal(channelReader);
	}
	
	public <A extends Appendable> A readText(long fieldId, A target) {
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		channelReader.readUTF(target);
		return target;
	}
	
	public <A extends Appendable> A readText(Object association, A target) {
		long fieldId = typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader));		
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		channelReader.readUTF(target);
		return target;
	}
	
	public <A extends Appendable> A readIntAsText(long fieldId, A target) {
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return Appendables.appendValue(target, channelReader.readPackedLong());
	}
	
	public double readRationalAsDouble(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return channelReader.readRationalAsDouble();
	}
	
	public double readRationalAsDouble(Object association) {
		long fieldId = typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader));		
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return channelReader.readRationalAsDouble();
	}
	
	public double readDecimalAsDouble(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return channelReader.readDecimalAsDouble();
	}
	
	public double readDecimalAsDouble(Object association) {
		long fieldId = typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader));		
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return channelReader.readDecimalAsDouble();
	}
	
	public long readDecimalMantissa(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
    	long m = channelReader.readPackedLong();
    	assert(channelReader.storeMostRecentPacked(m));    	
    	return m;
	}
	
	
	public byte readDecimalExponent(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		long m = channelReader.readPackedLong();
    	assert(channelReader.storeMostRecentPacked(m));
    	return channelReader.readByte();
	}	
	
	
	public boolean readBoolean(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return channelReader.readBoolean();
	}
	
	
	public boolean readBoolean(Object association) {
		long fieldId = typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader));		
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return channelReader.readBoolean();
	}	
	
	public int readInt(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return (int)channelReader.readPackedLong();
	}
		
	public long readLong(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return channelReader.readPackedLong();
	}
	
	public int readInt(Object association) {
		long fieldId = typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader));		
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return (int)channelReader.readPackedLong();
	}
	
	public long readLong(Object association) {
		long fieldId = typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader));		
		channelReader.position(channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId));
		return channelReader.readPackedLong();
	}

	public void visitInt(BStructIntListener visitor, Object association) {
		visitInt(visitor, typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	public void visitInt(BStructIntListener visitor, long fieldId) {
		
    	int pos = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    			StructRegistry.FIELD_MASK&(int)fieldId);
    	if (pos>=0) {
    		channelReader.position(pos);    	
    		visitor.value(channelReader.readPackedInt(), false, 0, 1);    		
    	} else {
    		visitor.value(channelReader.readPackedInt(), true, 0, 1);
    	}
    	
	}
	
	public void visitDimInt(BStructDimIntListener visitor, Object association) {
		visitDimInt(visitor,typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader))  );
	}
	
	public void visitDimInt(BStructDimIntListener visitor, long fieldId) {
		
		int dims = typeData.dims(fieldId);
		int instance = DataInputBlobReader.reserveDimArray(channelReader, dims, typeData.maxDim());		
		int[] dimPos = DataInputBlobReader.lookupDimArray(channelReader, dims, instance);
	
		assert(DataInputBlobReader.structTypeValidation((DataInputBlobReader<?>)channelReader, StructRegistry.FIELD_MASK&(int)fieldId)); //set value or check match.
		//assert fieldId is part of StructId
    	int pos = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader, StructRegistry.FIELD_MASK&(int)fieldId);
    	if (pos>=0) {
    		channelReader.position(pos);    	
    		visitor.value(channelReader.readPackedInt(), false, dimPos, 0, 1);    		
    	} else {
    		visitor.value(channelReader.readPackedInt(), true, dimPos, 0, 1);
    	}
    	
	}
	
	public <A extends Appendable> A readIntAsText(Object attachedInstance, A target) {
		
		positionToField(this, attachedInstance);
		return Appendables.appendValue(target, channelReader.readPackedLong());
	}
	
	public <A extends Appendable> A readDecimalAsText(Object attachedInstance, A target) {
		
		positionToField(this, attachedInstance);
		
		long m = channelReader.readPackedLong();
    	assert(channelReader.storeMostRecentPacked(m));
    	
    	return Appendables.appendDecimalValue(target, m, 
    											channelReader.readByte());
	}

	public long readDecimalMantissa(Object attachedInstance) {
		positionToField(this, attachedInstance);
    	long m = channelReader.readPackedLong();
    	assert(channelReader.storeMostRecentPacked(m));    	
    	return m;
	}
	
	
	public byte readDecimalExponent(Object attachedInstance) {
		positionToField(this, attachedInstance);
		long m = channelReader.readPackedLong();
    	assert(channelReader.storeMostRecentPacked(m));
    	return channelReader.readByte();
	}	
	
	private static void positionToField(StructuredReader that, Object attachedInstance) {
		that.channelReader.position(
				that.channelReader.readFromEndLastInt(
						that.typeData.lookupFieldIndex(attachedInstance, 
								DataInputBlobReader.getStructType(that.channelReader))));
	}

	public int type() {
		return DataInputBlobReader.getStructType(channelReader);
	}

	public int fullIndexSizeInBytes() {
		return 4*typeData.totalSizeOfIndexes(DataInputBlobReader.getStructType(channelReader));
	}

	public void fullIndexReadInto(DataOutputBlobWriter<RawDataSchema> str) {
		channelReader.readFromEndInto(str);
	}

}
