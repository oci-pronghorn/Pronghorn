package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.struct.BStructDimIntListener;
import com.ociweb.pronghorn.struct.BStructFieldVisitor;
import com.ociweb.pronghorn.struct.BStructIntListener;
import com.ociweb.pronghorn.struct.BStructSchema;
import com.ociweb.pronghorn.struct.BStructTypes;

public final class StructuredReader {

	public static final int PAYLOAD_INDEX_LOCATION = 0;
	private final DataInputBlobReader<?> channelReader; 
	private final BStructSchema typeData;
	
	public StructuredReader(DataInputBlobReader<?> reader, BStructSchema typeData) {
		this.channelReader = reader;
		this.typeData = typeData;
	}

	public <T> void visit(Class<T> attachedInstanceOf, BStructFieldVisitor<T> visitor) {		
		typeData.visit(channelReader, attachedInstanceOf, visitor);
	}
	
	public <T,B extends T> boolean identityVisit(B attachedInstance, BStructFieldVisitor<T> visitor) {		
		return typeData.identityVisit(channelReader, attachedInstance, visitor);
	}
	
//TODO: add isEqual method
	
	
	int indexCopyLenInBytes(int type) {
		return typeData.totalSizeOfIndexes(type)*4;
	}
	
	public ChannelReader read(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(BStructSchema.FIELD_MASK&(int)fieldId));
		return channelReader;
	}
	
	public String readText(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(BStructSchema.FIELD_MASK&(int)fieldId));
		return channelReader.readUTF();
	}
	
	public long readTextAsLong(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(BStructSchema.FIELD_MASK&(int)fieldId));
		return DataInputBlobReader.readUTFAsLong(channelReader);
	}
	
	public double readTextAsDouble(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(BStructSchema.FIELD_MASK&(int)fieldId));
		return DataInputBlobReader.readUTFAsDecimal(channelReader);
	}
	
	public <A extends Appendable> A readText(long fieldId, A target) {
		channelReader.position(channelReader.readFromEndLastInt(BStructSchema.FIELD_MASK&(int)fieldId));
		channelReader.readUTF(target);
		return target;
	}
	
	public double readRationalAsDouble(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(BStructSchema.FIELD_MASK&(int)fieldId));
		return channelReader.readRationalAsDouble();
	}
	
	public double readDecimalAsDouble(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(BStructSchema.FIELD_MASK&(int)fieldId));
		return channelReader.readDecimalAsDouble();
	}
	
	public boolean readBoolean(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(BStructSchema.FIELD_MASK&(int)fieldId));
		return channelReader.readBoolean();
	}	
	
	public int readInt(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(BStructSchema.FIELD_MASK&(int)fieldId));
		return (int)channelReader.readPackedLong();
	}
	
	public long readLong(long fieldId) {
		channelReader.position(channelReader.readFromEndLastInt(BStructSchema.FIELD_MASK&(int)fieldId));
		return channelReader.readPackedLong();
	}
	
	public int readInt(Object association) {
		long fieldId = typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader));		
		channelReader.position(channelReader.readFromEndLastInt(BStructSchema.FIELD_MASK&(int)fieldId));
		return (int)channelReader.readPackedLong();
	}
	
	public long readLong(Object association) {
		long fieldId = typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader));		
		channelReader.position(channelReader.readFromEndLastInt(BStructSchema.FIELD_MASK&(int)fieldId));
		return channelReader.readPackedLong();
	}

	public void visitInt(BStructIntListener visitor, Object association) {
		visitInt(visitor, typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	public void visitInt(BStructIntListener visitor, long fieldId) {
		
    	int pos = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    			BStructSchema.FIELD_MASK&(int)fieldId);
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
	
		assert(DataInputBlobReader.structTypeValidation((DataInputBlobReader<?>)channelReader, BStructSchema.FIELD_MASK&(int)fieldId)); //set value or check match.
		//assert fieldId is part of StructId
    	int pos = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader, BStructSchema.FIELD_MASK&(int)fieldId);
    	if (pos>=0) {
    		channelReader.position(pos);    	
    		visitor.value(channelReader.readPackedInt(), false, dimPos, 0, 1);    		
    	} else {
    		visitor.value(channelReader.readPackedInt(), true, dimPos, 0, 1);
    	}
    	
	}
	
}
