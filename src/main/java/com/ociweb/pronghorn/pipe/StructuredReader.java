package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.struct.BStructDimIntListener;
import com.ociweb.pronghorn.struct.StructShortListener;
import com.ociweb.pronghorn.struct.StructLongListener;
import com.ociweb.pronghorn.struct.StructFieldVisitor;
import com.ociweb.pronghorn.struct.StructIntListener;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructTypes;
import com.ociweb.pronghorn.util.Appendables;

public final class StructuredReader {

	public static final int PAYLOAD_INDEX_LOCATION = 0;
	private final DataInputBlobReader<?> channelReader; 
	final StructRegistry typeData;
	
	public StructuredReader(DataInputBlobReader<?> reader, StructRegistry typeData) {
		this.channelReader = reader;
		this.typeData = typeData;
	}

	public <T> void visit(Class<T> attachedInstanceOf, StructFieldVisitor<T> visitor) {		
		typeData.visit(channelReader, attachedInstanceOf, visitor);
	}
	
	public <T,B extends T> boolean identityVisit(B attachedInstance, StructFieldVisitor<T> visitor) {		
		return typeData.identityVisit(channelReader, attachedInstance, visitor);
	}
	

	private final boolean matchOneOfTypes(Object attachedInstance, StructTypes ... assoc) {
		long fieldId = typeData.fieldLookupByIdentity(attachedInstance, 
				DataInputBlobReader.getStructType(channelReader));	
		return matchOneOfTypes(fieldId, assoc);
	}

	private final boolean matchOneOfTypes(long fieldId, StructTypes... assoc) {
		boolean ok = false;
		StructTypes fieldType = typeData.fieldType(fieldId);
		int i=assoc.length;
		while (--i>=0) {
			if (assoc[i]==fieldType) {
				ok = true;
			}
		}
		return ok;
	}
	
	
	public final int fullIndexSizeInBytes() {
		return 4*typeData.totalSizeOfIndexes(DataInputBlobReader.getStructType(channelReader));
	}
	
	//set to a position for general reading unless index position is not provided
	public ChannelReader read(long fieldId) {
		final int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>0) {
			channelReader.position(index);
			return channelReader;
		} else {
			return null;
		}
	}
	
	public boolean isNull(long fieldId) {
		return channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId)<=0;
	}
	
	public boolean hasValue(long fieldId) {
		return channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId)>0;
	}
	
	//set to a position for general reading
	public ChannelReader read(Object association) {
		return read(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	public boolean isNull(Object association) {
		return isNull(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	public boolean hasValue(Object association) {
		return hasValue(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	//returns null if absent
	public String readText(long fieldId) {
	
		final int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>0) {
			channelReader.position(index);
			return channelReader.readUTF();
		} else {
			return null;
		}
	}
	
	//returns null if absent
	public String readText(Object association) {
		return readText(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}

	public boolean isEqual(long fieldId, byte[] utf8EncodedBytes) {
	
		final int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>0) {
			channelReader.position(index);
			if (channelReader.available()>=2) {
				int length = channelReader.readShort(); 
				return utf8EncodedBytes.length==length && channelReader.equalBytes(utf8EncodedBytes);
			} 
		}
		return false;
	}

	public boolean isEqual(Object association, byte[] utf8EncodedBytes) {
		return isEqual(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)), utf8EncodedBytes);
	}
	
	//returns -1 when absent
	public long readTextAsLong(long fieldId) {
		assert(typeData.fieldType(fieldId) == StructTypes.Text);
		final int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>0) {
			channelReader.position(index);
			return DataInputBlobReader.readUTFAsLong(channelReader);
		} else {
			return -1;
		}
	}
	
	//returns -1 when absent
	public long readTextAsLong(Object association) {
		return readTextAsLong(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	//returns -1 when absent
	public double readTextAsDouble(long fieldId) {
		assert(typeData.fieldType(fieldId) == StructTypes.Text);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>0) {
			channelReader.position(index);
			return DataInputBlobReader.readUTFAsDecimal(channelReader);
		} else {
			return -1;
		}
	}
	
	//returns -1 when absent
	public double readTextAsDouble(Object association) {
		return readTextAsDouble(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	//appends nothing when absent
	public <A extends Appendable> A readText(long fieldId, A target) {
		assert(typeData.fieldType(fieldId) == StructTypes.Text);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>0) {
			channelReader.position(index);
			channelReader.readUTF(target);
		}
		return target;
	}
	
	//appends nothing when absent
	public <A extends Appendable> A readText(Object association, A target) {
		return readText(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)), target);
	}
	
	//appends nothing when absent
	public <A extends Appendable> A readIntAsText(long fieldId, A target) {
		assert(typeData.fieldType(fieldId) == StructTypes.Short ||
			   typeData.fieldType(fieldId) == StructTypes.Integer ||
		   	   typeData.fieldType(fieldId) == StructTypes.Long);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>0) {
			channelReader.position(index);
			return Appendables.appendValue(target, channelReader.readPackedLong());
		} else {
			return target;
		}
	}
	
	//appends nothing when absent
	public <A extends Appendable> A readIntAsText(Object association, A target) {
		return readIntAsText(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)), target);
	}
	

	//method split out because NaN usage has a side effect of preventing HotSpot optimizations
	private double nullReadOfDouble() {
		return Double.NaN;
	}
	
	
	//return NaN when field is absent
	public double readRationalAsDouble(long fieldId) {
		assert(typeData.fieldType(fieldId) == StructTypes.Rational);
		final int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>0) {
			channelReader.position(index);
			return channelReader.readRationalAsDouble();
		} else {
			return nullReadOfDouble();
		}
	}

	//return NaN when field is absent
	public double readRationalAsDouble(Object association) {
		return readRationalAsDouble(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
		
	//return NaN when field is absent
	public double readDecimalAsDouble(long fieldId) {
		assert(typeData.fieldType(fieldId) == StructTypes.Decimal);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>0) {
			channelReader.position(index);
			return channelReader.readDecimalAsDouble();
		} else {
			return nullReadOfDouble();
		}
	}
	
	//return NaN when field is absent
	public double readDecimalAsDouble(Object association) {
		return readDecimalAsDouble(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	//returns -1 when absent
	public long readDecimalMantissa(long fieldId) {
		assert(typeData.fieldType(fieldId) == StructTypes.Decimal);
		final int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>0) {
			channelReader.position(index);
	    	final long m = channelReader.readPackedLong();
	    	assert(channelReader.storeMostRecentPacked(m));    	
	    	return m;
		} else {
			return -1;
		}
	}
	
	//returns -1 when absent
	public long readDecimalMantissa(Object association) {
		return readDecimalMantissa(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
		
	//returns 0 when absent
	public byte readDecimalExponent(long fieldId) {
		assert(typeData.fieldType(fieldId) == StructTypes.Decimal);
		final int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>0) {
			channelReader.position(index);
			final long m = channelReader.readPackedLong();
	    	assert(channelReader.storeMostRecentPacked(m));
	    	return channelReader.readByte();
		} else {
			return 0;
		}
	}	
	
	//returns 0 when absent
	public byte readDecimalExponent(Object association) {
		return readDecimalExponent(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
		
	//returns false when absent
	public boolean readBoolean(long fieldId) {
		assert(typeData.fieldType(fieldId) == StructTypes.Boolean);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>0) {
			channelReader.position(index);
			return channelReader.readBoolean();
		} else {
			return false;
		}
	}	
	
	//returns false when absent
	public boolean readBoolean(Object association) {
		return readBoolean(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}	
	
	
	//returns -1 when absent
	public int readInt(long fieldId) {
		assert(typeData.fieldType(fieldId) == StructTypes.Short ||
				   typeData.fieldType(fieldId) == StructTypes.Integer ||
			   	   typeData.fieldType(fieldId) == StructTypes.Long);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>0) {
			channelReader.position(index);
			return (int)channelReader.readPackedLong();
		} else {
			return -1;
		}
	}
		
	//returns -1 when absent
	public int readInt(Object association) {
		return readInt(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}

	//returns -1 when absent
	public long readLong(long fieldId) {
		assert(typeData.fieldType(fieldId) == StructTypes.Short ||
				   typeData.fieldType(fieldId) == StructTypes.Integer ||
			   	   typeData.fieldType(fieldId) == StructTypes.Long);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>0) {
			channelReader.position(index);
			return channelReader.readPackedLong();
		} else {
			return -1;
		}
	}
	
	//returns -1 when absent
	public long readLong(Object association) {
		return readLong(typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	///////////////////////////
	//TODO these visits need support for dim added ,, int long and short
	/////////

    //null values are also visited
	public void visitInt(StructIntListener visitor, long fieldId) {
		assert(typeData.fieldType(fieldId) == StructTypes.Short ||
			   typeData.fieldType(fieldId) == StructTypes.Integer ||
			   typeData.fieldType(fieldId) == StructTypes.Long);
    	int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);
    	
    	int instance = 0;
    	int totalCount = 1;
    	
    	if (index>0) {
    		channelReader.position(index);    	
			visitor.value(channelReader.readPackedInt(), false, instance, totalCount);    		
    	} else {
    		visitor.value(0, true, instance, totalCount);
    	}    	
	}
	
	public void visitInt(StructIntListener visitor, Object association) {
		visitInt(visitor, typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}

	public void visitLong(StructLongListener visitor, long fieldId) {
		assert(typeData.fieldType(fieldId) == StructTypes.Short ||
			   typeData.fieldType(fieldId) == StructTypes.Integer ||
			   typeData.fieldType(fieldId) == StructTypes.Long);
    	int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);
    	
    	
    	int instance = 0;
    	int totalCount = 1;
    	
    	if (index>0) {
    		channelReader.position(index);    	
    		visitor.value(channelReader.readPackedLong(), false, instance, totalCount);    		
    	} else {
    		visitor.value(0, true, instance, totalCount);
    	}    	
	}
	
	public void visitLong(StructLongListener visitor, Object association) {
		visitLong(visitor, typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	public void visitShort(StructShortListener visitor, long fieldId) {
		assert(typeData.fieldType(fieldId) == StructTypes.Short ||
			   typeData.fieldType(fieldId) == StructTypes.Integer ||
			   typeData.fieldType(fieldId) == StructTypes.Long);
    	int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);
    	
    	int instance = 0;
    	int totalCount = 1;
    	
    	
    	if (index>0) {
    		channelReader.position(index);    	
    		visitor.value(channelReader.readPackedShort(), false, instance, totalCount);    		
    	} else {
    		visitor.value((short)0, true, instance, totalCount);
    	}    	
	}
	
	public void visitShort(StructShortListener visitor, Object association) {
		visitShort(visitor, typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	////////////////////
	//////////////////
	////////////////

	
	public void visitDimInt(BStructDimIntListener visitor, long fieldId) {
		
		int dims = typeData.dims(fieldId);
		int dinstance = DataInputBlobReader.reserveDimArray(channelReader, dims, typeData.maxDim());		
		int[] dimPos = DataInputBlobReader.lookupDimArray(channelReader, dims, dinstance);
	
		assert(DataInputBlobReader.structTypeValidation((DataInputBlobReader<?>)channelReader, StructRegistry.FIELD_MASK&(int)fieldId)); //set value or check match.
		//assert fieldId is part of StructId
    	int pos = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader, StructRegistry.FIELD_MASK&(int)fieldId);
    	
    	int instance = 0;
    	int totalCount = 1;    	
    	
    	if (pos>=0) {
    		channelReader.position(pos);    	
    		visitor.value(channelReader.readPackedInt(), false, dimPos, instance, totalCount);    		
    	} else {
    		visitor.value(channelReader.readPackedInt(), true, dimPos, instance, totalCount);
    	}
    	
	}
	
	public void visitDimInt(BStructDimIntListener visitor, Object association) {
		visitDimInt(visitor,typeData.fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader))  );
	}

	
	public <A extends Appendable> A readDecimalAsText(Object attachedInstance, A target) {
		
		assert(matchOneOfTypes(attachedInstance, StructTypes.Decimal));
		
		this.channelReader.position(
		this.channelReader.readFromEndLastInt(
				StructRegistry.FIELD_MASK &
				(int)this.typeData.fieldLookupByIdentity(attachedInstance, 
						DataInputBlobReader.getStructType(this.channelReader))));
		
		long m = channelReader.readPackedLong();
    	assert(channelReader.storeMostRecentPacked(m));
    	
    	return Appendables.appendDecimalValue(target, m, 
    											channelReader.readByte());
	}



}
