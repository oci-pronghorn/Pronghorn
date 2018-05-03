package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.struct.BStructDimIntListener;
import com.ociweb.pronghorn.struct.StructShortListener;
import com.ociweb.pronghorn.struct.StructLongListener;
import com.ociweb.pronghorn.struct.StructRationalListener;
import com.ociweb.pronghorn.struct.StructFieldVisitor;
import com.ociweb.pronghorn.struct.StructIntListener;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructType;
import com.ociweb.pronghorn.util.Appendables;

public final class StructuredReader {

	public static final int PAYLOAD_INDEX_LOCATION = 0;
	private final DataInputBlobReader<?> channelReader; 
	
	public StructuredReader(DataInputBlobReader<?> reader) {
		this.channelReader = reader;		
	}

	public <T> void visit(Class<T> attachedInstanceOf, StructFieldVisitor<T> visitor) {		
		Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).visit(channelReader, attachedInstanceOf, visitor);
	}
	
	public <T,B extends T> boolean identityVisit(B attachedInstance, StructFieldVisitor<T> visitor) {		
		return Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).identityVisit(channelReader, attachedInstance, visitor);
	}
	

	private final boolean matchOneOfTypes(Object attachedInstance, StructType ... assoc) {
		long fieldId = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(attachedInstance, 
				DataInputBlobReader.getStructType(channelReader));	
		return matchOneOfTypes(fieldId, assoc);
	}

	private final boolean matchOneOfTypes(long fieldId, StructType... assoc) {
		boolean ok = false;
		StructType fieldType = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId);
		int i=assoc.length;
		while (--i>=0) {
			if (assoc[i]==fieldType) {
				ok = true;
			}
		}
		return ok;
	}
	
	
	public final int fullIndexSizeInBytes() {
		return 4+(4*Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).totalSizeOfIndexes(DataInputBlobReader.getStructType(channelReader)));
	}
	
	//set to a position for general reading unless index position is not provided
	public ChannelReader read(long fieldId) {
		final int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
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
		return read(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	public boolean isNull(Object association) {
		return isNull(
				Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader))
				    .fieldLookupByIdentity(association, 
				    		DataInputBlobReader.getStructType(channelReader) ));
	}
	
	public boolean hasValue(Object association) {
		return hasValue(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	//returns null if absent
	public String readText(long fieldId) {
	
		final int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			return channelReader.readUTF();
		} else {
			return null;
		}
	}
	
	//returns null if absent
	public String readText(Object association) {
		return readText(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}

	public boolean isEqual(long fieldId, byte[] utf8EncodedBytes) {

		final int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			if (channelReader.available()>=2) {
				int length = channelReader.readShort(); 				
				return utf8EncodedBytes.length==length && channelReader.equalBytes(utf8EncodedBytes);
			} 
		}
		return false;
	}

	public boolean isEqual(Object association, byte[] utf8EncodedBytes) {
		return isEqual(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)), utf8EncodedBytes);
	}
	
	//returns -1 when absent
	public long readTextAsLong(long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Text);
		final int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			return DataInputBlobReader.readUTFAsLong(channelReader);
		} else {
			return -1;
		}
	}
	
	//returns -1 when absent
	public long readTextAsLong(Object association) {
		return readTextAsLong(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	//returns -1 when absent
	public double readTextAsDouble(long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Text);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			return DataInputBlobReader.readUTFAsDecimal(channelReader);
		} else {
			return -1;
		}
	}
	
	//returns -1 when absent
	public double readTextAsDouble(Object association) {
		return readTextAsDouble(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	//appends nothing when absent
	public <A extends Appendable> A readText(long fieldId, A target) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Text);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			channelReader.readUTF(target);
		}
		return target;
	}
	
	//appends nothing when absent
	public <A extends Appendable> A readText(Object association, A target) {
		return readText(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)), target);
	}
	
	//appends nothing when absent
	public <A extends Appendable> A readIntAsText(long fieldId, A target) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Short ||
			   Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Integer ||
			   Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Long);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			return Appendables.appendValue(target, channelReader.readPackedLong());
		} else {
			return target;
		}
	}
	
	//appends nothing when absent
	public <A extends Appendable> A readIntAsText(Object association, A target) {
		return readIntAsText(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)), target);
	}
	

	//method split out because NaN usage has a side effect of preventing HotSpot optimizations
	private double nullReadOfDouble() {
		return Double.NaN;
	}
		
	//return NaN when field is absent
	public double readRationalAsDouble(long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Rational);
		final int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			return channelReader.readRationalAsDouble();
		} else {
			return nullReadOfDouble();
		}
	}

	//return NaN when field is absent
	public double readRationalAsDouble(Object association) {
		return readRationalAsDouble(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
		
	//return NaN when field is absent
	public double readDecimalAsDouble(long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Decimal);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			return channelReader.readDecimalAsDouble();
		} else {
			return nullReadOfDouble();
		}
	}
	
	//return NaN when field is absent
	public double readDecimalAsDouble(Object association) {
		return readDecimalAsDouble(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	//returns -1 when absent
	public long readDecimalMantissa(long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Decimal);
		final int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
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
		return readDecimalMantissa(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
		
	//returns 0 when absent
	public byte readDecimalExponent(long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Decimal);
		final int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
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
		return readDecimalExponent(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
		
	//returns false when absent
	public boolean readBoolean(long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Boolean);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			return channelReader.readBoolean();
		} else {
			return false;
		}
	}	
	
	//returns false when absent
	public boolean readBoolean(Object association) {
		return readBoolean(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}	
	
	
	//returns -1 when absent
	public int readInt(long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Short ||
				Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Integer ||
						Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Long);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			return (int)channelReader.readPackedLong();
		} else {
			return -1;
		}
	}
		
	//returns -1 when absent
	public int readInt(Object association) {
		return readInt(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}

	//returns -1 when absent
	public long readLong(long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Short ||
				Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Integer ||
				Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Long);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			return channelReader.readPackedLong();
		} else {
			return -1;
		}
	}
	
	//returns -1 when absent
	public long readLong(Object association) {
		return readLong(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	///////////////////////////
	//TODO these visits need support for dim added ,, int long and short
	/////////
	
	public void visitRational(StructRationalListener visitor, Object association) {
		visitRational(visitor, Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	public void visitRational(StructRationalListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Rational );
			
		
    	int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);
    	
    	int instance = 0;
    	int totalCount = 1;
    	
    	if (index>=0) {
    		channelReader.position(index);  
    		visitor.value(channelReader.readPackedLong(), channelReader.readPackedLong(), false, instance, totalCount);  		
    	} else {
    		visitor.value(0, 1, true, instance, totalCount);
    	}    	
	}
	
	
    //null values are also visited
	public void visitInt(StructIntListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Short ||
				Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Integer ||
						Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Long);
    	int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);
    	
    	int instance = 0;
    	int totalCount = 1;
    	
    	if (index>=0) {
    		channelReader.position(index);    	
			visitor.value(channelReader.readPackedInt(), false, instance, totalCount);    		
    	} else {
    		visitor.value(0, true, instance, totalCount);
    	}    	
	}
	
	public void visitInt(StructIntListener visitor, Object association) {
		visitInt(visitor, Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}

	public void visitLong(StructLongListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Short ||
				Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Integer ||
						Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Long);
    	int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);
    	
    	
    	int instance = 0;
    	int totalCount = 1;
    	
    	if (index>=0) {
    		channelReader.position(index);    	
    		visitor.value(channelReader.readPackedLong(), false, instance, totalCount);    		
    	} else {
    		visitor.value(0, true, instance, totalCount);
    	}    	
	}
	
	public void visitLong(StructLongListener visitor, Object association) {
		visitLong(visitor, Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	public void visitShort(StructShortListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Short ||
				Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Integer ||
						Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Long);
    	int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);
    	
    	int instance = 0;
    	int totalCount = 1;
    	
    	
    	if (index>=0) {
    		channelReader.position(index);    	
    		visitor.value(channelReader.readPackedShort(), false, instance, totalCount);    		
    	} else {
    		visitor.value((short)0, true, instance, totalCount);
    	}    	
	}
	
	public void visitShort(StructShortListener visitor, Object association) {
		visitShort(visitor, Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	////////////////////
	//////////////////
	////////////////

	
	public void visitDimInt(BStructDimIntListener visitor, long fieldId) {
		
		int dims = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId);
		int dinstance = DataInputBlobReader.reserveDimArray(channelReader, dims, Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).maxDim());		
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
		visitDimInt(visitor,
				   Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader))
				       .fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader))  );
	}

	
	public <A extends Appendable> A readDecimalAsText(Object attachedInstance, A target) {
		
		assert(matchOneOfTypes(attachedInstance, StructType.Decimal));
		
		this.channelReader.position(
		this.channelReader.readFromEndLastInt(
				StructRegistry.FIELD_MASK &
				(int)Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(attachedInstance, 
						DataInputBlobReader.getStructType(this.channelReader))));
		
		long m = channelReader.readPackedLong();
    	assert(channelReader.storeMostRecentPacked(m));
    	
    	return Appendables.appendDecimalValue(target, m, 
    											channelReader.readByte());
	}



}
