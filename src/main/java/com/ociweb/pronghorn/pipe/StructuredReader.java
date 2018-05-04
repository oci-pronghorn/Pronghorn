package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.struct.StructBooleanListener;
import com.ociweb.pronghorn.struct.StructByteListener;
import com.ociweb.pronghorn.struct.StructDecimalListener;
import com.ociweb.pronghorn.struct.StructDimIntListener;
import com.ociweb.pronghorn.struct.StructDoubleListener;
import com.ociweb.pronghorn.struct.StructFieldVisitor;
import com.ociweb.pronghorn.struct.StructFloatListener;
import com.ociweb.pronghorn.struct.StructIntListener;
import com.ociweb.pronghorn.struct.StructLongListener;
import com.ociweb.pronghorn.struct.StructRationalListener;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructShortListener;
import com.ociweb.pronghorn.struct.StructType;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.math.Decimal;

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
		
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
				
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
		
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
				
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
		
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
				
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
		
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
				
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
		
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
				
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
		
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
				
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
		
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
				
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
				
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
				
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
		
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
				
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
		
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
				
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
		
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
				
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
		
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
				
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
		
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
				
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
	//support for fields with dimension
	///////////////////////////
	
	public void visitRational(StructRationalListener visitor, Object association) {
		visitRational(visitor, Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	public void visitRational(StructRationalListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Rational );
			
		
    	int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);
    	
		int dims = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId);
    	
    	int totalCount = 1;
    	
    	if (index>=0) {
    		channelReader.position(index);  
    		
    		int count = consumeDimData(0, dims, channelReader);
    		if (count>0) {
    			totalCount = count;
    		}
    		
    		for(int c=0; c<totalCount; c++) {
				long numerator   = channelReader.readPackedLong();
				long denominator = channelReader.readPackedLong();
				
				
				boolean isNull = false;
				if (0==numerator) {
					isNull = channelReader.wasPackedNull();
				}
				visitor.value(numerator, denominator, isNull, c, totalCount);
    		}  
    	} else {
    		visitor.value(0, 1, true, 0, 0);
    	}    	
	}
	
	
	public void visitInt(StructIntListener visitor, Object association) {
		visitInt(visitor, 
				 Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
    //null values are also visited
	public void visitInt(StructIntListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Long);
    	
		
		int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);

		int dims = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId);
    	
    	int totalCount = 1;
    	
    	if (index>=0) {
    		channelReader.position(index);  
    		
    		int count = consumeDimData(0, dims, channelReader);
    		if (count>0) {
    			totalCount = count;
    		}
    		
    		for(int c=0; c<totalCount; c++) {
				int readPackedInt = channelReader.readPackedInt();
				boolean isNull = false;
				if (0==readPackedInt) {
					isNull = channelReader.wasPackedNull();
				}
				visitor.value(readPackedInt, isNull, c, totalCount);
    		}
			
    	} else {
    		visitor.value(0, true, 0, 0);
    	}    	
	}
	
	private int consumeDimData(int count, int dim, DataInputBlobReader<?> reader) {
		if (dim>0) {
			dim--;			
			int c = reader.readPackedInt(); //2  1     5 0  2 0
			if (dim>0) {			
				while (--c>=0) {			
					count = consumeDimData(count, dim, reader);
				}
			} else {
				return count+c;
			}
		}	
		return count;
	}

	
	public void visitLong(StructLongListener visitor, Object association) {
		visitLong(visitor, Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}

	public void visitLong(StructLongListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Long);
    	int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);
    	
		int dims = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId);
    	
    	int totalCount = 1;
    	
    	if (index>=0) {
    		channelReader.position(index); 
    		
    		
    		int count = consumeDimData(0, dims, channelReader);
    		if (count>0) {
    			totalCount = count;
    		}
    		
    		for(int c=0; c<totalCount; c++) {
				long readPackedLong = channelReader.readPackedLong();
				boolean isNull = false;
				if (0==readPackedLong) {
					isNull = channelReader.wasPackedNull();
				}
				visitor.value(readPackedLong, isNull, c, totalCount);
    		}
			    		
    	} else {
    		visitor.value(0, true, 0, 0);
    	}    	
	}

	
	public void visitShort(StructShortListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Short);
    	int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);
    	
		int dims = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId);
    	
    	int totalCount = 1;
    	
    	
    	if (index>=0) {
    		channelReader.position(index);
    		
    		int count = consumeDimData(0, dims, channelReader);
    		if (count>0) {
    			totalCount = count;
    		}
    		
    		for(int c=0; c<totalCount; c++) {
				short readPackedShort = channelReader.readPackedShort();
				boolean isNull = false;
				if (0==readPackedShort) {
					isNull = channelReader.wasPackedNull();
				}
				visitor.value(readPackedShort, isNull, c, totalCount);
    		}    		
    	} else {
    		visitor.value((short)0, true, 0, 0);
    	}    	
	}
	
	public void visitShort(StructShortListener visitor, Object association) {
		visitShort(visitor, Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	
	public void visitByte(StructByteListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Byte);
    	int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);
    	
		int dims = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId);
    	
    	int totalCount = 1;
    	
    	
    	if (index>=0) {
    		channelReader.position(index);
    		
    		int count = consumeDimData(0, dims, channelReader);
    		if (count>0) {
    			totalCount = count;
    		}
    		
    		for(int c=0; c<totalCount; c++) {
				byte value = channelReader.readByte();
				visitor.value(value, false, c, totalCount);
    		}    		
    	} else {
    		visitor.value((byte)0, true, 0, 0);
    	}    	
	}
	
	public void visitByte(StructByteListener visitor, Object association) {
		visitByte(visitor, Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
	
	public void visitFloat(StructFloatListener visitor, Object association) {
		visitFloat(visitor, 
				 Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
 	public void visitFloat(StructFloatListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Float);
    	
		
		int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);

		int dims = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId);
    	
    	int totalCount = 1;
    	
    	if (index>=0) {
    		channelReader.position(index);  
    		
    		int count = consumeDimData(0, dims, channelReader);
    		if (count>0) {
    			totalCount = count;
    		}
    		
    		for(int c=0; c<totalCount; c++) {    			
    			float value = Float.intBitsToFloat(channelReader.readPackedInt());    			
				visitor.value(value, Float.isNaN(value), c, totalCount);
    		}
			
    	} else {
    		visitor.value(0, true, 0, 0);
    	}    	
	}
	
	public void visitDouble(StructDoubleListener visitor, Object association) {
		visitDouble(visitor, 
				 Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
 	public void visitDouble(StructDoubleListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Double);
    	
		
		int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);

		int dims = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId);
    	
    	int totalCount = 1;
    	
    	if (index>=0) {
    		channelReader.position(index);  
    		
    		int count = consumeDimData(0, dims, channelReader);
    		if (count>0) {
    			totalCount = count;
    		}
    		
    		for(int c=0; c<totalCount; c++) {    			
    			double value = Double.longBitsToDouble(channelReader.readPackedLong());    			
				visitor.value(value, Double.isNaN(value), c, totalCount);
    		}
			
    	} else {
    		visitor.value(0, true, 0, 0);
    	}    	
	}
	
	public void visitBoolean(StructBooleanListener visitor, Object association) {
		visitBoolean(visitor, 
				 Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
 	public void visitBoolean(StructBooleanListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Double);
    	
		
		int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);

		int dims = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId);
    	
    	int totalCount = 1;
    	
    	if (index>=0) {
    		channelReader.position(index);  
    		
    		int count = consumeDimData(0, dims, channelReader);
    		if (count>0) {
    			totalCount = count;
    		}
    		
    		for(int c=0; c<totalCount; c++) {  
    			boolean value = channelReader.readBoolean();
    			boolean isNull = false;
    			if (!value) {
    				isNull = channelReader.wasBooleanNull();
    			}   			
				visitor.value(value, isNull, c, totalCount);
    		}
			
    	} else {
    		visitor.value(false, true, 0, 0);
    	}    	
	}
 	
 	public static int structType(StructuredReader that) {
 		return DataInputBlobReader.getStructType(that.channelReader); 
 	}
 	
	public void visitDecimal(StructDecimalListener visitor, Object association) {
		visitDecimal(visitor, 
				 Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
 	public void visitDecimal(StructDecimalListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Double);
    	
		
		int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);

		int dims = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId);
    	
    	int totalCount = 1;
    	
    	if (index>=0) {
    		channelReader.position(index);  
    		
    		int count = consumeDimData(0, dims, channelReader);
    		if (count>0) {
    			totalCount = count;
    		}
    		
    		for(int c=0; c<totalCount; c++) {    			
    			long m = channelReader.readPackedLong();
    	    	assert(channelReader.storeMostRecentPacked(m));
    	    	if (0!=m) {
    	    		visitor.value(channelReader.readByte(), m, false, c, totalCount);
    	    	} else {
    	    		if (!channelReader.wasPackedNull()) {
    	    			visitor.value(channelReader.readByte(), m, false, c, totalCount);
    	    		} else {
    	    			visitor.value((byte)0, 0, true, 0, 0);
    	    			channelReader.readByte();
    	    		}
    	    	}
    		}
			
    	} else {
    		visitor.value((byte)0, 0, true, 0, 0);
    	}    	
	}
 	//TODO: add visit of text...
 	
 	
	////////////////////////
	///////////////////////
	//////////////////////
	////////////////////
	//////////////////
	////////////////


	public void visitIntByDim(StructDimIntListener visitor, Object association) {
		visitIntByDim(visitor, 
				 Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(channelReader)));
	}
	
    //null values are also visited
	public void visitIntByDim(StructDimIntListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Long);
    	
		
		int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);

		int dims = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId);
    	
    	int totalCount = 1;
    	
    	if (index>=0) {
    		channelReader.position(index);  
    		
    		int count = consumeDimData(0, dims, channelReader);
    		if (count>0) {
    			totalCount = count;
    		}
    		
    		for(int c=0; c<totalCount; c++) {
				int readPackedInt = channelReader.readPackedInt();
				boolean isNull = false;
				if (0==readPackedInt) {
					isNull = channelReader.wasPackedNull();
				}
			//	visitor.value(readPackedInt, isNull, c, totalCount);
    		}
			
    	} else {
    	//	visitor.value(0, true, 0, 0);
    	}    	
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

	public boolean readLong(Object association, StructuredWriter output) {
				
		final long fieldId = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(this.channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(this.channelReader));
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
		
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Short ||
				Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Integer ||
				Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Long);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			
			//TODO: new method copyPackedLong which moves bytes until end is found
			output.writeLong(association, channelReader.readPackedLong());
			return true;
		}
		return false;
		
	}

	public boolean readInt(Object association, StructuredWriter output) {
		
		final long fieldId = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(this.channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(this.channelReader));
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
		
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Short ||
				Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Integer ||
				Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Long);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			
			//TODO: new method copyPackedLong which moves bytes until end is found
			output.writeInt(association, channelReader.readPackedInt());
			return true;
		}
		return false;
		
	}
	
	public boolean readShort(Object association, StructuredWriter output) {
		
		final long fieldId = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(this.channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(this.channelReader));
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
		
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Short ||
				Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Integer ||
				Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Long);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			
			//TODO: new method copyPackedLong which moves bytes until end is found
			output.writeShort(association, channelReader.readPackedShort());
			return true;
		}
		return false;
		
	}
	
	public boolean readBoolean(Object association, StructuredWriter output) {
		
		final long fieldId = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(this.channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(this.channelReader));
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
		
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Boolean);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			
			//TODO: new method copyPackedLong which moves bytes until end is found
			output.writeBoolean(association, channelReader.readBoolean());
			return true;
		}
		return false;
		
	}

	public boolean readText(Object association, StructuredWriter output) {
		
		final long fieldId = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(this.channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(this.channelReader));
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
		
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Text);
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			
			int length = channelReader.readShort();//length of text			
			ChannelWriter out = output.writeBlob(association);
			out.writeShort(length);
			channelReader.readInto(out, length);
			
			return true;
		}
		return false;
		
	}

	public boolean readDecimal(Object association, StructuredWriter output) {
		
		final long fieldId = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(this.channelReader)).fieldLookupByIdentity(association, DataInputBlobReader.getStructType(this.channelReader));
		assert(0==Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId)) : "This method only used for non dim fields.";
		
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Decimal);
		
		int index = channelReader.readFromEndLastInt(StructRegistry.FIELD_MASK&(int)fieldId);
		if (index>=0) {
			channelReader.position(index);
			
			long m = channelReader.readPackedLong();
	    	assert(channelReader.storeMostRecentPacked(m));
	    	if (0==m && channelReader.wasPackedNull()) {
	    		channelReader.readByte();
	    		return false;
	    	} else {
	    		byte e = channelReader.readByte();
	    		output.writeDecimal(association, m, e);
	    	}
			return true;
		}
		return false;
		
	}
	

}
