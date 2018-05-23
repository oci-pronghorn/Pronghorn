package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.pipe.util.IntArrayPool;
import com.ociweb.pronghorn.pipe.util.IntArrayPoolLocal;
import com.ociweb.pronghorn.struct.StructBlobListener;
import com.ociweb.pronghorn.struct.StructBooleanListener;
import com.ociweb.pronghorn.struct.StructByteListener;
import com.ociweb.pronghorn.struct.StructDecimalListener;
import com.ociweb.pronghorn.struct.StructDoubleListener;
import com.ociweb.pronghorn.struct.StructFieldVisitor;
import com.ociweb.pronghorn.struct.StructFloatListener;
import com.ociweb.pronghorn.struct.StructIntListener;
import com.ociweb.pronghorn.struct.StructLongListener;
import com.ociweb.pronghorn.struct.StructRationalListener;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructShortListener;
import com.ociweb.pronghorn.struct.StructTextListener;
import com.ociweb.pronghorn.struct.StructType;
import com.ociweb.pronghorn.util.Appendables;

public final class StructuredReader {

	public static final int PAYLOAD_INDEX_LOCATION = 0;
	private final DataInputBlobReader<?> channelReader;
	private static int[] EMPTY = new int[0];
	
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

	/**
	 * Reads text at given field
	 * @param fieldId to read from
	 * @return text if data exists, else <code>null</code>
	 */
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
	    		final int dimPos = channelReader.absolutePosition();
	    		int count = consumeDimData(0, dims, channelReader);
	    		if (count>0) {
	    			totalCount = count;
	    			IntArrayPool local = IntArrayPoolLocal.get();
	    			final int instanceKeyP = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyS = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyD = IntArrayPool.lockInstance(local, 1);
	    				    			
	    			final int[] curPos = IntArrayPool.getArray(local, dims, instanceKeyP);
	    			final int[] curSize = IntArrayPool.getArray(local, dims, instanceKeyS);
	    		    final int[] curData = IntArrayPool.getArray(local, 1, instanceKeyD);
	    			curData[0] = channelReader.absolutePosition();
	    			
	    			try {
	    				channelReader.absolutePosition(dimPos);
	    				visitRationals(visitor, totalCount, curPos, curSize, curData);
	    			} finally {
	    				IntArrayPool.releaseLock(local, dims, instanceKeyP);
	    				IntArrayPool.releaseLock(local, dims, instanceKeyS);
	    			}
	    		} else {
	    			visitRationals(visitor, totalCount, EMPTY, EMPTY, EMPTY); 
	    		}
    	} else {
    		visitor.value(0, 1, true, EMPTY, EMPTY, 0, 0);
    	}    	
	}

	private void visitRationals(StructRationalListener visitor, int totalCount, 
            final int[] curPos, 
            final int[] curSize, 
            final int[] curData) {
	
		consumeDimRationalData(visitor, totalCount, curPos, curSize, curData,
			           0, curSize.length,
			           channelReader);
		
	}

	private int consumeDimRationalData(StructRationalListener visitor, int totalCount,
	                   final int[] curPos, final int[] curSize, final int[] curData,
	                   int count, int dim, 
	                   DataInputBlobReader<?> reader) {
	if (dim>0) {
	dim--;			
	int c = reader.readPackedInt(); //2  1     5 0  2 0
	if (dim>0) {	
		final int idx = curPos.length-(1+dim);
		curSize[idx] = c;
		for(int i=0; i<c; i++) {			
			curPos[idx] = i;
			count += consumeDimRationalData(visitor, totalCount, 
					                   curPos, curSize, curData, 
					                   count, dim, reader);
		}
	} else {
		//dim is zero so we have reached the data
		
		int dimPos = reader.absolutePosition();
	
		if (curData.length==1) {
			//restore the data position
			reader.absolutePosition(curData[0]);
		}
		
		final int idx = curPos.length-1;
		curSize[idx] = c;
		for(int i=0; i<c; i++) {
			curPos[idx] = i;
			visitSingleRational(visitor, totalCount, curPos, curSize, count+i);
		}
		
		if (curData.length==1) {
			//store the current data position again
			curData[0] = reader.absolutePosition();
		}
		
		//restore the old position to read the next dim
		reader.absolutePosition(dimPos);
						
		return count+c;
	}
	}	
	return count;
	}

	private void visitSingleRational(StructRationalListener visitor, int totalCount, final int[] curPos,
			final int[] curSize, int c) {
		long numerator   = channelReader.readPackedLong();
		long denominator = channelReader.readPackedLong();

		
		boolean isNull = false;
		if (0==numerator) {
			isNull = channelReader.wasPackedNull();
		}
		visitor.value(numerator, denominator, isNull, curPos, curSize, c, totalCount);
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

		final int dims = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId);
    	
    	int totalCount = 1;
    	
    	if (index>=0) {
    		channelReader.position(index);  
    		final int dimPos = channelReader.absolutePosition();
    		int count = consumeDimData(0, dims, channelReader);
    		if (count>0) {
    			totalCount = count;
    			
    			IntArrayPool local = IntArrayPoolLocal.get();
    			final int instanceKeyP = IntArrayPool.lockInstance(local, dims);
    			final int instanceKeyS = IntArrayPool.lockInstance(local, dims);
    			final int instanceKeyD = IntArrayPool.lockInstance(local, 1);
    			    			
    			final int[] curPos = IntArrayPool.getArray(local, dims, instanceKeyP);
    			final int[] curSize = IntArrayPool.getArray(local, dims, instanceKeyS);
    		    final int[] curData = IntArrayPool.getArray(local, 1, instanceKeyD);
    			curData[0] = channelReader.absolutePosition();
    			
    			try {  
    				channelReader.absolutePosition(dimPos);
    				visitInts(visitor, totalCount, curPos, curSize, curData);				
    			} finally {
    				IntArrayPool.releaseLock(local, dims, instanceKeyP);
    				IntArrayPool.releaseLock(local, dims, instanceKeyS);
    				IntArrayPool.releaseLock(local, 1, instanceKeyD);
    			}
    			
    		} else {
    			visitInts(visitor, totalCount, EMPTY, EMPTY, EMPTY);
    		}
    	} else {
    		visitor.value(0, true, EMPTY, EMPTY, 0, 0);
    	}    	
	}

	private void visitInts(StructIntListener visitor, int totalCount, 
			               final int[] curPos, 
			               final int[] curSize, 
			               final int[] curData) {
				
		consumeDimIntData(visitor, totalCount, curPos, curSize, curData,
				           0, curSize.length,
				           channelReader);
		
	}

	private int consumeDimIntData(StructIntListener visitor, int totalCount,
			                      final int[] curPos, final int[] curSize, final int[] curData,
			                      int count, int dim, 
			                      DataInputBlobReader<?> reader) {
		if (dim>0) {
			dim--;			
			int c = reader.readPackedInt(); //2  1     5 0  2 0
			if (dim>0) {	
				final int idx = curPos.length-(1+dim);
				curSize[idx] = c;
				for(int i=0; i<c; i++) {			
					curPos[idx] = i;
					count += consumeDimIntData(visitor, totalCount, 
							                   curPos, curSize, curData, 
							                   count, dim, reader);
				}
			} else {
				//dim is zero so we have reached the data
				
				int dimPos = reader.absolutePosition();
	
				if (curData.length==1) {
					//restore the data position
					reader.absolutePosition(curData[0]);
				}
				
				final int idx = curPos.length-1;
				curSize[idx] = c;
				for(int i=0; i<c; i++) {
					curPos[idx] = i;
					visitSingleInt(visitor, totalCount, curPos, curSize, count+i);
				}
				
				if (curData.length==1) {
					//store the current data position again
					curData[0] = reader.absolutePosition();
				}
				
				//restore the old position to read the next dim
				reader.absolutePosition(dimPos);
								
				return count+c;
			}
		}	
		return count;
	}

	private void visitSingleInt(StructIntListener visitor, int totalCount,
			final int[] curPos, final int[] curSize, int c) {
		final int readPackedInt = channelReader.readPackedInt();
		boolean isNull = false;
		if (0==readPackedInt) {
			isNull = channelReader.wasPackedNull();
		}
		visitor.value(readPackedInt, isNull, curPos, curSize, c, totalCount);
	}
	
	public void visitBlob(StructBlobListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Long);
    	
		
		int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);

		final int dims = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId);
    	
    	int totalCount = 1;
    	
    	if (index>=0) {
    		channelReader.position(index);  
    		final int dimPos = channelReader.absolutePosition();
    		int count = consumeDimData(0, dims, channelReader);
    		if (count>0) {
    			totalCount = count;
    			
    			IntArrayPool local = IntArrayPoolLocal.get();
    			final int instanceKeyP = IntArrayPool.lockInstance(local, dims);
    			final int instanceKeyS = IntArrayPool.lockInstance(local, dims);
    			final int instanceKeyD = IntArrayPool.lockInstance(local, 1);
    			    			
    			final int[] curPos = IntArrayPool.getArray(local, dims, instanceKeyP);
    			final int[] curSize = IntArrayPool.getArray(local, dims, instanceKeyS);
    		    final int[] curData = IntArrayPool.getArray(local, 1, instanceKeyD);
    			curData[0] = channelReader.absolutePosition();
    			
    			try {  
    				channelReader.absolutePosition(dimPos);
    				visitBlobs(visitor, totalCount, curPos, curSize, curData);				
    			} finally {
    				IntArrayPool.releaseLock(local, dims, instanceKeyP);
    				IntArrayPool.releaseLock(local, dims, instanceKeyS);
    				IntArrayPool.releaseLock(local, 1, instanceKeyD);
    			}
    			
    		} else {
    			visitBlobs(visitor, totalCount, EMPTY, EMPTY, EMPTY);
    		}
    	} else {
    		visitor.value(null, EMPTY, EMPTY, 0, 0);
    	}    	
	}
	
	private void visitBlobs(StructBlobListener visitor, int totalCount, 
            final int[] curPos, 
            final int[] curSize, 
            final int[] curData) {
	
		consumeDimBlobData(visitor, totalCount, curPos, curSize, curData,
			           0, curSize.length,
			           channelReader);
		
	}
	
	private int consumeDimBlobData(StructBlobListener visitor, int totalCount, final int[] curPos, final int[] curSize,
			final int[] curData, int count, int dim, DataInputBlobReader<?> reader) {
		if (dim > 0) {
			dim--;
			int c = reader.readPackedInt(); // 2 1 5 0 2 0
			if (dim > 0) {
				final int idx = curPos.length - (1 + dim);
				curSize[idx] = c;
				for (int i = 0; i < c; i++) {
					curPos[idx] = i;
					count += consumeDimBlobData(visitor, totalCount, curPos, curSize, curData, count, dim, reader);
				}
			} else {
				// dim is zero so we have reached the data

				int dimPos = reader.absolutePosition();

				if (curData.length == 1) {
					// restore the data position
					reader.absolutePosition(curData[0]);
				}

				final int idx = curPos.length - 1;
				curSize[idx] = c;
				for (int i = 0; i < c; i++) {
					curPos[idx] = i;
					visitSingleBlob(visitor, totalCount, curPos, curSize, count + i);
				}

				if (curData.length == 1) {
					// store the current data position again
					curData[0] = reader.absolutePosition();
				}

				// restore the old position to read the next dim
				reader.absolutePosition(dimPos);

				return count + c;
			}
		}
		return count;
	}
	
	
	private void visitSingleBlob(StructBlobListener visitor, int totalCount,
			final int[] curPos, final int[] curSize, int c) {
		
		visitor.value(channelReader, curPos, curSize, c, totalCount);
	
	}
	
	public void visitText(StructTextListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Long);
    	
		
		int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);

		final int dims = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId);
    	
    	int totalCount = 1;
    	
    	if (index>=0) {
    		channelReader.position(index);  
    		final int dimPos = channelReader.absolutePosition();
    		int count = consumeDimData(0, dims, channelReader);
    		if (count>0) {
    			totalCount = count;
    			
    			IntArrayPool local = IntArrayPoolLocal.get();
    			final int instanceKeyP = IntArrayPool.lockInstance(local, dims);
    			final int instanceKeyS = IntArrayPool.lockInstance(local, dims);
    			final int instanceKeyD = IntArrayPool.lockInstance(local, 1);
    			    			
    			final int[] curPos = IntArrayPool.getArray(local, dims, instanceKeyP);
    			final int[] curSize = IntArrayPool.getArray(local, dims, instanceKeyS);
    		    final int[] curData = IntArrayPool.getArray(local, 1, instanceKeyD);
    			curData[0] = channelReader.absolutePosition();
    			
    			try {  
    				channelReader.absolutePosition(dimPos);
    				visitTexts(visitor, totalCount, curPos, curSize, curData);				
    			} finally {
    				IntArrayPool.releaseLock(local, dims, instanceKeyP);
    				IntArrayPool.releaseLock(local, dims, instanceKeyS);
    				IntArrayPool.releaseLock(local, 1, instanceKeyD);
    			}
    			
    		} else {
    			visitTexts(visitor, totalCount, EMPTY, EMPTY, EMPTY);
    		}
    	} else {
    		visitor.value(channelReader, true, EMPTY, EMPTY, 0, 0);
    	}    	
	}
	
	
	private void visitTexts(StructTextListener visitor, int totalCount, 
            final int[] curPos, 
            final int[] curSize, 
            final int[] curData) {
	
		consumeDimTextData(visitor, totalCount, curPos, curSize, curData,
			           0, curSize.length,
			           channelReader);
		
	}
	
	private int consumeDimTextData(StructTextListener visitor, int totalCount, final int[] curPos, final int[] curSize,
			final int[] curData, int count, int dim, DataInputBlobReader<?> reader) {
		if (dim > 0) {
			dim--;
			int c = reader.readPackedInt(); // 2 1 5 0 2 0
			if (dim > 0) {
				final int idx = curPos.length - (1 + dim);
				curSize[idx] = c;
				for (int i = 0; i < c; i++) {
					curPos[idx] = i;
					count += consumeDimTextData(visitor, totalCount, curPos, curSize, curData, count, dim, reader);
				}
			} else {
				// dim is zero so we have reached the data

				int dimPos = reader.absolutePosition();

				if (curData.length == 1) {
					// restore the data position
					reader.absolutePosition(curData[0]);
				}

				final int idx = curPos.length - 1;
				curSize[idx] = c;
				for (int i = 0; i < c; i++) {
					curPos[idx] = i;
					visitSingleText(visitor, totalCount, curPos, curSize, count + i);
				}

				if (curData.length == 1) {
					// store the current data position again
					curData[0] = reader.absolutePosition();
				}

				// restore the old position to read the next dim
				reader.absolutePosition(dimPos);

				return count + c;
			}
		}
		return count;
	}
	
	
	private void visitSingleText(StructTextListener visitor, int totalCount,
			final int[] curPos, final int[] curSize, int c) {
		visitor.value(channelReader, DataInputBlobReader.peekShort(channelReader)<0, curPos, curSize, c, totalCount);
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
	    		final int dimPos = channelReader.absolutePosition();
	    		int count = consumeDimData(0, dims, channelReader);
	    		if (count>0) {
	    			totalCount = count;
	    			IntArrayPool local = IntArrayPoolLocal.get();
	    			final int instanceKeyP = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyS = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyD = IntArrayPool.lockInstance(local, 1);	    			
	    			
	    			final int[] curPos = IntArrayPool.getArray(local, dims, instanceKeyP);
	    			final int[] curSize = IntArrayPool.getArray(local, dims, instanceKeyS);
	    			final int[] curData = IntArrayPool.getArray(local, 1, instanceKeyD);
	     			curData[0] = channelReader.absolutePosition();
	    			
	    			try {
	    				channelReader.absolutePosition(dimPos);
		    			visitLongs(visitor, totalCount, curPos, curSize, curData);
	    			} finally {
	    				IntArrayPool.releaseLock(local, dims, instanceKeyP);
	    				IntArrayPool.releaseLock(local, dims, instanceKeyS);
	    			}
	    		} else {
	    			visitLongs(visitor, totalCount, EMPTY, EMPTY, EMPTY);
	    		}
    	} else {
    		visitor.value(0, true, EMPTY, EMPTY, 0, 0);
    	}    	
	}

	private void visitLongs(StructLongListener visitor, int totalCount, 
            final int[] curPos, 
            final int[] curSize, 
            final int[] curData) {
	
		consumeDimLongData(visitor, totalCount, curPos, curSize, curData,
			           0, curSize.length,
			           channelReader);
	
	}

	private int consumeDimLongData(StructLongListener visitor, int totalCount,
                   final int[] curPos, final int[] curSize, final int[] curData,
                   int count, int dim, 
                   DataInputBlobReader<?> reader) {
		if (dim>0) {
		dim--;			
		int c = reader.readPackedInt(); //2  1     5 0  2 0
		if (dim>0) {	
			final int idx = curPos.length-(1+dim);
			curSize[idx] = c;
			for(int i=0; i<c; i++) {			
				curPos[idx] = i;
				count += consumeDimLongData(visitor, totalCount, 
						                   curPos, curSize, curData, 
						                   count, dim, reader);
			}
		} else {
			//dim is zero so we have reached the data
			
			int dimPos = reader.absolutePosition();
		
			if (curData.length==1) {
				//restore the data position
				reader.absolutePosition(curData[0]);
			}
			
			final int idx = curPos.length-1;
			curSize[idx] = c;
			for(int i=0; i<c; i++) {
				curPos[idx] = i;
				visitSingleLong(visitor, totalCount, curPos, curSize, count+i);
			}
			
			if (curData.length==1) {
				//store the current data position again
				curData[0] = reader.absolutePosition();
			}
			
			//restore the old position to read the next dim
			reader.absolutePosition(dimPos);
							
			return count+c;
		}
		}	
		return count;
	}

	private void visitSingleLong(StructLongListener visitor, int totalCount, final int[] curPos, final int[] curSize,
			int c) {
		long readPackedLong = channelReader.readPackedLong();
		boolean isNull = false;
		if (0==readPackedLong) {
			isNull = channelReader.wasPackedNull();
		}
		visitor.value(readPackedLong, isNull, curPos, curSize, c, totalCount);
	}

	
	public void visitShort(StructShortListener visitor, long fieldId) {
		assert(Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).fieldType(fieldId) == StructType.Short);
    	int index = DataInputBlobReader.readFromLastInt((DataInputBlobReader<?>) channelReader,
    									StructRegistry.FIELD_MASK&(int)fieldId);
    	
		int dims = Pipe.structRegistry(DataInputBlobReader.getBackingPipe(channelReader)).dims(fieldId);
    	
    	int totalCount = 1;
    	    	
    	if (index>=0) {
	    		channelReader.position(index);
	    		final int dimPos = channelReader.absolutePosition();
	    		int count = consumeDimData(0, dims, channelReader);
	    		if (count>0) {
	    			totalCount = count;
	    			IntArrayPool local = IntArrayPoolLocal.get();
	    			final int instanceKeyP = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyS = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyD = IntArrayPool.lockInstance(local, 1);	
	    			
	    			final int[] curPos = IntArrayPool.getArray(local, dims, instanceKeyP);
	    			final int[] curSize = IntArrayPool.getArray(local, dims, instanceKeyS);
	    			final int[] curData = IntArrayPool.getArray(local, 1, instanceKeyD);
	     			curData[0] = channelReader.absolutePosition();
	     			
	    			try {
	    				channelReader.absolutePosition(dimPos);
	    				visitShorts(visitor, totalCount, curPos, curSize, curData); 
	    			} finally {
	    				IntArrayPool.releaseLock(local, dims, instanceKeyP);
	    				IntArrayPool.releaseLock(local, dims, instanceKeyS);
	    			}
	    		} else {
	    			visitShorts(visitor, totalCount, EMPTY, EMPTY, EMPTY); 
	    		}
    	} else {
    		visitor.value((short)0, true, EMPTY, EMPTY, 0, 0);
    	}    	
	}

	private void visitShorts(StructShortListener visitor, int totalCount, 
            final int[] curPos, 
            final int[] curSize, 
            final int[] curData) {
	
		consumeDimShortData(visitor, totalCount, curPos, curSize, curData,
			           0, curSize.length,
			           channelReader);
	
	}

	private int consumeDimShortData(StructShortListener visitor, int totalCount,
                   final int[] curPos, final int[] curSize, final int[] curData,
                   int count, int dim, 
                   DataInputBlobReader<?> reader) {
		if (dim>0) {
		dim--;			
		int c = reader.readPackedInt(); //2  1     5 0  2 0
		if (dim>0) {	
			final int idx = curPos.length-(1+dim);
			curSize[idx] = c;
			for(int i=0; i<c; i++) {			
				curPos[idx] = i;
				count += consumeDimShortData(visitor, totalCount, 
						                   curPos, curSize, curData, 
						                   count, dim, reader);
			}
		} else {
			//dim is zero so we have reached the data
			
			int dimPos = reader.absolutePosition();
		
			if (curData.length==1) {
				//restore the data position
				reader.absolutePosition(curData[0]);
			}
			
			final int idx = curPos.length-1;
			curSize[idx] = c;
			for(int i=0; i<c; i++) {
				curPos[idx] = i;
				visitSingleShort(visitor, totalCount, curPos, curSize, count+i);
			}
			
			if (curData.length==1) {
				//store the current data position again
				curData[0] = reader.absolutePosition();
			}
			
			//restore the old position to read the next dim
			reader.absolutePosition(dimPos);
							
			return count+c;
		}
		}	
		return count;
	}

	private void visitSingleShort(StructShortListener visitor, int totalCount, final int[] curPos, final int[] curSize,
			int c) {
		short readPackedShort = channelReader.readPackedShort();
		boolean isNull = false;
		if (0==readPackedShort) {
			isNull = channelReader.wasPackedNull();
		}
		visitor.value(readPackedShort, isNull, curPos, curSize, c, totalCount);
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
	    		final int dimPos = channelReader.absolutePosition();
	    		int count = consumeDimData(0, dims, channelReader);
	    		if (count>0) {
	    			totalCount = count;
	    			IntArrayPool local = IntArrayPoolLocal.get();
	    			final int instanceKeyP = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyS = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyD = IntArrayPool.lockInstance(local, 1);	
	    			
	    			final int[] curPos = IntArrayPool.getArray(local, dims, instanceKeyP);
	    			final int[] curSize = IntArrayPool.getArray(local, dims, instanceKeyS);
	    			final int[] curData = IntArrayPool.getArray(local, 1, instanceKeyD);
	     			curData[0] = channelReader.absolutePosition();
	     			
	    			try {
	    				channelReader.absolutePosition(dimPos);
	    				visitBytes(visitor, totalCount, curPos, curSize, curData);	    				
	    			} finally {
	    				IntArrayPool.releaseLock(local, dims, instanceKeyP);
	    				IntArrayPool.releaseLock(local, dims, instanceKeyS);
	    			}
	    		} else {
	    			visitBytes(visitor, totalCount, EMPTY, EMPTY, EMPTY);  
	    		}
	    		
    	} else {
    		visitor.value((byte)0, true, EMPTY, EMPTY, 0, 0);
    	}    	
	}

	private void visitBytes(StructByteListener visitor, int totalCount, 
            final int[] curPos, 
            final int[] curSize, 
            final int[] curData) {
	
		consumeDimByteData(visitor, totalCount, curPos, curSize, curData,
			           0, curSize.length,
			           channelReader);
	
	}

	private int consumeDimByteData(StructByteListener visitor, int totalCount,
                   final int[] curPos, final int[] curSize, final int[] curData,
                   int count, int dim, 
                   DataInputBlobReader<?> reader) {
		if (dim>0) {
		dim--;			
		int c = reader.readPackedInt(); //2  1     5 0  2 0
		if (dim>0) {	
			final int idx = curPos.length-(1+dim);
			curSize[idx] = c;
			for(int i=0; i<c; i++) {			
				curPos[idx] = i;
				count += consumeDimByteData(visitor, totalCount, 
						                   curPos, curSize, curData, 
						                   count, dim, reader);
			}
		} else {
			//dim is zero so we have reached the data
			
			int dimPos = reader.absolutePosition();
		
			if (curData.length==1) {
				//restore the data position
				reader.absolutePosition(curData[0]);
			}
			
			final int idx = curPos.length-1;
			curSize[idx] = c;
			for(int i=0; i<c; i++) {
				curPos[idx] = i;
				visitSingleByte(visitor, totalCount, curPos, curSize, count+i);
			}
			
			if (curData.length==1) {
				//store the current data position again
				curData[0] = reader.absolutePosition();
			}
			
			//restore the old position to read the next dim
			reader.absolutePosition(dimPos);
							
			return count+c;
		}
		}	
		return count;
	}

	private void visitSingleByte(StructByteListener visitor, int totalCount, final int[] curPos, final int[] curSize,
			int c) {
		byte value = channelReader.readByte();
		visitor.value(value, false, curPos, curSize, c, totalCount);
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
	    		final int dimPos = channelReader.absolutePosition();
	    		int count = consumeDimData(0, dims, channelReader);
	    		if (count>0) {
	    			totalCount = count;
	    			IntArrayPool local = IntArrayPoolLocal.get();
	    			final int instanceKeyP = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyS = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyD = IntArrayPool.lockInstance(local, 1);	
	    			
	    			final int[] curPos = IntArrayPool.getArray(local, dims, instanceKeyP);
	    			final int[] curSize = IntArrayPool.getArray(local, dims, instanceKeyS);
	    			final int[] curData = IntArrayPool.getArray(local, 1, instanceKeyD);
	     			curData[0] = channelReader.absolutePosition();
	     			
	    			try {
	    				channelReader.absolutePosition(dimPos);
	    				visitFloats(visitor, totalCount, curPos, curSize, curData);
	    			} finally {
	    				IntArrayPool.releaseLock(local, dims, instanceKeyP);
	    				IntArrayPool.releaseLock(local, dims, instanceKeyS);
	    			}
	    		} else {
	    			visitFloats(visitor, totalCount, EMPTY, EMPTY, EMPTY);
	    		}
    	} else {
    		visitor.value(0, true, EMPTY, EMPTY, 0, 0);
    	}    	
	}

	private void visitFloats(StructFloatListener visitor, int totalCount, 
            final int[] curPos, 
            final int[] curSize, 
            final int[] curData) {
	
		consumeDimFloatData(visitor, totalCount, curPos, curSize, curData,
			           0, curSize.length,
			           channelReader);
	
	}

	private int consumeDimFloatData(StructFloatListener visitor, int totalCount,
                   final int[] curPos, final int[] curSize, final int[] curData,
                   int count, int dim, 
                   DataInputBlobReader<?> reader) {
		if (dim>0) {
		dim--;			
		int c = reader.readPackedInt(); //2  1     5 0  2 0
		if (dim>0) {	
			final int idx = curPos.length-(1+dim);
			curSize[idx] = c;
			for(int i=0; i<c; i++) {			
				curPos[idx] = i;
				count += consumeDimFloatData(visitor, totalCount, 
						                   curPos, curSize, curData, 
						                   count, dim, reader);
			}
		} else {
			//dim is zero so we have reached the data
			
			int dimPos = reader.absolutePosition();
		
			if (curData.length==1) {
				//restore the data position
				reader.absolutePosition(curData[0]);
			}
			
			final int idx = curPos.length-1;
			curSize[idx] = c;
			for(int i=0; i<c; i++) {
				curPos[idx] = i;
				visitSingleFloat(visitor, totalCount, curPos, curSize, count+i);
			}
			
			if (curData.length==1) {
				//store the current data position again
				curData[0] = reader.absolutePosition();
			}
			
			//restore the old position to read the next dim
			reader.absolutePosition(dimPos);
							
			return count+c;
		}
		}	
		return count;
	}

	private void visitSingleFloat(StructFloatListener visitor, int totalCount, final int[] curPos, final int[] curSize,
			int c) {
		float value = Float.intBitsToFloat(channelReader.readPackedInt());    			
		visitor.value(value, Float.isNaN(value), curPos, curSize, c, totalCount);
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
	    		final int dimPos = channelReader.absolutePosition();
	    		int count = consumeDimData(0, dims, channelReader);
	    		if (count>0) {
	    			totalCount = count;
	    			IntArrayPool local = IntArrayPoolLocal.get();
	    			final int instanceKeyP = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyS = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyD = IntArrayPool.lockInstance(local, 1);	
	    			
	    			final int[] curPos = IntArrayPool.getArray(local, dims, instanceKeyP);
	    			final int[] curSize = IntArrayPool.getArray(local, dims, instanceKeyS);
	    			final int[] curData = IntArrayPool.getArray(local, 1, instanceKeyD);
	     			curData[0] = channelReader.absolutePosition();
	     			
	    			try {
	    				channelReader.absolutePosition(dimPos);
	    				visitDoubles(visitor, totalCount, curPos, curSize, curData);
	    				
	    			} finally {
	    				IntArrayPool.releaseLock(local, dims, instanceKeyP);
	    				IntArrayPool.releaseLock(local, dims, instanceKeyS);
	    			}
	    		} else {
	    			visitDoubles(visitor, totalCount, EMPTY, EMPTY, EMPTY);
	    		}
	    		
    	} else {
    		visitor.value(0, true, EMPTY, EMPTY, 0, 0);
    	}    	
	}

 	private void visitDoubles(StructDoubleListener visitor, int totalCount, 
            final int[] curPos, 
            final int[] curSize, 
            final int[] curData) {
	
		consumeDimDoubleData(visitor, totalCount, curPos, curSize, curData,
			           0, curSize.length,
			           channelReader);
	
	}

	private int consumeDimDoubleData(StructDoubleListener visitor, int totalCount,
                   final int[] curPos, final int[] curSize, final int[] curData,
                   int count, int dim, 
                   DataInputBlobReader<?> reader) {
		if (dim>0) {
		dim--;			
		int c = reader.readPackedInt(); //2  1     5 0  2 0
		if (dim>0) {	
			final int idx = curPos.length-(1+dim);
			curSize[idx] = c;
			for(int i=0; i<c; i++) {			
				curPos[idx] = i;
				count += consumeDimDoubleData(visitor, totalCount, 
						                   curPos, curSize, curData, 
						                   count, dim, reader);
			}
		} else {
			//dim is zero so we have reached the data
			
			int dimPos = reader.absolutePosition();
		
			if (curData.length==1) {
				//restore the data position
				reader.absolutePosition(curData[0]);
			}
			
			final int idx = curPos.length-1;
			curSize[idx] = c;
			for(int i=0; i<c; i++) {
				curPos[idx] = i;
				visitSingleDouble(visitor, totalCount, curPos, curSize, count+i);
			}
			
			if (curData.length==1) {
				//store the current data position again
				curData[0] = reader.absolutePosition();
			}
			
			//restore the old position to read the next dim
			reader.absolutePosition(dimPos);
							
			return count+c;
		}
		}	
		return count;
	}

	private void visitSingleDouble(StructDoubleListener visitor, int totalCount, final int[] curPos,
			final int[] curSize, int c) {
		double value = Double.longBitsToDouble(channelReader.readPackedLong());    			
		visitor.value(value, Double.isNaN(value), curPos, curSize, c, totalCount);
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
	    		final int dimPos = channelReader.absolutePosition();
	    		int count = consumeDimData(0, dims, channelReader);
	    		if (count>0) {
	    			totalCount = count;
	    			IntArrayPool local = IntArrayPoolLocal.get();
	    			final int instanceKeyP = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyS = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyD = IntArrayPool.lockInstance(local, 1);	
	    			
	    			final int[] curPos = IntArrayPool.getArray(local, dims, instanceKeyP);
	    			final int[] curSize = IntArrayPool.getArray(local, dims, instanceKeyS);
	    			final int[] curData = IntArrayPool.getArray(local, 1, instanceKeyD);
	     			curData[0] = channelReader.absolutePosition();
	     			
	    			try {
	    				channelReader.absolutePosition(dimPos);
	    				visitBooleans(visitor, totalCount, curPos, curSize, curData);
	    				
	    			} finally {
	    				IntArrayPool.releaseLock(local, dims, instanceKeyP);
	    				IntArrayPool.releaseLock(local, dims, instanceKeyS);
	    			}
	    		} else {
	    			visitBooleans(visitor, totalCount, EMPTY, EMPTY, EMPTY);
	    		}
    	} else {
    		visitor.value(false, true, EMPTY, EMPTY, 0, 0);
    	}    	
	}

	private void visitBooleans(StructBooleanListener visitor, int totalCount, 
            final int[] curPos, 
            final int[] curSize, 
            final int[] curData) {
	
		consumeDimBooleanData(visitor, totalCount, curPos, curSize, curData,
			           0, curSize.length,
			           channelReader);
	
	}

	private int consumeDimBooleanData(StructBooleanListener visitor, int totalCount,
                   final int[] curPos, final int[] curSize, final int[] curData,
                   int count, int dim, 
                   DataInputBlobReader<?> reader) {
		if (dim>0) {
		dim--;			
		int c = reader.readPackedInt(); //2  1     5 0  2 0
		if (dim>0) {	
			final int idx = curPos.length-(1+dim);
			curSize[idx] = c;
			for(int i=0; i<c; i++) {			
				curPos[idx] = i;
				count += consumeDimBooleanData(visitor, totalCount, 
						                   curPos, curSize, curData, 
						                   count, dim, reader);
			}
		} else {
			//dim is zero so we have reached the data
			
			int dimPos = reader.absolutePosition();
		
			if (curData.length==1) {
				//restore the data position
				reader.absolutePosition(curData[0]);
			}
			
			final int idx = curPos.length-1;
			curSize[idx] = c;
			for(int i=0; i<c; i++) {
				curPos[idx] = i;
				visitSingleBoolean(visitor, totalCount, curPos, curSize, count+i);
			}
			
			if (curData.length==1) {
				//store the current data position again
				curData[0] = reader.absolutePosition();
			}
			
			//restore the old position to read the next dim
			reader.absolutePosition(dimPos);
							
			return count+c;
		}
		}	
		return count;
	}

	private void visitSingleBoolean(StructBooleanListener visitor, int totalCount, final int[] curPos,
			final int[] curSize, int c) {
		//TODO: populate arrays for call 
		
		boolean value = channelReader.readBoolean();
		boolean isNull = false;
		if (!value) {
			isNull = channelReader.wasBooleanNull();
		}   			
		visitor.value(value, isNull, curPos, curSize, c, totalCount);
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
	    		final int dimPos = channelReader.absolutePosition();
	    		int count = consumeDimData(0, dims, channelReader);
	    		if (count>0) {
	    			totalCount = count;
	    			IntArrayPool local = IntArrayPoolLocal.get();
	    			final int instanceKeyP = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyS = IntArrayPool.lockInstance(local, dims);
	    			final int instanceKeyD = IntArrayPool.lockInstance(local, 1);	
	    			
	    			final int[] curPos = IntArrayPool.getArray(local, dims, instanceKeyP);
	    			final int[] curSize = IntArrayPool.getArray(local, dims, instanceKeyS);
	    			final int[] curData = IntArrayPool.getArray(local, 1, instanceKeyD);
	     			curData[0] = channelReader.absolutePosition();
	     			
	    			try {   
	    				channelReader.absolutePosition(dimPos);
	    				visitDecimals(visitor, totalCount, curPos, curSize, curData);
	    			} finally {				
	    				IntArrayPool.releaseLock(local, dims, instanceKeyP);
	    				IntArrayPool.releaseLock(local, dims, instanceKeyS);
	    			}
	    		} else {
	    			visitDecimals(visitor, totalCount, EMPTY, EMPTY, EMPTY);
	    		}
    	} else {
    		visitor.value((byte)0, 0, true, EMPTY, EMPTY, 0, 0);
    	}    	
	}

	private void visitDecimals(StructDecimalListener visitor, int totalCount, 
            final int[] curPos, 
            final int[] curSize, 
            final int[] curData) {
	
		consumeDimDecimalData(visitor, totalCount, curPos, curSize, curData,
			           0, curSize.length,
			           channelReader);
	
	}

	private int consumeDimDecimalData(StructDecimalListener visitor, int totalCount,
                   final int[] curPos, final int[] curSize, final int[] curData,
                   int count, int dim, 
                   DataInputBlobReader<?> reader) {
		if (dim>0) {
		dim--;			
		int c = reader.readPackedInt(); //2  1     5 0  2 0
		if (dim>0) {	
			final int idx = curPos.length-(1+dim);
			curSize[idx] = c;
			for(int i=0; i<c; i++) {			
				curPos[idx] = i;
				count += consumeDimDecimalData(visitor, totalCount, 
						                   curPos, curSize, curData, 
						                   count, dim, reader);
			}
		} else {
			//dim is zero so we have reached the data
			
			int dimPos = reader.absolutePosition();
		
			if (curData.length==1) {
				//restore the data position
				reader.absolutePosition(curData[0]);
			}
			
			final int idx = curPos.length-1;
			curSize[idx] = c;
			for(int i=0; i<c; i++) {
				curPos[idx] = i;
				visitSingleDecimal(visitor, totalCount, curPos, curSize, count+i);
			}
			
			if (curData.length==1) {
				//store the current data position again
				curData[0] = reader.absolutePosition();
			}
			
			//restore the old position to read the next dim
			reader.absolutePosition(dimPos);
							
			return count+c;
		}
		}	
		return count;
	}

	private void visitSingleDecimal(StructDecimalListener visitor, int totalCount, final int[] curPos,
			final int[] curSize, int c) {
		long m = channelReader.readPackedLong();
		assert(channelReader.storeMostRecentPacked(m));
		if (0!=m) {
			visitor.value(channelReader.readByte(), m, false, curPos, curSize, c, totalCount);
		} else {
			if (!channelReader.wasPackedNull()) {
				visitor.value(channelReader.readByte(), m, false, curPos, curSize, c, totalCount);
			} else {
				visitor.value((byte)0, 0, true, null, null, 0, 0);
				channelReader.readByte();
			}
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
			//we know boolean is stored as a single byte so just copy it
			output.writeByte(association, channelReader.readByte());
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
