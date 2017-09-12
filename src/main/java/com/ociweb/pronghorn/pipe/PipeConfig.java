package com.ociweb.pronghorn.pipe;

/**
 * @param <T>  
 */
public class PipeConfig<T extends MessageSchema<T>> {
	
	//try to keep all this under 20MB and 1 RB under 64K if possible under 256K is highly encouraged
	final byte slabBits;
	final byte blobBits;
	final byte[] byteConst;
	final T schema; 
	int debugFlags = 0;
		
   /**
     * This is NOT the constructor you are looking for.
     */
    public PipeConfig(byte primaryBits, byte byteBits, byte[] byteConst, T messageSchema) {
        this.slabBits = primaryBits;
        this.blobBits = byteBits;
        this.byteConst = byteConst;
        this.schema = messageSchema;
     }
    
     public PipeConfig(T messageSchema) {
        //default size which is smaller than half of 64K because this is the L1 cache size on intel haswell.
        this.slabBits = 6;
        this.blobBits = 15;
        this.byteConst = null;
        this.schema = messageSchema;
        //validate
        FieldReferenceOffsetManager.maxVarLenFieldsPerPrimaryRingSize(MessageSchema.from(messageSchema), 1<<slabBits);
     }
	     
     public long totalBytesAllocated() {
    	 return (1L<<blobBits)+(4L<<slabBits);
     }
     
     public byte slabBits() {
    	 return slabBits;
     }
     
     public int minimumFragmentsOnPipe() {
    	 return (1<<slabBits)/FieldReferenceOffsetManager.maxFragmentSize(schema.from);
     }
     
     public int maxVarLenSize() {
    	 return (1<<blobBits)/FieldReferenceOffsetManager.maxVarLenFieldsPerPrimaryRingSize(schema.from, 1<<slabBits);
     }
     
     
    public static <S extends MessageSchema<S>> Pipe<S> pipe(PipeConfig<S> config) {
        return new Pipe<S>(config);
    }
	
	public String toString() {
		return "Primary:"+slabBits+" Secondary:"+blobBits;
	}
	/**
	 * This is the constructor you are looking for.
	 * 
	 * Build a reusable ring configuration object that holds the FROM and ring size definition.  We wait to allocate 
	 * the ring later to support NUMA platforms. In order to wait we require an object to hold this information.
	 * 
	 * Because some messages are made up of multiple fragments and not all fragments are the same size this constructor will
	 * make use of the largest fragment defined in the from as the bases for how big to make the primary ring.  Once the 
	 * primary ring is defined we find the fragment with the highest ratio of variable length fields and assume the ring
	 * is full of those exclusively.  This gives us the maximum number of variable length fields that can be expected which 
	 * is multiplied by the provided maximumLenghOfVariableLengthFields to get the minimum size of the byte ring. This value is
	 * then rounded up to the next power of 2.
	 */
	
	public PipeConfig(T messageSchema, int minimumFragmentsOnRing) {
	    this(messageSchema, minimumFragmentsOnRing, 0);
	}
    public PipeConfig(T messageSchema, int minimumFragmentsOnRing, int maximumLenghOfVariableLengthFields) {
        
        FieldReferenceOffsetManager from = MessageSchema.from(messageSchema);
        
		int biggestFragment = FieldReferenceOffsetManager.maxFragmentSize(from);        
        int primaryMinSize = minimumFragmentsOnRing*biggestFragment;  
        this.slabBits = (byte)(32 - Integer.numberOfLeadingZeros(primaryMinSize - 1));        
        int maxVarFieldsInRingAtOnce = FieldReferenceOffsetManager.maxVarLenFieldsPerPrimaryRingSize(from, 1<<slabBits);
        int totalBlobSize = maxVarFieldsInRingAtOnce *  maximumLenghOfVariableLengthFields;
      
        this.blobBits = ((0==maximumLenghOfVariableLengthFields) | (0==maxVarFieldsInRingAtOnce))? (byte)0 : (byte)(32 - Integer.numberOfLeadingZeros(totalBlobSize - 1));
      
        this.byteConst = null;
        this.schema = messageSchema;
        
        validate(messageSchema, minimumFragmentsOnRing, maximumLenghOfVariableLengthFields);
    }
    
    private void validate(T messageSchema, int minimumFragmentsOnRing, int maximumLenghOfVariableLengthFields) {
        if (blobBits>30) {
            throw new UnsupportedOperationException("Unable to support blob data larger than 1GB Reduce either the data size or count of desired message msgs:"+
                    minimumFragmentsOnRing+" varLen:"+maximumLenghOfVariableLengthFields+" schema: "+messageSchema+
                    " slabBits: "+slabBits+" maxFragSize: "+FieldReferenceOffsetManager.maxFragmentSize(MessageSchema.from(messageSchema)));
        }
        
        if (slabBits>30) {
            throw new UnsupportedOperationException("Unable to support slab data larger than 1GB, Reduce the count of desired message msgs:"+
                    minimumFragmentsOnRing+" varLen:"+maximumLenghOfVariableLengthFields+" schema: "+messageSchema);
        }
        
    }

    public PipeConfig(T messageSchema, int minimumFragmentsOnRing, int maximumLenghOfVariableLengthFields, byte[] byteConst) {
        
        int biggestFragment = FieldReferenceOffsetManager.maxFragmentSize(MessageSchema.from(messageSchema));
        int primaryMinSize = minimumFragmentsOnRing*biggestFragment;        
        this.slabBits = (byte)(32 - Integer.numberOfLeadingZeros(primaryMinSize - 1));
        
        int maxVarFieldsInRingAtOnce = FieldReferenceOffsetManager.maxVarLenFieldsPerPrimaryRingSize(MessageSchema.from(messageSchema), 1<<slabBits);
        int secondaryMinSize = maxVarFieldsInRingAtOnce *  maximumLenghOfVariableLengthFields;
        this.blobBits = ((0==maximumLenghOfVariableLengthFields) | (0==maxVarFieldsInRingAtOnce))? (byte)0 : (byte)(32 - Integer.numberOfLeadingZeros(secondaryMinSize - 1));

        this.byteConst = byteConst;
        this.schema = messageSchema;
        validate(messageSchema, minimumFragmentsOnRing, maximumLenghOfVariableLengthFields );
     }
	
	public PipeConfig<T> grow2x(){
		return new PipeConfig<T>((byte)(1+slabBits), (byte)(1+blobBits), byteConst, schema);
	}
	
	public PipeConfig<T> blobGrow2x(){
		return new PipeConfig<T>((byte)(slabBits), (byte)(1+blobBits), byteConst, schema);
	}
	
	
	public PipeConfig<T> debug(int debugFlags){
		PipeConfig<T> result = new PipeConfig<T>((byte)(slabBits), (byte)(blobBits), byteConst, schema);
		result.debugFlags = debugFlags;
		return result;
	}
	
	public static final int SHOW_HEAD_PUBLISH = 1;

	/**
	 * Returns true if this configuration is of the same schema and is larger or equal to the source config.
	 */
    public boolean canConsume(PipeConfig<T> sourceConfig) {
        if (this.schema == sourceConfig.schema) {
            if (this.blobBits>=sourceConfig.blobBits) {
                if (this.slabBits>=sourceConfig.slabBits) {
                    //NOTE: proably also want to check the ratio.
                    return true;
                }
            }
        }
        return false;
        
    }
	
}
