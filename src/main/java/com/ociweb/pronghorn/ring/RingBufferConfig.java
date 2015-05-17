package com.ociweb.pronghorn.ring;

public class RingBufferConfig {
	
	//try to keep all this under 20MB and 1 RB under 64K if possible under 256K is highly encouraged
	public final byte primaryBits;
	public final byte byteBits;
	public final byte[] byteConst;
	public final FieldReferenceOffsetManager from;
	public int debugFlags = 0;

	/**
	 * This is not the constructor you are looking for.
	 * @param from
	 */
	public RingBufferConfig(byte primaryBits, byte byteBits, byte[] byteConst, FieldReferenceOffsetManager from) {
		this.primaryBits = primaryBits;
		this.byteBits = byteBits;
		this.byteConst = byteConst;
		this.from = from;
	}
	
	public RingBufferConfig(FieldReferenceOffsetManager from) {
		//default size which is smaller than half of 64K because this is the L1 cache size on intel haswell.
		this.primaryBits = 6;
		this.byteBits = 15;
		this.byteConst = null;
		this.from = from;
		//validate
    	FieldReferenceOffsetManager.maxVarLenFieldsPerPrimaryRingSize(from, 1<<primaryBits);
	}
	
    public static RingBuffer pipe(RingBufferConfig config) {
        return new RingBuffer(config);
    }
	
	public String toString() {
		return "Primary:"+primaryBits+" Secondary:"+byteBits;
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
	 * 
	 * @param from
	 * @param minimumFragmentsOnRing The minimum number of fragments/messages that the application must be able to put on the ring.
	 * @param maximumLenghOfVariableLengthFields
	 */
	public RingBufferConfig(FieldReferenceOffsetManager from, int minimumFragmentsOnRing, int maximumLenghOfVariableLengthFields) {
		
		int biggestFragment = FieldReferenceOffsetManager.maxFragmentSize(from);
		int primaryMinSize = minimumFragmentsOnRing*biggestFragment;		
		this.primaryBits = (byte)(32 - Integer.numberOfLeadingZeros(primaryMinSize - 1));
		
		int maxVarFieldsInRingAtOnce = FieldReferenceOffsetManager.maxVarLenFieldsPerPrimaryRingSize(from, 1<<primaryBits);
        int secondaryMinSize = maxVarFieldsInRingAtOnce *  maximumLenghOfVariableLengthFields;
        this.byteBits = (byte)(32 - Integer.numberOfLeadingZeros(secondaryMinSize - 1));

		this.byteConst = null;
		this.from = from;
	}
	
    public RingBufferConfig(FieldReferenceOffsetManager from, int minimumFragmentsOnRing, int maximumLenghOfVariableLengthFields, byte[] byteConst) {
        
        int biggestFragment = FieldReferenceOffsetManager.maxFragmentSize(from);
        int primaryMinSize = minimumFragmentsOnRing*biggestFragment;        
        this.primaryBits = (byte)(32 - Integer.numberOfLeadingZeros(primaryMinSize - 1));
        
        int maxVarFieldsInRingAtOnce = FieldReferenceOffsetManager.maxVarLenFieldsPerPrimaryRingSize(from, 1<<primaryBits);
        int secondaryMinSize = maxVarFieldsInRingAtOnce *  maximumLenghOfVariableLengthFields;
        this.byteBits = (byte)(32 - Integer.numberOfLeadingZeros(secondaryMinSize - 1));

        this.byteConst = byteConst;
        this.from = from;
     }
	
	public RingBufferConfig grow2x(){
		return new RingBufferConfig((byte)(1+primaryBits), (byte)(1+byteBits), byteConst, from);
	}
	
	public RingBufferConfig debug(int debugFlags){
		RingBufferConfig result = new RingBufferConfig((byte)(primaryBits), (byte)(byteBits), byteConst, from);
		result.debugFlags = debugFlags;
		return result;
	}
	
	public static final int SHOW_HEAD_PUBLISH = 1;
	
}
