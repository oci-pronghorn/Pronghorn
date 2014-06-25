package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveReader;

public abstract class FASTDecoder{

    //debugging state, remove
    protected static DispatchObserver observer;
    
    //active state, TODO: C, minimize or remove these.
    public int sequenceCountStackHead = -1;

    private final int[] templateStartIdx;
    private final int[] templateLimitIdx;
    
    public int activeScriptCursor; //needed by generated code to hold state between calls.
    public int ringBufferIdx= -1;
    
    public final int maxTemplatePMapSize;
    
    public final int[] sequenceCountStack;
    
    //dictionary data
    protected final long[] rLongDictionary;
    protected final int[] rIntDictionary;
    protected final ByteHeap byteHeap;
    protected final TextHeap textHeap;
    
    public int templateId=-1;
    public int preambleA=0;
    public int preambleB=0;
            
    public final byte preambleDataLength;
    public final FASTRingBuffer[] ringBuffers;
        
    public int neededSpaceOrTemplate = -1; //<0 need template, 0 need nothing, >0 need this many units in (which?) ring buffer.

    public FASTDecoder(TemplateCatalogConfig catalog) {
        this(catalog.dictionaryFactory(), catalog.getMaxGroupDepth(), computePMapStackInBytes(catalog), 
             catalog.getTemplateStartIdx(), catalog.getTemplateLimitIdx(),
             catalog.maxTemplatePMapSize(), catalog.clientConfig().getPreableBytes(), catalog.ringBuffers());
    }
    
    private static int computePMapStackInBytes(TemplateCatalogConfig catalog) {
        return 2 + ((Math.max(
                catalog.maxTemplatePMapSize(), catalog.maxNonTemplatePMapSize()) + 2) * catalog.getMaxGroupDepth());
    }
    
            
    private FASTDecoder(DictionaryFactory dcr, int maxNestedGroupDepth, int maxPMapCountInBytes,
            int[] templateStartIdx, int[] templateLimitIdx,
            int maxTemplatePMapSize, int preambleDataLength, FASTRingBuffer[] ringBuffers) {

        this.textHeap = dcr.charDictionary();
        this.byteHeap = dcr.byteDictionary();

        this.maxTemplatePMapSize = maxTemplatePMapSize;
        this.preambleDataLength = (byte)preambleDataLength;
        
        this.sequenceCountStack = new int[maxNestedGroupDepth];
        this.rIntDictionary = dcr.integerDictionary();
        this.rLongDictionary = dcr.longDictionary();
        
        this.templateStartIdx = templateStartIdx;
        this.templateLimitIdx = templateLimitIdx;
        
        this.ringBuffers = ringBuffers;
        
        assert (rIntDictionary.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(rIntDictionary.length));
        assert (rLongDictionary.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(rLongDictionary.length));
        assert(null==textHeap || textHeap.itemCount()<TokenBuilder.MAX_INSTANCE);
        assert(null==textHeap || TokenBuilder.isPowerOfTwo(textHeap.itemCount()));

    }
    
    public void setDispatchObserver(DispatchObserver observer) {
        this.observer = observer;
    }
//inject the observer
    protected boolean gatherReadData(PrimitiveReader reader, int cursor, int token) {

        if (null != observer) {
            String value = "";
            // totalRead is bytes loaded from stream.

            long absPos = PrimitiveReader.totalRead(reader) - PrimitiveReader.bytesReadyToParse(reader);
            observer.tokenItem(absPos, token, cursor, value);
        }

        return true;
    }

    protected boolean gatherReadData(PrimitiveReader reader, int cursor, String value, int token) {

        if (null != observer) {
            // totalRead is bytes loaded from stream.

            long absPos = PrimitiveReader.totalRead(reader) - PrimitiveReader.bytesReadyToParse(reader);
            observer.tokenItem(absPos, token, cursor, value);
        }

        return true;
    }

    protected boolean gatherReadData(PrimitiveReader reader, String msg, int activeScriptCursor) {

        if (null != observer) {
            long absPos = PrimitiveReader.totalRead(reader) - PrimitiveReader.bytesReadyToParse(reader);
            observer.tokenItem(absPos, -1, activeScriptCursor, msg);
        }

        return true;
    }
    
    public void reset(DictionaryFactory dictionaryFactory) {
                
        // clear all previous values to un-set
        dictionaryFactory.reset(rIntDictionary);
        dictionaryFactory.reset(rLongDictionary);
        if (null != textHeap) {
            textHeap.reset();
        }
        if (null!=byteHeap) {
            byteHeap.reset();
        }
        sequenceCountStackHead = -1;

    }

    public FASTRingBuffer ringBuffer(int idx) {
        return ringBuffers[idx];
    }
    
    public abstract boolean decode(PrimitiveReader reader);
        

    
    

    public int activeScriptLimit; //TODO: A, remvoe this once limit is removed from iterprister after stack is used for exit flag.
    
    public int requiredBufferSpace2() {
        
        activeScriptCursor = templateStartIdx[templateId];//set location for the generated code state.
        activeScriptLimit = templateLimitIdx[templateId];

        return (activeScriptLimit - activeScriptCursor) << 2;        
        
    }
    

    static void pump2startTemplate(FASTDecoder dispatch, PrimitiveReader reader) {
        // get next token id then immediately start processing the script
        // /read prefix bytes if any (only used by some implementations)
        assert (dispatch.preambleDataLength != 0 && dispatch.gatherReadData(reader, "Preamble", 0));
        //ring buffer is build on int32s so the implementation limits preamble to units of 4
        assert ((dispatch.preambleDataLength&0x3)==0) : "Preable may only be in units of 4 bytes";
        assert (dispatch.preambleDataLength<=8) : "Preable may only be 8 or fewer bytes";
        //Hold the preamble value here until we know the template and therefore the needed ring buffer.
        
        
        //break out into custom gen method
        int p = dispatch.preambleDataLength;
        int a=0, b=0;
        if (p>0) {
            a = PrimitiveReader.readRawInt(reader);
             if (p>4) {
                b = PrimitiveReader.readRawInt(reader);
                assert(p==8) : "Unsupported large preamble";
            }
        }
        
        // /////////////////
        // open message (special type of group)
        dispatch.templateId = PrimitiveReader.openMessage(dispatch.maxTemplatePMapSize, reader);
                    
        // write template id at the beginning of this message
        int neededSpace = 1 + dispatch.preambleDataLength + dispatch.requiredBufferSpace2();
        dispatch.ringBufferIdx = dispatch.activeScriptCursor;
        //we know the templateId so we now know which ring buffer to use.
        FASTRingBuffer rb = dispatch.ringBuffers[dispatch.activeScriptCursor];
        
        if (neededSpace > 0) {
            int size = rb.maxSize;
            if (( size-(rb.addPos.value-rb.remPos.value)) < neededSpace) {
                while (( size-(rb.addPos.value-rb.remPos.value)) < neededSpace) {
                    //TODO: must call blocking policy on this, already committed to read.
                  //  System.err.println("no room in ring buffer");
                   Thread.yield();// rb.dump(rb);
                }
                
            }
        }                   
                
        //break out into second half of gen.
        p = dispatch.preambleDataLength;
        if (p>0) {
            //TODO: X, add mode for reading the preamble above but NOT writing to ring buffer because it is not needed.
            FASTRingBuffer.addValue(rb.buffer, rb.mask, rb.addPos, a);
            if (p>4) {
                FASTRingBuffer.addValue(rb.buffer, rb.mask, rb.addPos, b);
            }
        }
        //System.err.println("> Wrote templateID:"+templateId+" at pos "+rb.addPos.value+" vs "+rb.addCount.get()); 
        FASTRingBuffer.addValue(rb.buffer, rb.mask, rb.addPos, dispatch.templateId);
        
    }

}
