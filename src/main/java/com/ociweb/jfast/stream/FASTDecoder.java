package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;

public abstract class FASTDecoder {

    //debugging state
    protected DispatchObserver observer;
    
    //active state, TODO: C, minimize or remove these.
    public int sequenceCountStackHead = -1;
    public int activeScriptCursor;
    public int activeScriptLimit;
    
    public final int[] sequenceCountStack;
    
    //dictionary data
    protected final long[] rLongDictionary;
    protected final int[] rIntDictionary;
    protected final ByteHeap byteHeap;
    protected final TextHeap textHeap;
    
    public final int maxPMapCountInBytes;
    
    private final int[] templateStartIdx;
    private final int[] templateLimitIdx;
    public final int maxTemplatePMapSize;
    public final byte preambleDataLength;
        
    public int neededSpaceOrTemplate = -1; //<0 need template, 0 need nothing, >0 need this many units in (which?) ring buffer.

    public FASTDecoder(TemplateCatalog catalog) {
        this(catalog.dictionaryFactory(), catalog.maxNonTemplatePMapSize(), catalog.dictionaryResetMembers(), catalog.getMaxTextLength(), 
                catalog.getMaxByteVectorLength(), catalog.getTextGap(), 
                catalog.getByteVectorGap(), catalog.fullScript(), catalog.getMaxGroupDepth(),
                8, 7, computePMapStackInBytes(catalog), catalog.templateStartIdx, catalog.templateLimitIdx,
                catalog.maxTemplatePMapSize(), catalog.getIntProperty(TemplateCatalog.KEY_PARAM_PREAMBLE_BYTES,0));
    }
    
    private static int computePMapStackInBytes(TemplateCatalog catalog) {
        return 2 + ((Math.max(
                catalog.maxTemplatePMapSize(), catalog.maxNonTemplatePMapSize()) + 2) * catalog.getMaxGroupDepth());
    }
    
            
    public FASTDecoder(DictionaryFactory dcr, int nonTemplatePMapSize, int[][] dictionaryMembers,
            int maxTextLen, int maxVectorLen, int charGap, int bytesGap, int[] fullScript, int maxNestedGroupDepth,
            int primaryRingBits, int textRingBits, int maxPMapCountInBytes, int[] templateStartIdx, int[] templateLimitIdx,
            int maxTemplatePMapSize, int preambleDataLength) {

        this.maxPMapCountInBytes = maxPMapCountInBytes;
        this.textHeap = dcr.charDictionary(maxTextLen, charGap);
        this.byteHeap = dcr.byteDictionary(maxVectorLen, bytesGap);

        this.maxTemplatePMapSize = maxTemplatePMapSize;
        this.preambleDataLength = (byte)preambleDataLength;
        
        this.sequenceCountStack = new int[maxNestedGroupDepth];
        this.rIntDictionary = dcr.integerDictionary();
        this.rLongDictionary = dcr.longDictionary();
        
        this.templateStartIdx = templateStartIdx;
        this.templateLimitIdx = templateLimitIdx;
        
        assert (rIntDictionary.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(rIntDictionary.length));
        assert (rLongDictionary.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(rLongDictionary.length));
        assert(null==textHeap || textHeap.itemCount()<TokenBuilder.MAX_INSTANCE);
        assert(null==textHeap || TokenBuilder.isPowerOfTwo(textHeap.itemCount()));

    }

    public static FASTRingBuffer ringBufferBuilder(int primaryRingBits, 
                                                    int textRingBits, 
                                                    FASTDecoder decoder) {

        return new FASTRingBuffer((byte) primaryRingBits,
                                               (byte) textRingBits, 
                                                null==decoder.textHeap? null : decoder.textHeap.rawInitAccess(),
                                                null==decoder.byteHeap? null : decoder.byteHeap.rawInitAccess()
                                                );
    }
    
    public void setDispatchObserver(DispatchObserver observer) {
        this.observer = observer;
    }

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

    protected boolean gatherReadData(PrimitiveReader reader, String msg) {

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

    public abstract boolean decode(PrimitiveReader reader);

    public int requiredBufferSpace(int i) {
        
        //TODO: B, this assignment is required for the interpreter but we can optimize it out for the compiled version.
        
        // set the cursor start and stop for this template
        activeScriptCursor = templateStartIdx[i];
        activeScriptLimit = templateLimitIdx[i];

        // Worst case scenario is that this is full of decimals which each need
        // 3.
        // but for easy math we will use 4, will require a little more empty
        // space in buffer
        // however we will not need a lookup table
        return (activeScriptLimit - activeScriptCursor) << 2;
    }
    
    public void reset() {
        activeScriptCursor = 0;
        activeScriptLimit = 0;
    }

}
