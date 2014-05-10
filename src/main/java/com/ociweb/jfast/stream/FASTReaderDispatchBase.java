package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;

public abstract class FASTReaderDispatchBase {

    //NOTE: these must be public or the generated code will not be able to work with them, known JDK bug.
    
    public final PrimitiveReader reader;
    public final TextHeap textHeap;
    public final ByteHeap byteHeap;

    public final FASTReaderDispatchBase dispatch;

    // This is the GLOBAL dictionary
    // When unspecified in the template GLOBAL is the default so these are used.
    public final int MAX_INT_INSTANCE_MASK;
    public final int[] rIntDictionary;
    public final int[] rIntInit;

    public final int MAX_LONG_INSTANCE_MASK;
    public final long[] rLongDictionary;
    public final long[] rLongInit;

    public final int MAX_TEXT_INSTANCE_MASK;

    public final int nonTemplatePMapSize;
    public final int[][] dictionaryMembers;

    public final DictionaryFactory dictionaryFactory;

    public DispatchObserver observer;

    public final int maxNestedSeqDepth;
    public final int[] sequenceCountStack;

    public int sequenceCountStackHead = -1;
    public boolean doSequence; //NOTE: return value from closeGroup
    public int jumpSequence; // Only needs to be set when returning true.



    public int activeScriptCursor;
    public int activeScriptLimit;

    public int[] fullScript;
    
    public static final int INIT_VALUE_MASK = 0x80000000;
    public int byteInstanceMask;
    
    
    
    public FASTReaderDispatchBase(PrimitiveReader reader, DictionaryFactory dcr, int nonTemplatePMapSize,
            int[][] dictionaryMembers, int maxTextLen, int maxVectorLen, int charGap, int bytesGap, int[] fullScript,
            int maxNestedGroupDepth, int primaryRingBits, int textRingBits) {
        this.reader = reader;
        this.dictionaryFactory = dcr;
        this.nonTemplatePMapSize = nonTemplatePMapSize;
        this.dictionaryMembers = dictionaryMembers;

        this.textHeap = dcr.charDictionary(maxTextLen, charGap);
        this.byteHeap = dcr.byteDictionary(maxVectorLen, bytesGap);

        this.maxNestedSeqDepth = maxNestedGroupDepth;
        this.sequenceCountStack = new int[maxNestedSeqDepth];

        this.fullScript = fullScript;

        this.rIntDictionary = dcr.integerDictionary();
        this.rIntInit = dcr.integerDictionary();
        assert (rIntDictionary.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(rIntDictionary.length));
        assert (rIntDictionary.length == rIntInit.length);
        this.MAX_INT_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rIntDictionary.length - 1));

        this.rLongDictionary = dcr.longDictionary();
        this.rLongInit = dcr.longDictionary();
        assert (rLongDictionary.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(rLongDictionary.length));
        assert (rLongDictionary.length == rLongInit.length);

        this.MAX_LONG_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rLongDictionary.length - 1));

        assert(null==textHeap || textHeap.itemCount()<TokenBuilder.MAX_INSTANCE);
        assert(null==textHeap || TokenBuilder.isPowerOfTwo(textHeap.itemCount()));
        this.MAX_TEXT_INSTANCE_MASK = (null==textHeap)?TokenBuilder.MAX_INSTANCE:Math.min(TokenBuilder.MAX_INSTANCE, (textHeap.itemCount()-1));

        this.rbRingBuffer = new FASTRingBuffer((byte) primaryRingBits,
                                               (byte) textRingBits, 
                                                null==textHeap? null : textHeap.rawInitAccess(),
                                                null==byteHeap? null : byteHeap.rawInitAccess()
                                                );
//        rbMask = rbRingBuffer.mask;
//        rbB = rbRingBuffer.buffer;
        
        byteInstanceMask = null == byteHeap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE,
        byteHeap.itemCount() - 1);
        
        this.dispatch = this;
        
    }
    
    public void setDispatchObserver(DispatchObserver observer) {
        this.observer = observer;
    }

    protected boolean gatherReadData(PrimitiveReader reader, int cursor) {

        int token = fullScript[cursor];

        if (null != observer) {
            String value = "";
            // totalRead is bytes loaded from stream.

            long absPos = PrimitiveReader.totalRead(reader) - PrimitiveReader.bytesReadyToParse(reader);
            observer.tokenItem(absPos, token, cursor, value);
        }

        return true;
    }

    protected boolean gatherReadData(PrimitiveReader reader, int cursor, String value) {

        int token = fullScript[cursor];

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
    
    public void reset() {

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

        public final FASTRingBuffer rbRingBuffer;
    
    public FASTRingBuffer ringBuffer() {
        return rbRingBuffer;//TODO: A, remove method.
    }

    public abstract boolean dispatchReadByToken();

}
