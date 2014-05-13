package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FASTWriterDispatch { //TODO: rename as FASTEncoder
    protected int templateStackHead = 0;
    protected final int[] templateStack;

    protected final PrimitiveWriter writer;
    protected final int instanceBytesMask;
    
    public final int[] intValues;
    protected final int[] intInit;
    public final int intInstanceMask;
    
    public final long[] longValues;
    protected final long[] longInit;
    public final int longInstanceMask;
    
    protected final int nonTemplatePMapSize;

    protected int readFromIdx = -1;

    protected final DictionaryFactory dictionaryFactory;
    protected final FASTRingBuffer queue;
    protected final int[][] dictionaryMembers;

    protected final int[] sequenceCountStack;
    protected int sequenceCountStackHead = -1;
    protected boolean isFirstSequenceItem = false;
    protected boolean isSkippedSequence = false;
    protected DispatchObserver observer;
    protected int activeScriptCursor;
    protected int activeScriptLimit;
    protected final int[] fullScript;
    
    protected TextHeap textHeap;
    protected ByteHeap byteHeap;

    protected RingCharSequence ringCharSequence = new RingCharSequence();
    protected static final int INIT_VALUE_MASK = 0x80000000;
    protected final int TEXT_INSTANCE_MASK;

    public FASTWriterDispatch(PrimitiveWriter writer, DictionaryFactory dcr, int maxTemplates, int maxCharSize,
            int maxBytesSize, int gapChars, int gapBytes, FASTRingBuffer queue, int nonTemplatePMapSize,
            int[][] dictionaryMembers, int[] fullScript, int maxNestedGroupDepth) {

        this.fullScript = fullScript;
        this.writer = writer;
        this.dictionaryFactory = dcr;
        this.nonTemplatePMapSize = nonTemplatePMapSize;

        this.sequenceCountStack = new int[maxNestedGroupDepth];

        this.intValues = dcr.integerDictionary();
        this.intInit = dcr.integerDictionary();
        assert (intValues.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(intValues.length));
        this.intInstanceMask = Math.min(TokenBuilder.MAX_INSTANCE, (intValues.length - 1));
        
        this.longValues = dcr.longDictionary();
        this.longInit = dcr.longDictionary();
        assert (longValues.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(longValues.length));
        this.longInstanceMask = Math.min(TokenBuilder.MAX_INSTANCE, (longValues.length - 1));
        
        this.textHeap = dcr.charDictionary(maxCharSize, gapChars);
        this.byteHeap = dcr.byteDictionary(maxBytesSize, gapBytes);

        this.TEXT_INSTANCE_MASK = null == textHeap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (textHeap.itemCount() - 1));
        this.instanceBytesMask = null==byteHeap? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (byteHeap.itemCount()-1));

        this.templateStack = new int[maxTemplates];
        this.queue = queue;
        this.dictionaryMembers = dictionaryMembers;
    }

    public void setDispatchObserver(DispatchObserver observer) {
        this.observer = observer;
    }
}
