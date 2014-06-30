package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArrayEquals;

public class FASTEncoder { 
    public int templateStackHead = 0;
    protected final int[] templateStack;

    protected final int instanceBytesMask;
    
    public final int[] intValues;
    protected final int[] intInit;
    public final int intInstanceMask;
    
    public final long[] longValues;
    protected final long[] longInit;
    public final int longInstanceMask;
    
    protected final int nonTemplatePMapSize;
    protected final int templatePMapSize;

    protected int readFromIdx = -1;

    protected final DictionaryFactory dictionaryFactory;
    protected final int[][] dictionaryMembers;

    protected DispatchObserver observer;
    public int activeScriptCursor;
    protected int activeScriptLimit;
    protected final int[] fullScript;
    
    protected TextHeap textHeap;
    protected ByteHeap byteHeap;

    protected RingCharSequence ringCharSequence = new RingCharSequence();
    protected static final int INIT_VALUE_MASK = 0x80000000;
    protected final int TEXT_INSTANCE_MASK;

    protected final FASTRingBuffer[] ringBuffers;
    
    public FASTEncoder(TemplateCatalogConfig catalog, FASTRingBuffer[] ringBuffers) {
        this(catalog.dictionaryFactory(), catalog.templatesCount(),
             catalog.maxNonTemplatePMapSize(), catalog.maxTemplatePMapSize(), catalog.dictionaryResetMembers(),
             catalog.fullScript(), catalog.getMaxGroupDepth(), ringBuffers);
    }
    
    
    public FASTEncoder(DictionaryFactory dcr, int maxTemplates, int nonTemplatePMapSize, int templatePMapSize,
                                int[][] dictionaryMembers, int[] fullScript, 
                                int maxNestedGroupDepth, FASTRingBuffer[] ringBuffers) {

        this.fullScript = fullScript;
        this.dictionaryFactory = dcr;
        
        this.nonTemplatePMapSize = nonTemplatePMapSize;
        this.templatePMapSize = templatePMapSize;

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
        
        this.textHeap = dcr.charDictionary();
        this.byteHeap = dcr.byteDictionary();

        this.TEXT_INSTANCE_MASK = null == textHeap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (textHeap.itemCount() - 1));
        this.instanceBytesMask = null==byteHeap? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (byteHeap.itemCount()-1));

        this.templateStack = new int[maxTemplates];
        this.dictionaryMembers = dictionaryMembers;
        this.ringBuffers = ringBuffers;
    }

    public void setDispatchObserver(DispatchObserver observer) {
        this.observer = observer;
    }


    protected static boolean notifyFieldPositions(PrimitiveWriter writer, int activeScriptCursor) {
        
        if (writer.output instanceof FASTOutputByteArrayEquals) {
            FASTOutputByteArrayEquals testingOutput = (FASTOutputByteArrayEquals)writer.output;
            testingOutput.recordPosition(writer.limit,activeScriptCursor);
        }
    
        return true;
    }
}
