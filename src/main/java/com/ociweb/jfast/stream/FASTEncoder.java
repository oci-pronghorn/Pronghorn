package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArrayEquals;

public abstract class FASTEncoder { 
    
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

    public final DictionaryFactory dictionaryFactory;
    protected final int[][] dictionaryMembers;

    protected DispatchObserver observer;
    public int activeScriptCursor;
    protected int activeScriptLimit;
    protected final int[] fullScript;
    
    public final LocalHeap byteHeap;

    public int fieldPos = -1;
    
    protected RingCharSequence ringCharSequence = new RingCharSequence();
    protected static final int INIT_VALUE_MASK = 0x80000000;
    protected final int TEXT_INSTANCE_MASK;

    protected final RingBuffers ringBuffers;
    
    public final byte[] preambleData;
    
    public FASTEncoder(TemplateCatalogConfig catalog) {
        this(catalog.dictionaryFactory(), catalog.templatesCount(),
             catalog.maxNonTemplatePMapSize(), catalog.maxTemplatePMapSize(), catalog.dictionaryResetMembers(),
             catalog.fullScript(), catalog.getMaxGroupDepth(), catalog.ringBuffers(), catalog.clientConfig().getPreableBytes());
    }
    
    public FASTEncoder(TemplateCatalogConfig catalog, RingBuffers ringBuffers) {
        this(catalog.dictionaryFactory(), catalog.templatesCount(),
             catalog.maxNonTemplatePMapSize(), catalog.maxTemplatePMapSize(), catalog.dictionaryResetMembers(),
             catalog.fullScript(), catalog.getMaxGroupDepth(), ringBuffers, catalog.clientConfig().getPreableBytes());
    }
    
    
    public FASTEncoder(DictionaryFactory dcr, int maxTemplates, int nonTemplatePMapSize, int templatePMapSize,
                                int[][] dictionaryMembers, int[] fullScript, 
                                int maxNestedGroupDepth, RingBuffers ringBuffers, int preambleBytes) {

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
        
        this.byteHeap = dcr.byteDictionary();

        this.TEXT_INSTANCE_MASK = null == byteHeap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (LocalHeap.itemCount(byteHeap) - 1));
        this.instanceBytesMask = null==byteHeap? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (LocalHeap.itemCount(byteHeap)-1));

        this.templateStack = new int[maxTemplates];
        this.dictionaryMembers = dictionaryMembers;
        this.ringBuffers = ringBuffers;
        this.preambleData = new byte[preambleBytes];
    }
    


    public void setDispatchObserver(DispatchObserver observer) {
        this.observer = observer;
    }


    protected static boolean notifyFieldPositions(PrimitiveWriter writer, int activeScriptCursor) {
        
        if (null!=writer && writer.output instanceof FASTOutputByteArrayEquals) {
            FASTOutputByteArrayEquals testingOutput = (FASTOutputByteArrayEquals)writer.output;
            testingOutput.recordPosition(writer.limit,activeScriptCursor);
        }
    
        return true;
    }
    
    public abstract int encode(PrimitiveWriter writer, FASTRingBuffer ringBuffer);
    
    // must happen just before Group so the Group in question must always have
    // an outer group.
    protected static void pushTemplate(int fieldPos, PrimitiveWriter writer, FASTRingBuffer queue) {

        int templateId = FASTRingBufferReader.readInt(queue, fieldPos);
        
     //   int top = dispatch.templateStack[dispatch.templateStackHead];
//        if (top == templateId) {
//            PrimitiveWriter.writePMapBit((byte) 0, writer);
//        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeIntegerUnsigned(templateId, writer);
      //      top = templateId;
     //   }

        //dispatch.templateStack[dispatch.templateStackHead++] = top;
    }
    
    public void setActiveScriptCursor(int cursor) {
       activeScriptCursor = cursor;
    }
    
    public int getActiveScriptCursor() {
        return activeScriptCursor;
    }

    public void setActiveScriptLimit(int limit) { //TODO: B, find a way to remove this?
        activeScriptLimit = limit;
    }
    
    
}
