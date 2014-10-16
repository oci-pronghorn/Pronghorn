package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.catalog.loader.DictionaryFactory;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveReader;

public abstract class FASTDecoder{
    
    //active state, TODO: C, minimize or remove these. 
    
    //all constants always skipped.
    //decimals as well??
    
    public final int[] templateStartIdx; //These constants can be remvoed
     
    
    //runtime count of sequence lengths
    public int sequenceCountStackHead = -1;
    public final int[] sequenceCountStack;
    
    //private ring buffers for writing content into
    public final RingBuffers ringBuffers;
    
    //dictionary data
    protected final long[] rLongDictionary; //final array with constant references
    protected final int[] rIntDictionary; //final array with constant references
    protected final LocalHeap byteHeap;
    
    public int activeScriptCursor=-1; //needed by generated code to hold state between calls.
    public int templateId=-1; //must hold between read (wait for space on queue) and write of templateId
    public int preambleA=0; //must hold between read (wait for space on queue) and write (if it happens)
    public int preambleB=0; //must hold between read (wait for space on queue) and write (if it happens)
    public int maxPMapCountInBytes;       
    
    public final byte[] preambleData;
   
        
    public FASTDecoder(TemplateCatalogConfig catalog) {
        this(catalog.dictionaryFactory(), 
        	 catalog.getMaxGroupDepth(),
        	 catalog.getTemplateStartIdx(), 
             catalog.clientConfig().getPreableBytes(), 
             catalog.ringBuffers(), 
             TemplateCatalogConfig.maxPMapCountInBytes(catalog) );
    }
    
            
    private FASTDecoder(DictionaryFactory dcr, int maxNestedGroupDepth, 
    		            int[] templateStartIdx,
			            int preambleBytes, 
			            RingBuffers ringBuffers,
			            int maxPMapCountInBytes) {

        this.byteHeap = dcr.byteDictionary();
        
        this.sequenceCountStack = new int[maxNestedGroupDepth];
        this.rIntDictionary = dcr.integerDictionary();
        this.rLongDictionary = dcr.longDictionary();
        
        this.templateStartIdx = templateStartIdx;        
        this.preambleData = new byte[preambleBytes];
        
        this.ringBuffers = ringBuffers;
        
        this.maxPMapCountInBytes = maxPMapCountInBytes;
        
        assert (rIntDictionary.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(rIntDictionary.length));
        assert (rLongDictionary.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(rLongDictionary.length));
    }
    
    
    public static void reset(DictionaryFactory dictionaryFactory, FASTDecoder decoder) {
        
        // clear all previous values to un-set
        dictionaryFactory.reset(decoder.rIntDictionary); 
        dictionaryFactory.reset(decoder.rLongDictionary); 
                
        
        if (null!=decoder.byteHeap) {
            LocalHeap.reset(decoder.byteHeap);
        }
        decoder.sequenceCountStackHead = -1;
        
        RingBuffers.reset(decoder.ringBuffers);        

    }
    

    public abstract int decode(PrimitiveReader reader);
        


}
