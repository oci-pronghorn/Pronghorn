package com.ociweb.jfast.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.util.IntWriteOnceOrderedSet;
import com.ociweb.pronghorn.ring.util.hash.MurmurHash;

public class GeneratorData {
    public SourceTemplates templates;
    public StringBuilder fieldMethodBuilder;
    public StringBuilder groupMethodBuilder;
    public StringBuilder statsBuilder;
    public List<String> caseParaDefs;
    public List<String> caseParaVals;
    public int scriptPos;
    public long templateId;
    public String fieldPrefix;
    public int fieldMethodCount;
    public String caseTail;
    public IntWriteOnceOrderedSet sequenceStarts;
    public byte[] origCatBytes;
    public int[] hashedCat;
    
    public int runningComplexity;
    public String lastFieldParaValues;
    public Map<String, AtomicInteger> usages;
    
    public final String dispatchType;
    
    public final StringBuilder dictionaryBuilderInt;
    public final StringBuilder dictionaryBuilderLong;
    public int readerPmapBit=6;
    public int writerPmapBit0=6;
    public int writerPmapBit1=6;
	public final FieldReferenceOffsetManager from;
	public final RingBuffer mockRB;
    
    
    static final String END_FIELD_METHOD = "};\n";
    //A fragment is the smallest unit that can be passed to the caller. It is never larger than a group but may often be the same size as one.
    static final String FRAGMENT_METHOD_NAME = "fragment";

    public GeneratorData(byte[] catBytes, Class clazz) {
        
        this(   catBytes,
                "}\n", 0, "_",
                clazz);
        
    }
    
    private GeneratorData(
            byte[] catBytes,
            String caseTail,
            int runningComplexity, 
            String lastFieldParaValues,
            Class clazz) {
    	
        this.origCatBytes = catBytes;        
        this.hashedCat = hashCatBytes(catBytes);        
        
        this.caseParaDefs = new ArrayList<String>(1<<16);
        this.caseParaVals = new ArrayList<String>(1<<16);
        this.caseTail = caseTail;
        this.sequenceStarts = new IntWriteOnceOrderedSet(17);
        this.runningComplexity = runningComplexity;
        this.lastFieldParaValues = lastFieldParaValues;
        this.usages = new HashMap<String,AtomicInteger>();
        this.templates = new SourceTemplates(clazz);
        boolean isReader = FASTReaderDispatchTemplates.class==clazz;
        this.dispatchType = isReader ? "FASTReaderGeneratedDispatch" : "FASTWriterGeneratedDispatch";
        this.fieldMethodBuilder = new StringBuilder();
        this.groupMethodBuilder = new StringBuilder();
        this.statsBuilder = new StringBuilder();
        this.fieldMethodCount = 0;
        this.dictionaryBuilderInt = new StringBuilder();
        this.dictionaryBuilderLong = new StringBuilder();
        
        //NOTE: this does produce some extra garbage that could be avoided if the caller passed in the catalog
        TemplateCatalogConfig template = new TemplateCatalogConfig(catBytes);
        this.from = template.getFROM();
        //must be zero size to make the mask also zero
        this.mockRB = new RingBuffer(new RingBufferConfig((byte)0, (byte)0, null, this.from));
        
    }

	public static int[] hashCatBytes(byte[] catBytes) {
		int[] seeds = new int[]{15485863, 104395301, 217645177, 314606869, 413158551, 512927377, 613651349, 715225739};
		
		int j = seeds.length;
		int[] results = new int[j]; 
		while (--j>=0) {
			results[j] = MurmurHash.hash32(catBytes, 0, catBytes.length, seeds[j]);			
		}
		return results;	
	}
}