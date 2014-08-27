package com.ociweb.jfast.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.jfast.primitive.PrimitiveReader;

public class GeneratorData {
    public SourceTemplates templates;
    public StringBuilder fieldMethodBuilder;
    public StringBuilder groupMethodBuilder;
    public StringBuilder statsBuilder;
    public List<String> caseParaDefs;
    public List<String> caseParaVals;
    public int scriptPos;
    public int templateId;
    public String fieldPrefix;
    public int fieldMethodCount;
    public String caseTail;
    public Set<Integer> sequenceStarts;
    public byte[] origCatBytes;
    public int runningComplexity;
    public String lastFieldParaValues;
    public Map<String, AtomicInteger> usages;
    
    public final String dispatchType;
    
    public final StringBuilder dictionaryBuilderInt;
    public final StringBuilder dictionaryBuilderLong;
    public int readerPmapBit;
    public int writerPmapBit0;
    public int writerPmapBit1;
    
    
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
        this.caseParaDefs = new ArrayList<String>();
        this.caseParaVals = new ArrayList<String>();
        this.caseTail = caseTail;
        this.sequenceStarts = new HashSet<Integer>();
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
    }
}