package com.ociweb.jfast.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class GeneratorData {
    public SourceTemplates templates;
    public StringBuilder fieldMethodBuilder;
    public StringBuilder groupMethodBuilder;
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
    static final int COMPLEXITY_LIMITY_PER_METHOD = 10;
    static final String END_FIELD_METHOD = "};\n";
    //A fragment is the smallest unit that can be passed to the caller. It is never larger than a group but may often be the same size as one.
    static final String FRAGMENT_METHOD_NAME = "fragment";

    public GeneratorData() {
        
        this(   new ArrayList<String>(), 
                new ArrayList<String>(), 
                "}\n", 
                new HashSet<Integer>(), 0, "_",
                new HashMap<String,AtomicInteger>());
        
    }
    
    public GeneratorData(List<String> caseParaDefs, List<String> caseParaVals, String caseTail,
            Set<Integer> sequenceStarts, int runningComplexity, String lastFieldParaValues,
            Map<String, AtomicInteger> usages) {
        this.caseParaDefs = caseParaDefs;
        this.caseParaVals = caseParaVals;
        this.caseTail = caseTail;
        this.sequenceStarts = sequenceStarts;
        this.runningComplexity = runningComplexity;
        this.lastFieldParaValues = lastFieldParaValues;
        this.usages = usages;
    }
}