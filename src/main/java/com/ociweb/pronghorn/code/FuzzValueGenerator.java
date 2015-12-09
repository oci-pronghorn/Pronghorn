package com.ociweb.pronghorn.code;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.pronghorn.util.Appendables;

public class FuzzValueGenerator extends Code implements SingleResult {
    

    //when value masked by this is zero we will generate a null instead of the value.
    //CAUTION: this implies that this particular literal value will not be tested.
    private static final String MASK_FOR_NULL = "0x3FF"; 
    private static final int    MAX_FOR_NULL_INT = 0x3FF; 

    
    //TODO: generate code into resources folder,  maven, where to put generated. 
    //TODO: must reset back to zero when we get to large.
    
    private final String tab = "    ";
    private final int varId;
    private final boolean isLong;
    private final boolean isNullable;
    private final boolean isChars;
        
    private final long longMask;
    private final long longFloor;

    private final int  intMask;
    private final int  intFloor;
    private final boolean isFullRange;
    
    private final long startValueLiteral;
    
    private static final int[] primes = new int[]{ 137,    3,      5,      7,     11,     13,     17,     19,     23,     29,
                                                   31,     37,     41,     43,     47,     53,     59,     61,     67,     71, 
                                                   73,     79,     83,     89,     97,    101,    103,    107,    109,    113,
                                                   127,    131}; //32 primes as seeds
    private static final AtomicInteger genSeed = new AtomicInteger(); 
    private static final int genSeedMask = (1<<5)-1;
    
    private final int stepSize;    
    
    
    public FuzzValueGenerator(AtomicInteger varIdGen, boolean isLong, boolean isSigned, boolean isNullable) {
        this(varIdGen,isLong,isSigned, isNullable, false);
    }
    
    public FuzzValueGenerator(AtomicInteger varIdGen, boolean isLong, boolean isSigned, boolean isNullable, boolean isChars) {
        this(varIdGen,isLong,isSigned,isNullable,isChars,primes[genSeedMask&genSeed.incrementAndGet()], Long.MIN_VALUE);
                
    }
    
    public FuzzValueGenerator(AtomicInteger varIdGen, boolean isLong, boolean isSigned, boolean isNullable, boolean isChars, int step, long startLiteral) {
        super(varIdGen,1,true);
        
        this.startValueLiteral = startLiteral;
        this.stepSize = step;
        this.varId = varIdGen.incrementAndGet();
        this.isLong = isLong;       
        this.isNullable = isNullable;
        this.isChars = isChars;
        
        int bits;
        if (isSigned) {
            isFullRange = true;
            if (isLong) {
                bits = 64;     
                longFloor = Long.MIN_VALUE;            
                intFloor = 0;       
            } else {
                bits = 32;  
                longFloor = 0;    
                intFloor = Integer.MIN_VALUE;       
            }
        } else {
            isFullRange = false;
            if (isLong) {
                bits = 63;          
                longFloor = 0;            
                intFloor = 0;       
            } else {
                bits = 31;  
                longFloor = 0;            
                intFloor = 0;       
            }
        }
        
        if (isLong) {
            longMask = (1L<<bits)-1L;
            intMask = 0;                
        } else {
            intMask = (1<<bits)-1;
            longMask = 0;
        }
        
    }
    
    public FuzzValueGenerator(AtomicInteger varIdGen, boolean isLong, boolean isNullable, boolean isChars, int positiveRangeMask) {
        super(varIdGen,1,true);
        
        this.startValueLiteral = Long.MIN_VALUE;
        this.stepSize = primes[genSeedMask&genSeed.incrementAndGet()];
        this.varId = varIdGen.incrementAndGet();
        this.isLong = isLong;       
        this.isNullable = isNullable;
        this.isChars = isChars;
        
        isFullRange = false;
        if (isLong) {        
            longFloor = 0;            
            intFloor = 0;       
        } else {
            longFloor = 0;            
            intFloor = 0;       
        }

        assert(positiveRangeMask< Integer.MAX_VALUE);
      
        
        if (isLong) {
            longMask = positiveRangeMask;
            intMask = 0;                
        } else {
            intMask = positiveRangeMask;
            longMask = 0;
        }
        
        
        
    }
    
    
    //TODO: must test interesting numbers then use random.  use prime number to pick unique number generator.
    
    @Override
    public void defineMembers(Appendable target) throws IOException {
        
        appendMemberVar(target.append(tab).append(isLong ? "private long " : "private int "), varId);
        if (Long.MIN_VALUE!=startValueLiteral) {
            Appendables.appendValue(target, " = ", startValueLiteral);
            if (isLong) {
                target.append('L');
            }
        }
        target.append(";\n");
    }
    
    
    @Override
    protected void upFrontDefinition(Appendable target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void singleResult(Appendable target) throws IOException {
        
        nullableBoxedValues(target);
        
    }
        
    public static CharSequence stringGenerator(long key) {
        try {
            return (CharSequence)Appendables.appendHexDigits(new StringBuilder(), key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
   
    public static CharSequence nullableStringGenerator(long key) {
        if (0==(key&MAX_FOR_NULL_INT)) {
            return null;
        }
        try {
            return (CharSequence)Appendables.appendHexDigits(new StringBuilder(), key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void nullableBoxedValues(Appendable target) throws IOException {
        if (isNullable) {
                        
            if (isChars) {
                
                appendMemberVar(target.append("(").append(FuzzValueGenerator.class.getCanonicalName()).append(".nullableStringGenerator("),varId);                
                
            } else {                 
                appendMemberVar(target.append("((0==("),varId).append("&").append(MASK_FOR_NULL).append(")) ? null : ");
                if (isLong) {
                    target.append("new Long(");
                } else {
                    target.append("new Integer(");
                }
            }
        } else {
            if (isChars) {
                appendMemberVar(target.append("(").append(FuzzValueGenerator.class.getCanonicalName()).append(".stringGenerator("),varId); 
            }
        }
        
        coreValue(target);
                
        if (isNullable || isChars) {
            target.append("))");
        }
    }


    private void coreValue(Appendable target) throws IOException {
        //only reverse when > FF FF FF
        if (isLong) {
            if (0!=longFloor) {
                Appendables.appendValue(target, longFloor).append("+");
            }
            target.append("(");
            if (!isFullRange) {
                Appendables.appendHexDigits(target, longMask).append("L & ");
            }
            appendMemberVar(target.append("("),varId);
            Appendables.appendValue(target.append(" += "), stepSize).append("))");
        } else {
            if (0!=intFloor) {
                System.out.println("appending the intFloor of "+intFloor);
                Appendables.appendValue(target, intFloor).append("+");
            }
            target.append("(");
            if (!isFullRange) {
                Appendables.appendHexDigits(target, intMask).append(" & ");
            }
            appendMemberVar(target.append("("),varId);
            Appendables.appendValue(target.append(" += "), stepSize).append("))");
        }
    }

    
    
}
