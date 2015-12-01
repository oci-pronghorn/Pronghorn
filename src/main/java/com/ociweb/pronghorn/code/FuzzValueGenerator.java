package com.ociweb.pronghorn.code;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.pronghorn.pipe.util.Appendables;

public class FuzzValueGenerator extends Code implements SingleResult {
    

    //when value masked by this is zero we will generate a null instead of the value.
    //CAUTION: this implies that this particular literal value will not be tested.
    private static final String MASK_FOR_NULL = "0x3FF"; 
    private static final int    MAX_FOR_NULL_INT = 0x3FF; 

    
    //TODO: generate code into resources folder,  maven, where to put generated. 
    //TODO: must reset back to zero when we get to large.
    
    
    private final int varId;
    private final boolean isLong;
    private final boolean isSigned;
    private final long minimumInclusive;
    private final long maximumExclusive;
    private final boolean isNullable;
    private final boolean isChars;
        
    public FuzzValueGenerator(AtomicInteger varIdGen, boolean isLong, boolean isSigned, boolean isNullable) {
        this(varIdGen,isLong,isSigned,Long.MIN_VALUE,Long.MAX_VALUE, isNullable, false);
    }
    
    public FuzzValueGenerator(AtomicInteger varIdGen, boolean isLong, boolean isSigned, long minimum, long maximum, boolean isNullable, boolean isChars) {
        super(varIdGen,1,true);
        this.varId = varIdGen.incrementAndGet();
        this.isLong = isLong;
        this.isSigned = isSigned;
        this.minimumInclusive = minimum;
        this.maximumExclusive = maximum;
        this.isNullable = isNullable;
        this.isChars = isChars;
    }
    
    //TODO: must test interesting numbers then use random.  use prime number to pick unique number generator.
    
    @Override
    public void defineMembers(Appendable target) throws IOException {
        
        appendMemberVar(target.append(isLong ? "long " : "int "), varId).append(";\n");
    
    }
    
    
    @Override
    protected void upFrontDefinition(Appendable target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void singleResult(Appendable target) throws IOException {
        
        nullableBoxedValues(target);
        
    }
    
    public static int intGenerator(int key) {
        return key < 0xFFFFF ? key : Integer.reverse(key);
    }
    
    public static long longGenerator(long key) {
        return key < 0xFFFFF ? key : Long.reverse(key);
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
        
        boundedLowEnd(target);
                
        if (isNullable || isChars) {
            target.append("))");
        }
    }

    private void boundedLowEnd(Appendable target) throws IOException {
        if (Long.MIN_VALUE!=minimumInclusive) {
            Appendables.appendValue(target.append("Math.max("),minimumInclusive).append(',');
        }   
        
        boundedHighEnd(target);
        
        if (Long.MIN_VALUE!=minimumInclusive) {
            target.append(")");
        }
    }

    private void boundedHighEnd(Appendable target) throws IOException {
        if (Long.MAX_VALUE!=maximumExclusive) {
            Appendables.appendValue(target.append("Math.min("),maximumExclusive-1).append(',');
        }        
        
        absOrSignedValue(target);

        if (Long.MAX_VALUE!=maximumExclusive) {
            target.append(")");
        }
    }

    private void absOrSignedValue(Appendable target) throws IOException {
        if (!isSigned) {
            target.append("Math.abs(");
        }
        
        coreValue(target);        
        
        if (!isSigned) {
            target.append(")");
        }
    }

    private void coreValue(Appendable target) throws IOException {
        //only reverse when > FF FF FF
        if (isLong) {
            appendMemberVar(target.append(FuzzValueGenerator.class.getCanonicalName()).append(".longGenerator("),varId).append("++) ");
        } else {
            appendMemberVar(target.append(FuzzValueGenerator.class.getCanonicalName()).append(".intGenerator("),varId).append("++) ");
        }
    }

    
    
}
