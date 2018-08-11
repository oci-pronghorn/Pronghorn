package com.ociweb.pronghorn.code;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.pronghorn.util.Appendables;

public abstract class Code {

    enum State{ 
        TreeBuilding,
        CodeBuilding
    }
    
    private State state = State.TreeBuilding;
    
    //for both single and multiple as total count of calls to inc.
    private int usageInstanceCount = 0;
    private int singleResponseVar;
    
    //for multiple return value behavior
    private final int[] usageResponseCounts;
    private final int[] multiResponseVars;
    private final int[] defaultResponseOrder; 
    
    private final AtomicInteger varIdGen;
    private final boolean alwaysInline;
    
    protected Code(AtomicInteger varIdGen) {
        this(varIdGen, 1, false);
    }
    
    protected Code(AtomicInteger varIdGen, boolean alwaysInline) {
        this(varIdGen, 1, alwaysInline);
    }
    
    protected Code(AtomicInteger varIdGen, int aryResultCount, boolean alwaysInline) {
        this.alwaysInline = alwaysInline;
        
        if (aryResultCount>1) {
            usageResponseCounts = new int[aryResultCount];
            assert(false==alwaysInline): "Undefined, can not inline multiple return values";
            multiResponseVars = new int[aryResultCount];
            defaultResponseOrder = new int[aryResultCount];  
            int i = aryResultCount;
            while(--i>=0) {
                defaultResponseOrder[i] = i;
            }
        } else {
            usageResponseCounts = null;
            multiResponseVars = null;
            defaultResponseOrder = null;
        }
        
        this.varIdGen = varIdGen;
    }
    
    public void incUsesCount() {
        assert(State.TreeBuilding == state);
        assert(this instanceof SingleResult);
        usageInstanceCount++;
    }
   
    public void incUsesCount(int ... selections) {
        assert(State.TreeBuilding == state);
        assert(this instanceof MultipleResult);
        usageInstanceCount++;
        int j = selections.length;
        while(--j>=0) {
            usageResponseCounts[selections[j]]++;
        }
    }
        
    protected final boolean isUsed(int varPos) {
        assert(this instanceof MultipleResult);
        return usageResponseCounts[varPos] > 0;
    }
    
    protected void appendResultVarName(Appendable target, int varPos) throws IOException {
        assert(this instanceof MultipleResult);
        assert(isUsed(varPos));
        appendResultVar(target, multiResponseVars[varPos]);
        
    }
    
    protected void appendResultVarName(Appendable target) throws IOException {
        assert(this instanceof SingleResult);
        appendResultVar(target, singleResponseVar);
    }
    
    private void appendResultVar(Appendable target, int value) throws IOException {
        Appendables.appendHexDigits(target.append("r"), value); 
    }
    protected static Appendable appendMemberVar(Appendable target, int value) throws IOException {
        return Appendables.appendHexDigits(target.append("m"), value); 
    }
    
    //must be called before calling any result
    public void preCall(Appendable target) throws IOException {
        assert(usageInstanceCount>0) : "Did not expect to be used";
        if (State.TreeBuilding == state) {
            
            if (null != usageResponseCounts) {
                //define the vars for multi
                int j = usageResponseCounts.length;
                while (--j>=0) {
                    assert(usageResponseCounts[j]>=0) : "Negative values are undefined";
                    if (isUsed(j)) {
                        multiResponseVars[j] = varIdGen.incrementAndGet();
                    }       
                }
                upFrontDefinition(target);
                //NOTE: if temps are needed the { } will be used to wrap the block with assignments out to these defined vars                
            } else if (usageInstanceCount>1 && !alwaysInline) {
                //define the var for single
                singleResponseVar = varIdGen.incrementAndGet();
                upFrontDefinition(target);
            }
            //else only one instance so the method call will be in-lined.            
            state = State.CodeBuilding;
        }
    }
    
    //calls appendResultVarName in order to define and pre-populate result vars when needed
    //multi must also call isUsed
    protected abstract void upFrontDefinition(Appendable target) throws IOException; //only done when call is used in more than one pace or always when using multi return
    protected abstract void singleResult(Appendable target) throws IOException; //write method call which will return the value
    
    
    public Appendable result(Appendable target) throws IOException {
        assert(State.CodeBuilding == state);
        assert(this instanceof SingleResult);
        if (0 != singleResponseVar) {
            appendResultVarName(target);
        } else {
            singleResult(target);
        }
        return target;
    }
    
    public Appendable result(Appendable target, int ... selections) throws IOException {
        assert(State.CodeBuilding == state);
        assert(this instanceof SingleResult);
        multiResult(target, selections);
        return target;
    } 
    
    public Appendable resultExpanded(Appendable target) throws IOException {
        assert(State.CodeBuilding == state);
        assert(this instanceof SingleResult);
        multiResult(target, defaultResponseOrder);
        return target;
    } 
    
    protected void multiResult(Appendable target, int[] selections) throws IOException {
     
        for(int i = 0; i<selections.length; i++) {
            assert(isUsed(selections[i]));
            if (i>0) {
                target.append(", ");
            }
            appendResultVar(target, selections[i]);
        }
    }

    public void defineMembers(Appendable target) throws IOException {
       
    }
    
}
