package com.ociweb.pronghorn.stage.test;

import java.io.IOException;
import java.nio.channels.Pipe;
import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.pronghorn.code.Code;
import com.ociweb.pronghorn.code.FuzzValueGenerator;
import com.ociweb.pronghorn.code.Literal;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.pipe.util.Appendables;
import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGeneratorLowLevelWriter;

import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class FuzzGeneratorGenerator extends TemplateProcessGeneratorLowLevelWriter{

    
    //TODO: unit test must use this to generate class then use it
    //TPDP: still need constructor 
    //TOOD: still need startup, shutdown
    //TODO: generated classes need to be used and deloverd but not checked in? How
    
    //Simple random generator stage generated for a given Schema
    
    private static AtomicInteger id = new AtomicInteger();
    private Code[] generators = new Code[MessageSchema.from(schema).tokens.length];
    private Code msgGenerator = new FuzzValueGenerator(id, false, false, false, false);
        //0, MessageSchema.from(schema).messageStarts.length, 
    private long latencyTimeFieldId = -1;//undefined
    
    public FuzzGeneratorGenerator(MessageSchema schema, Appendable target) {
        super(schema, target, schema.getClass().getSimpleName()+"FuzzGenerator", "extends PronghornStage");
    }
    
    public void setTimeFieldId(long id) {
        latencyTimeFieldId = id;
    }
    
    @Override
    protected void buildConstructors(Appendable target, String className) throws IOException {
        
        target.append("public ").append(className).append("(");
        target.append(GraphManager.class.getCanonicalName()).append(" gm, ");
        Appendables.appendClass(target, Pipe.class, schema.getClass()).append(" ").append(pipeVarName).append(") {\n");
        
        target.append("super(gm,NONE,").append(pipeVarName).append(");\n");
        target.append("this.").append(pipeVarName).append(" = ").append(pipeVarName).append(";\n"); 
        
        target.append("}\n\n");
                
    }
    
    @Override
    protected void additionalMembers(Appendable target) throws IOException {
        
        msgGenerator.defineMembers(target);
        msgGenerator.incUsesCount();
        
        //need one generator for each, but we may not use them all.
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        int[] tokens = from.tokens;
        long[] scriptIds = from.fieldIdScript;                
        
        int i = tokens.length;
        while (--i>=0) {
            
            int type = TokenBuilder.extractType(tokens[i]);
            
            if (TypeMask.isLong(type) || TypeMask.isInt(type) || TypeMask.isText(type)) {                        
                boolean isLong =TypeMask.isLong(type);
                boolean isSigned = !TypeMask.isUnsigned(type);
                boolean isNullable = TypeMask.isOptional(type); 
                boolean isChars = TypeMask.isText(type);
                                
                if (isLong && (scriptIds[i]==latencyTimeFieldId)) {
                    //Do not generate fuzz but generate send time time on the fly instead
                    generators[i] = new Literal("System.currentTimeMillis()");
                } else {
                    generators[i] = new FuzzValueGenerator(id,isLong,isSigned,isNullable,isChars);
                }
                generators[i].defineMembers(target);
                generators[i].incUsesCount();
            }
        }
    }
    
    protected void additionalMethods(Appendable target) throws IOException {
    }
    
    @Override
    protected void additionalImports(Appendable target) throws IOException {
       target.append("import com.ociweb.pronghorn.stage.PronghornStage;");       
       target.append("import ").append(schema.getClass().getCanonicalName()).append(";\n");
    }

    @Override
    protected void bodyOfNextMessageIdx(Appendable target) throws IOException {
        msgGenerator.preCall(target);
        
        int startsCount = MessageSchema.from(schema).messageStarts().length;
        
        target.append(tab).append("return Pipe.from(output).messageStarts[(");
        msgGenerator.result(target).append(")%");
        Appendables.appendValue(target, startsCount).append("];\n");
        
    }

    @Override
    protected void bodyOfBusinessProcess(Appendable target, int cursor, int firstField, int fieldCount) throws IOException {
        
        for(int f = firstField; f<(firstField+fieldCount); f++) { 
            generators[f].preCall(target);
        }
        
        appendWriteMethodName(target.append(tab), cursor).append("(\n");        
        
        for(int f = firstField; f<(firstField+fieldCount); f++) { 
            if (f > firstField) {
                target.append(",\n");
            }
            
            generators[f].result(target);
        }
        target.append("\n");
        target.append(tab).append(");\n");
        
    }


}
