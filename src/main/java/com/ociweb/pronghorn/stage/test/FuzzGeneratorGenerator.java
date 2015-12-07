package com.ociweb.pronghorn.stage.test;

import java.io.IOException;
import java.nio.channels.Pipe;
import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.pronghorn.code.Code;
import com.ociweb.pronghorn.code.FuzzValueGenerator;
import com.ociweb.pronghorn.code.Literal;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.pipe.util.Appendables;
import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGeneratorLowLevelWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class FuzzGeneratorGenerator extends TemplateProcessGeneratorLowLevelWriter{

        
    //Simple random generator stage generated for a given Schema
    
    private static AtomicInteger id = new AtomicInteger();
    private Code[] generators = new Code[MessageSchema.from(schema).tokens.length];
    private Code msgGenerator = new FuzzValueGenerator(id, false, false, false, false);
        //0, MessageSchema.from(schema).messageStarts.length, 
    
    private long latencyTimeFieldId = -1;//undefined
    private int maximumSequenceMask = Integer.MAX_VALUE; //TODO: A should be max fragments on pipe and validated with assert.
    
    private final boolean generateRunnable;
    
    //TODO: Add fixed length for sequences support - 20 min
    //TODO: Add sparse population of sequences support. - 40 min
    
    //TODO: Add generator to build Objects as iterator? 10 hours may be helpful to grove work.
    
    public FuzzGeneratorGenerator(MessageSchema schema, Appendable target) {
        this(schema, target, false);
    }
    
    
    public FuzzGeneratorGenerator(MessageSchema schema, Appendable target, boolean generateRunnable, boolean scopeProtected) {
        super(schema, target, generateClassName(schema),  generateRunnable ? "implements Runnable" : "extends PronghornStage",
                                                          generateRunnable ? null : "output",
                                                          scopeProtected ? "protected" : "private",
                                                          false);
        this.generateRunnable = generateRunnable;
    }
    
    public FuzzGeneratorGenerator(MessageSchema schema, Appendable target, boolean generateRunnable) {
        this(schema, target, generateRunnable, generateRunnable);
    }

    private static String generateClassName(MessageSchema schema) {
        if (schema instanceof MessageSchemaDynamic) {
            String name = MessageSchema.from(schema).name.replaceAll("/", "").replaceAll(".xml", "")+"FuzzGenerator";
            if (Character.isLowerCase(name.charAt(0))) {
                return Character.toUpperCase(name.charAt(0))+name.substring(1);
            }
            return name;
        } else {
            return (schema.getClass().getSimpleName().replace("Schema", ""))+"FuzzGenerator";
        }
    }
    
    public void setTimeFieldId(long id) {
        latencyTimeFieldId = id;
    }
    
    public void setMaxSequenceLengthInBits(int saneBits) {
        assert(saneBits>0);
        assert(saneBits<=32);
        maximumSequenceMask = (1<<saneBits)-1;
    }
    
    @Override
    protected void buildConstructors(Appendable target, String className) throws IOException {
        
        target.append("public ").append(className).append("(");
        
        if (generateRunnable) {
            target.append(") { \n");
            
            FieldReferenceOffsetManager from = MessageSchema.from(schema);
            if (!from.hasSimpleMessagesOnly) {
                target.append(tab).append("startup();\n");
            }
            
        } else {        
            target.append(GraphManager.class.getCanonicalName()).append(" gm, ");
            Appendables.appendClass(target, Pipe.class, schema.getClass()).append(" ").append(pipeVarName).append(") {\n");
            
            target.append("super(gm,NONE,").append(pipeVarName).append(");\n");
            target.append("this.").append(pipeVarName).append(" = ").append(pipeVarName).append(";\n"); 
            Appendables.appendStaticCall(target, Pipe.class, "from").append(pipeVarName).append(").validateGUID(FROM_GUID);\n");
        }
        
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
            
            if (TypeMask.isLong(type) || TypeMask.isInt(type) || TypeMask.isText(type) || TypeMask.isByteVector(type)) {                        
                boolean isLong =TypeMask.isLong(type);
                boolean isSigned = !TypeMask.isUnsigned(type);
                boolean isNullable = TypeMask.isOptional(type); 
                boolean isChars = TypeMask.isText(type) || TypeMask.isByteVector(type);
                                
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
       target.append("import ").append(PronghornStage.class.getCanonicalName()).append(";\n");       
       target.append("import ").append(schema.getClass().getCanonicalName()).append(";\n");
    }

    @Override
    protected void bodyOfNextMessageIdx(Appendable target) throws IOException {
        msgGenerator.preCall(target);
        
        int startsCount = MessageSchema.from(schema).messageStarts().length;
        
        if (startsCount==1) {
            target.append(tab).append("return ");
            Appendables.appendValue(target, MessageSchema.from(schema).messageStarts()[0]).append(";\n");
        } else {
            target.append(tab).append("return ");
            
            if (null==pipeVarName) {
                if (!(schema instanceof MessageSchemaDynamic)) {
                    target.append(schema.getClass().getSimpleName()).append(".");
                }
                
                target.append("FROM");
            } else {
                Appendables.appendStaticCall(target, Pipe.class, "from").append(pipeVarName).append(")");
            }
            
            target.append(".messageStarts[(");
            msgGenerator.result(target).append(")%");
            Appendables.appendValue(target, startsCount).append("];\n");
            
            
        }
    }

    @Override
    protected void bodyOfBusinessProcess(Appendable target, int cursor, int firstField, int fieldCount) throws IOException {
        
        for(int f = firstField; f<(firstField+fieldCount); f++) { 
            if (null==generators[f]) {
                throw new UnsupportedOperationException("Unsupported Type "+TokenBuilder.tokenToString(MessageSchema.from(schema).tokens[f]));
            }
            generators[f].preCall(target);
        }
        
        appendWriteMethodName(target.append(tab), cursor).append("(\n");        
        
        for(int f = firstField; f<(firstField+fieldCount); f++) { 
            if (f > firstField) {
                target.append(",\n");
            }
            
            target.append(tab).append(tab);
            if (Integer.MAX_VALUE != maximumSequenceMask &&
                TypeMask.GroupLength == TokenBuilder.extractType(MessageSchema.from(schema).tokens[f]) ) {
                Appendables.appendHexDigits(target, maximumSequenceMask).append("&");
            }
            
            generators[f].result(target);
        }
        target.append("\n");
        target.append(tab).append(");\n");
        
    }


}
