package com.ociweb.pronghorn.pipe.util.build;

import java.io.IOException;
import java.util.Arrays;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;
import static com.ociweb.pronghorn.pipe.util.Appendables.*;

public class TemplateProcessGeneratorLowLevelReader extends TemplateProcessGenerator {

    private static final String WORKSPACE = "workspace";
    //Low Level Reader
    
    //TODO: this will be the base class for Protobuf, Avero, Thrif, and internal serialziation bridgest
    
    
    private static final String SeqCountSuffix = "Count";
    
    private final Appendable bodyTarget;
    
    
    private final String tab = "    ";
    private final boolean hasSimpleMessagesOnly; //for simple messages there is no LowLevelStateManager
    private final String stageMgrClassName = LowLevelStateManager.class.getSimpleName();
    private final String stageMgrVarName = "navState"; 
    private final String cursorVarName = "cursor";
    private final String pipeName = Pipe.class.getSimpleName();
    private final String pipeVarName = "input";
    private final String exponentNameSuffix = "Exponent";
    private final String mantissaNameSuffix = "Mantissa";
    private final Class schemaClass;
       
    
    private final CharSequence[] fragmentParaTypes;
    private final CharSequence[] fragmentParaArgs;
    private final CharSequence[] fragmentParaSuff;
    private int fragmentParaCount;
    private int fragmentBusinessCursor;
    private final long[] workspacesDefined;
    private int    workspacesDefinedCount;
    
    
    public TemplateProcessGeneratorLowLevelReader(MessageSchema schema, Appendable bodyTarget) {
        super(schema);
        
        this.hasSimpleMessagesOnly = MessageSchema.from(schema).hasSimpleMessagesOnly;        
        this.bodyTarget = bodyTarget;
        this.schemaClass = schema.getClass();
        
        int maxFieldCount = FieldReferenceOffsetManager.maxFragmentSize(MessageSchema.from(schema));
        
        this.fragmentParaTypes = new String[maxFieldCount];
        this.fragmentParaArgs = new String[maxFieldCount];
        this.fragmentParaSuff = new CharSequence[maxFieldCount];
        this.workspacesDefined = new long[maxFieldCount];
    }
    
    private Appendable appendWorkspaceName(Appendable target, long id) throws IOException {
        return appendHexDigits(target.append(WORKSPACE), id);
    }

    @Override
    protected void preprocessTextfields(String name, long id) throws IOException {    
        int i = workspacesDefinedCount;
        while (--i>=0) {
            if (id == workspacesDefined[i]) {
                return; //already defined
            }
        }
        
        appendWorkspaceName(bodyTarget.append("private final StringBuilder "),id).append(" = new StringBuilder(").append(pipeVarName).append(".maxAvgVarLen);\n");        
    
        workspacesDefined[workspacesDefinedCount++] = id;
    }

    @Override
    protected void preprocessBytefields(String name, long id) throws IOException {  
       
        int i = workspacesDefinedCount;
        while (--i>=0) {
            if (id == workspacesDefined[i]) {
                return; //already defined
            }
        }
        
        appendWorkspaceName(appendClass(bodyTarget.append("private final "), DataInputBlobReader.class, schemaClass).append(" "),id).append(" = new ");
        appendClass(bodyTarget, DataInputBlobReader.class, schemaClass).append("(").append(pipeName).append(");\n");
        
        workspacesDefined[workspacesDefinedCount++] = id;
    }
    
    
    @Override
    protected void processCallerPrep() throws IOException {
                        
        //research question for CS - can we use the name of the method to capture refactoring.
        //System.out.println("helllo "+LowLevelStateManager::activeCursor);
        //LowLevelStateManager.class.getMethods()[0].
        
        bodyTarget.append("\n");
        bodyTarget.append("// GENERATED LOW LEVEL READER \n");
        bodyTarget.append("// # Low level API is the fastest way of reading from a pipe in a business semantic way. \n");
        bodyTarget.append("// # Do not change the order that fields are read, this is fixed when using low level. \n");
        bodyTarget.append("// # Do not remove any field reading, every field must be consumed when using low level. \n");
        bodyTarget.append("\n");
        bodyTarget.append("// Details to keep in mind when you expect the schema to change over time\n");
        bodyTarget.append("// # Low level API is CAN be extensiable in the sense which means ignore unrecognized messages. \n");
        bodyTarget.append("// # Low level API is CAN NOT be extensiable in the sense of dealing with mising or extra/new fields. \n");
        bodyTarget.append("// # Low level API is CAN NOT be extensiable in the sense of dealing with fields encoded with different types. \n"); 
        
        
        bodyTarget.append("\n");
        bodyTarget.append("public void run() {\n");
        bodyTarget.append(tab).append("int ").append(cursorVarName).append(";\n");
        
        if (hasSimpleMessagesOnly) {
            
            bodyTarget.append("if (").append(pipeName).append(".hasContentToRead(").append("input").append(")) {\n");
            bodyTarget.append(cursorVarName).append(" = ").append(pipeName).append(".takeMsgIdx(").append(pipeVarName).append(");\n");
            
            
        } else {
            //if (LowLevelStateManager.isStartNewMessage(navState)) {
            bodyTarget.append(tab).append("if (").append(stageMgrClassName).append(".isStartNewMessage(").append(stageMgrVarName).append(")) {\n");
            //    cursor = Pipe.takeMsgIdx(input);
            bodyTarget.append(tab).append(tab).append(cursorVarName).append(" = ").append(pipeName).append(".takeMsgIdx(").append(pipeVarName).append(");\n");
            //} else {
            bodyTarget.append(tab).append("} else {\n");
            //    cursor = LowLevelStateManager.activeCursor(navState);
            bodyTarget.append(tab).append(tab).append(cursorVarName).append(" = ").append(stageMgrClassName).append(".activeCursor(").append(stageMgrVarName).append(");\n");
            //}
            bodyTarget.append(tab).append("}\n");
            
        }
        //switch(cursor)) {
        bodyTarget.append(tab).append("switch(").append(cursorVarName).append(") {\n");
      
        
    }

    @Override
    protected void processCaller(int cursor) throws IOException {
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
     
        bodyTarget.append(tab).append(tab).append("case ");
        
        appendMessageIdentifier(cursor, schema);
        
        bodyTarget.append(":\n");
        bodyTarget.append(tab).append(tab).append(tab);
                                       appendInternalMethodName(cursor).append("();\n");
                                       
                                       
        //Pipe.confirmLowLevelRead(input, 8);
        int fragmentSizeLiteral = from.fragDataSize[cursor];
        bodyTarget.append(tab).append(tab).append(tab).append(pipeName).append(".confirmLowLevelRead(").append(pipeVarName).append(", ").append(Integer.toString(fragmentSizeLiteral)).append(" /* fragment size */);\n");
                                      
        bodyTarget.append(tab).append(tab).append("break;\n");
        
    }

    

    private void appendMessageIdentifier(int cursor, MessageSchema schema) throws IOException {
        FieldReferenceOffsetManager from = MessageSchema.from(schema);

       bodyTarget.append("/*");
       appendInternalMethodName(cursor);
       bodyTarget.append("*/");
       
        if (schema instanceof MessageSchemaDynamic || null==from.fieldNameScript[cursor]) {
            bodyTarget.append(Integer.toString(cursor));
        } else {
            bodyTarget.append(schema.getClass().getSimpleName()).append(".");
            bodyTarget.append(FieldReferenceOffsetManager.buildMsgConstName(from, cursor));
        }
        
    }

    private Appendable appendInternalMethodName(int cursor) throws IOException {
        return appendFragmentName(bodyTarget.append("processPipe"), cursor);
    }


    private Appendable appendFragmentName(Appendable target, int cursor) throws IOException {
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        if (null!=from.fieldNameScript[cursor]) {
            //if this is NOT a message start then also prefix with its name
            if ( Arrays.binarySearch(from.messageStarts, cursor)<0) {
                appendMessageName(cursor, from);
            }
            target.append(from.fieldNameScript[cursor]);
        } else {
            //Go Up and find parent name then add cursor position to make it unique
            int msgCursor = appendMessageName(cursor, from);
            appendSequenceName(cursor, from, msgCursor);
            target.append("End");
        }
        return target;
    }


    private void appendSequenceName(int cursor, FieldReferenceOffsetManager from, int msgCursor) throws IOException {
        int j = cursor;
        final int depth = from.fragDepth[j];
        String foundName = null;
        while (--j>=msgCursor) {
            if (from.fragDepth[j]==depth) {
                if (null!=from.fieldNameScript[j]) {
                    foundName = from.fieldNameScript[j];
                }
            }
        }
        bodyTarget.append(foundName);
    }


    private int appendMessageName(int cursor, FieldReferenceOffsetManager from) throws IOException {
        int i = cursor;
        while (--i>=0 && (null==from.fieldNameScript[i] || Arrays.binarySearch(from.messageStarts, i)<0)  ) {}            
        bodyTarget.append(from.fieldNameScript[i]);
        return i;
    }

    @Override
    protected void processCallerPost() throws IOException {
        
        //-1 for shutdown
        //default ignore new stuff..
        
        bodyTarget.append(tab).append("}\n");
               
        //Pipe.releaseReads{input);
        bodyTarget.append(tab).append(pipeName).append(".releaseReads(").append(pipeVarName).append(");\n");
          
        
        bodyTarget.append("}\n");
        bodyTarget.append("\n");
        
    }
    
    

    @Override
    protected void processCalleeOpen(int cursor) throws IOException {
        
        bodyTarget.append("private void ");
        appendInternalMethodName(cursor).append("() {\n");
        fragmentParaCount = 0;
        
    }
    
    @Override
    protected void processCalleeClose(int cursor) throws IOException {
        
        businessMethodCall(cursor);
        
        bodyTarget.append("}\n");
        bodyTarget.append("\n");
        
        businessMethodDefine();
    }
    
    private void businessMethodDefine() throws IOException {
        if (fragmentParaCount<0) {
            //must be negative to know that we used the call
            fragmentParaCount = -fragmentParaCount;
            bodyTarget.append("protected void businessMethod");
            appendFragmentName(bodyTarget,fragmentBusinessCursor);
            bodyTarget.append("(");
            for(int i=0;i<fragmentParaCount;i++) {
                if (i>0) {
                    bodyTarget.append(", ");
                }
                bodyTarget.append(fragmentParaTypes[i]).append(' ').append(fragmentParaArgs[i]).append(fragmentParaSuff[i]);
            }
            bodyTarget.append(") {\n");
            bodyTarget.append("}\n");
            bodyTarget.append("\n");
            
            fragmentParaCount = 0;
        }
    }
    
    private void businessMethodCall(int cursor) throws IOException {
        
        if (fragmentParaCount>0) {
            //must be positive to know that we have not already defined the call
            bodyTarget.append(tab).append("businessMethod");
            appendFragmentName(bodyTarget,cursor);
            bodyTarget.append("(");
            for(int i=0;i<fragmentParaCount;i++) {
                if (i>0) {
                    bodyTarget.append(", ");
                }
                bodyTarget.append(fragmentParaArgs[i]).append(fragmentParaSuff[i]);
            }
            bodyTarget.append(");\n");
            fragmentParaCount = -fragmentParaCount;
            fragmentBusinessCursor = cursor;
            
        }
    }


    @Override
    protected void processFragmentOpen(String string, int cursor, long id) throws IOException {
        //System.out.println("processFragmentOpen");
        //nothing to do
    }

    @Override
    protected void processSequenceOpen(int fragmentCursor, String name, int idx, int fieldCursor, long id) throws IOException {
        if (hasSimpleMessagesOnly) {
            
            throw new UnsupportedOperationException();
            
        } else {
            
            String varName = varBuilder(name);
            
            fragmentParaTypes[fragmentParaCount] = "int";
            fragmentParaArgs[fragmentParaCount] = varName;
            fragmentParaSuff[fragmentParaCount++] = SeqCountSuffix;
            
            
            bodyTarget.append(tab).append("int ");
            appendSequenceCounterVar(varName);
            bodyTarget.append(" = ").append(pipeName).append(".takeValue(").append(pipeVarName).append(");");
            
            
            
            if (0!=id) {
                bodyTarget.append("/* id:").append(Long.toString(id));
                bodyTarget.append("  */");
            }
            bodyTarget.append("\n");
            
            bodyTarget.append(tab).append(stageMgrClassName).append(".processGroupLength(").append(stageMgrVarName).append(", ").append(Integer.toString(fragmentCursor)).append(", ");
            appendSequenceCounterVar(varName);
            bodyTarget.append(");\n");
            
        }
    }


    private void appendSequenceCounterVar(String varName) throws IOException {
        bodyTarget.append(varName).append(SeqCountSuffix);
    }



    private String varBuilder(String name) {
        
        //TODO: add validation into the SAX parse step.
        return 'p'+name.replace(' ', '_'); //TODO: build appendableUtility for this.
        
    }
    
    @Override
    protected void processFragmentClose() throws IOException {
        
        if (!hasSimpleMessagesOnly) {
            //LowLevelStateManager.closeFragment(navState);
            bodyTarget.append(tab).append(stageMgrClassName).append(".closeFragment(").append(stageMgrVarName).append(");\n");
        }        
    }

    @Override
    protected void processDictionary() throws IOException {
        //System.out.println("processDictionary");
        //nothing to do
    }

    @Override
    protected void processByteArrayOptional(String name, int idx, int fieldCursor, long id) throws IOException {
        fragmentParaTypes[fragmentParaCount] = "DataInput";
        fragmentParaArgs[fragmentParaCount] = WORKSPACE;
        fragmentParaSuff[fragmentParaCount++] = appendHexDigits(new StringBuilder(), id); 
        
        appendWorkspaceName(bodyTarget.append("this."), id).append(".openLowLevelAPIField();\n");
        
        appendWorkspaceName(bodyTarget.append("DataInput "), id).append(" =  ");
        appendWorkspaceName(bodyTarget.append("this."), id).append(".nullable();\n");
                
    }

    @Override
    protected void processByteArray(String name, int idx, int fieldCursor, long id) throws IOException {
        
        fragmentParaTypes[fragmentParaCount] = "DataInput";
        fragmentParaArgs[fragmentParaCount] = WORKSPACE;
        fragmentParaSuff[fragmentParaCount++] = appendHexDigits(new StringBuilder(), id); 

        appendWorkspaceName(bodyTarget, id).append(".openLowLevelAPIField();\n");
        
    }

    private void appendTextRead(long id, String methodName) throws IOException {
        appendWorkspaceName(bodyTarget.append(tab), id).append(".setLength(0);\n");
        appendWorkspaceName(bodyTarget.append(tab).append("StringBuilder "), id).append(" = ").append(pipeName).append('.').append(methodName).append('(').
        append(pipeVarName).append(", this.");
        appendWorkspaceName(bodyTarget, id).append(", ").                                                      
        append(pipeName).append(".takeRingByteMetaData(").append(pipeVarName).append("), ").
        append(pipeName).append(".takeRingByteLen(").append(pipeVarName).append("));\n");
    }
    
    @Override
    protected void processTextUTF8Optional(String name, int idx, int fieldCursor, long id) throws IOException {
        
        fragmentParaTypes[fragmentParaCount] = "StringBuilder";
        fragmentParaArgs[fragmentParaCount] = WORKSPACE;
        fragmentParaSuff[fragmentParaCount++] = appendHexDigits(new StringBuilder(), id); 

        appendTextRead(id, "readOptionalUTF8");
    }


    @Override
    protected void processTextUTF8(String name, int idx, int fieldCursor, long id) throws IOException {
                
        fragmentParaTypes[fragmentParaCount] = "StringBuilder";
        fragmentParaArgs[fragmentParaCount] = WORKSPACE;
        fragmentParaSuff[fragmentParaCount++] = appendHexDigits(new StringBuilder(), id); 
        
        appendTextRead(id, "readUTF8");
    }

    @Override
    protected void processTextASCIIOptional(String name, int idx, int fieldCursor, long id) throws IOException {
        
        fragmentParaTypes[fragmentParaCount] = "StringBuilder";
        fragmentParaArgs[fragmentParaCount] = WORKSPACE;
        fragmentParaSuff[fragmentParaCount++] = appendHexDigits(new StringBuilder(), id); 

        appendTextRead(id, "readOptionalASCII");
    }

    @Override
    protected void processTextASCII(String name, int idx, int fieldCursor, long id) throws IOException {
                
        fragmentParaTypes[fragmentParaCount] = "StringBuilder";
        fragmentParaArgs[fragmentParaCount] = WORKSPACE;
        fragmentParaSuff[fragmentParaCount++] = appendHexDigits(new StringBuilder(), id); 

        appendTextRead(id, "readASCII");
    }

    @Override
    protected void processDecimalOptional(String name, int idx, int fieldCursor, long id) throws IOException {
        
        String varName = varBuilder(name);

        fragmentParaTypes[fragmentParaCount] = "Integer";
        fragmentParaArgs[fragmentParaCount] = varName;
        fragmentParaSuff[fragmentParaCount++] = exponentNameSuffix;        
        
        fragmentParaTypes[fragmentParaCount] = "long";
        fragmentParaArgs[fragmentParaCount] = varName;
        fragmentParaSuff[fragmentParaCount++] = exponentNameSuffix;    
        
        bodyTarget.append(tab).append("Integer ").append(varName).append(exponentNameSuffix).append(" = ").append(pipeName).append(".takeOptionalValue(").append(pipeVarName).append(");/* id:").append(Long.toString(id)).append(" */\n");
        bodyTarget.append(tab).append("long ").append(varName).append(mantissaNameSuffix).append(" = ").append(pipeName).append(".takeLong(").append(pipeVarName).append(");/* id:").append(Long.toString(id)).append(" */\n");
        bodyTarget.append(tab).append("/* The decimal is 'null' if the exponent value is null */");
        
    }

    @Override
    protected void processDecimal(String name, int idx, int fieldCursor, long id) throws IOException {
        
        String varName = varBuilder(name);
        
        fragmentParaTypes[fragmentParaCount] = "int";
        fragmentParaArgs[fragmentParaCount] = varName;
        fragmentParaSuff[fragmentParaCount++] = exponentNameSuffix;
                
        fragmentParaTypes[fragmentParaCount] = "long";
        fragmentParaArgs[fragmentParaCount] = varName;
        fragmentParaSuff[fragmentParaCount++] = mantissaNameSuffix;
        
        bodyTarget.append(tab).append("int ").append(varName).append(exponentNameSuffix).append(" = ").append(pipeName).append(".takeValue(").append(pipeVarName).append(");/* id:").append(Long.toString(id)).append(" */\n");
        bodyTarget.append(tab).append("long ").append(varName).append(mantissaNameSuffix).append(" = ").append(pipeName).append(".takeLong(").append(pipeVarName).append(");/* id:").append(Long.toString(id)).append(" */\n");

    }

    @Override
    protected void processLongUnsignedOptional(String name, int idx, int fieldCursor, long id) throws IOException {
        String varName = varBuilder(name);
        
        fragmentParaTypes[fragmentParaCount] = "Long";
        fragmentParaArgs[fragmentParaCount] = varName;
        fragmentParaSuff[fragmentParaCount++] = "";
        
        bodyTarget.append(tab).append("Long ").append(varName).append(" = ").append(pipeName).append(".takeOptionalLong(").append(pipeVarName).append(");/* id:").append(Long.toString(id)).append(" */\n");
    }

    @Override
    protected void processLongSignedOptional(String name, int idx, int fieldCursor, long id) throws IOException {
        String varName = varBuilder(name);
        
        fragmentParaTypes[fragmentParaCount] = "Long";
        fragmentParaArgs[fragmentParaCount] = varName;
        fragmentParaSuff[fragmentParaCount++] = "";
        
        bodyTarget.append(tab).append("Long ").append(varName).append(" = ").append(pipeName).append(".takeOptionalLong(").append(pipeVarName).append(");/* id:").append(Long.toString(id)).append(" */\n");
    }

    @Override
    protected void processLongUnsigned(String name, int idx, int fieldCursor, long id) throws IOException {
        String varName = varBuilder(name);
        
        fragmentParaTypes[fragmentParaCount] = "long";
        fragmentParaArgs[fragmentParaCount] = varName;
        fragmentParaSuff[fragmentParaCount++] = "";
        
        // long truckId = Pipe.takeLong(input);
        bodyTarget.append(tab).append("long ").append(varName).append(" = ").append(pipeName).append(".takeLong(").append(pipeVarName).append(");/* id:").append(Long.toString(id)).append(" */\n");
    }

    @Override
    protected void processLongSigned(String name, int idx, int fieldCursor, long id) throws IOException {
        String varName = varBuilder(name);
        
        fragmentParaTypes[fragmentParaCount] = "long";
        fragmentParaArgs[fragmentParaCount] = varName;
        fragmentParaSuff[fragmentParaCount++] = "";
        
        // long truckId = Pipe.takeLong(input);
        bodyTarget.append(tab).append("long ").append(varName).append(" = ").append(pipeName).append(".takeLong(").append(pipeVarName).append(");/* id:").append(Long.toString(id)).append(" */\n");
    }

    @Override
    protected void processIntegerUnsignedOptional(String name, int i, int fieldCursor, long id) throws IOException {
        String varName = varBuilder(name);
        
        fragmentParaTypes[fragmentParaCount] = "Integer";
        fragmentParaArgs[fragmentParaCount] = varName;
        fragmentParaSuff[fragmentParaCount++] = "";
        
        bodyTarget.append(tab).append("Integer ").append(varName).append(" = ").append(pipeName).append(".takeOptionalValue(").append(pipeVarName).append(");/* id:").append(Long.toString(id)).append(" */\n");
    }

    @Override
    protected void processIntegerSignedOptional(String name, int i, int fieldCursor, long id) throws IOException {
        String varName = varBuilder(name);
        
        fragmentParaTypes[fragmentParaCount] = "Integer";
        fragmentParaArgs[fragmentParaCount] = varName;
        fragmentParaSuff[fragmentParaCount++] = "";
        
        bodyTarget.append(tab).append("Integer ").append(varName).append(" = ").append(pipeName).append(".takeOptionalValue(").append(pipeVarName).append(");/* id:").append(Long.toString(id)).append(" */\n");
    }

    @Override
    protected void processIntegerUnsigned(String name, int i, int fieldCursor, long id) throws IOException {
        String varName = varBuilder(name);
        
        fragmentParaTypes[fragmentParaCount] = "int";
        fragmentParaArgs[fragmentParaCount] = varName;
        fragmentParaSuff[fragmentParaCount++] = "";
        
       //int justonemorequestion = Pipe.takeValue(input);
        bodyTarget.append(tab).append("int ").append(varName).append(" = ").append(pipeName).append(".takeValue(").append(pipeVarName).append(");/* id:").append(Long.toString(id)).append(" */\n");
    }

    @Override
    protected void pronghornIntegerSigned(String name, int i, int fieldCursor, long id) throws IOException {
        String varName = varBuilder(name);
        
        fragmentParaTypes[fragmentParaCount] = "int";
        fragmentParaArgs[fragmentParaCount] = varName;
        fragmentParaSuff[fragmentParaCount++] = "";
        
        //int justonemorequestion = Pipe.takeValue(input);
        bodyTarget.append(tab).append("int ").append(varName).append(" = ").append(pipeName).append(".takeValue(").append(pipeVarName).append(");/* id:").append(Long.toString(id)).append(" */\n");
    }


    @Override
    protected void postProcessSequence(int fieldCursor) throws IOException {
        if (hasSimpleMessagesOnly) {            
            throw new UnsupportedOperationException();            
        } else {        
            bodyTarget.append(tab).append(stageMgrClassName).append(".continueAtThisCursor(").append(stageMgrVarName).append(", ");
            bodyTarget.append("/*");
            appendInternalMethodName(fieldCursor);
            bodyTarget.append("*/");
            bodyTarget.append(Integer.toString(fieldCursor)).append(");\n");
        }
    }

    @Override
    protected void processMessageClose(String name, long id, boolean needsToCloseFragment) throws IOException {
        
        if (!hasSimpleMessagesOnly && needsToCloseFragment) {    
            bodyTarget.append(tab).append(stageMgrClassName).append(".closeFragment(").append(stageMgrVarName).append(");\n");  
        }

    }
    
    

    @Override
    protected boolean processSequenceInstanceClose(String name, long id, int cursor) throws IOException {
        if (hasSimpleMessagesOnly) {            
            throw new UnsupportedOperationException();            
        } else { 
            
            businessMethodCall(cursor);
            
            bodyTarget.append(tab).append("if (!").append(stageMgrClassName).append(".closeSequenceIteration(").append(stageMgrVarName).append(")) {\n");
            bodyTarget.append(tab).append(tab).append("return; /* Repeat this fragment*/\n");
            bodyTarget.append(tab).append("}\n");
            
            processEndOfSequence(name,id); //hook to add other code to process and end of full sequence
                
            bodyTarget.append(tab).append(stageMgrClassName).append(".closeFragment(").append(stageMgrVarName).append(");\n");
            
        }
        
        //this is fixed because we are doing code generation
        return false;
    }


    protected void processEndOfSequence(String name, long id) {
    }






}
