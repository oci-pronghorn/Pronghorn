package com.ociweb.pronghorn.pipe.util.build;

import static com.ociweb.pronghorn.util.Appendables.*;

import java.io.IOException;
import java.util.Arrays;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.util.Appendables;

public class TemplateProcessGeneratorLowLevelReader extends TemplateProcessGenerator {

    private static final String WORKSPACE = "workspace";
    //Low Level Reader
    
    //TODO: this will be the base class for Protobuf, Avero, Thrif, and internal serialziation bridgest
        
    
    private final Appendable bodyTarget;
    
    
    private final String tab = "    ";
    private final boolean hasSimpleMessagesOnly; //for simple messages there is no LowLevelStateManager
    private final String stageMgrClassName = LowLevelStateManager.class.getSimpleName();
    private final String stageMgrVarName = "navState"; 
    private static final String SeqCountSuffix = "Count";
    private final String cursorVarName = "cursor";
    private final String pipeVarName;
    private final String exponentNameSuffix = "Exponent";
    private final String mantissaNameSuffix = "Mantissa";
    private final Class schemaClass;
    private final Class pipeClass = Pipe.class;
       
    
    private final CharSequence[] fragmentParaTypes;
    private final CharSequence[] fragmentParaArgs;
    private final CharSequence[] fragmentParaSuff;
    private int fragmentParaCount;
    private int fragmentBusinessCursor;
    private int fragmentBusinessState;
    private boolean fragmentBusinessComma = false;
    private final long[] workspacesDefined;
    private int    workspacesDefinedCount;
    
    private boolean inLinedMethod = true;
    private StringBuilder workspace1;
    
    private final String packageName;
    private final String className;
    
    protected final String readerName = "reader";
    
    public TemplateProcessGeneratorLowLevelReader(MessageSchema schema, Appendable bodyTarget) {
        super(schema);
        
        this.hasSimpleMessagesOnly = MessageSchema.from(schema).hasSimpleMessagesOnly;        
        this.bodyTarget = bodyTarget;
        this.schemaClass = schema.getClass();
        this.pipeVarName = "input";
        
        int maxFieldCount = FieldReferenceOffsetManager.maxFragmentSize(MessageSchema.from(schema));
        
        this.fragmentParaTypes = new CharSequence[maxFieldCount];
        this.fragmentParaArgs = new CharSequence[maxFieldCount];
        this.fragmentParaSuff = new CharSequence[maxFieldCount];
        this.workspacesDefined = new long[maxFieldCount];
        
        this.className = "LowLevelReader";//TODO: should be passed in 
        this.packageName = "com.ociweb.pronghorn.pipe.build";//TODO: shuld be passed in/
        //Startup Method
        /// only create navstate if needed
        //  create workspace objects
        //  
        
        
    }
    
    private Appendable appendWorkspaceName(Appendable target, long id) throws IOException {
        return appendHexDigits(target.append(WORKSPACE), id);
    }

    public String getClassName() {
        return className;
    }
    
    public String getPackageName() {
        return packageName;
        
    }
    
    protected void additionalTokens(Appendable target) throws IOException {
    }
    
    public void processSchema() throws IOException {
    

        
        final FieldReferenceOffsetManager from = MessageSchema.from(schema);
        
        workspacesDefinedCount = 0;
       
                        
        super.processSchema();
        
        //place at the end the business methods which are overridden
        bodyTarget.append('\n').append('\n').append(workspace1);
        
    }

    protected void helperMethods(final FieldReferenceOffsetManager from) throws IOException {
        bodyTarget.append("\n");
        bodyTarget.append("public void startup() {\n");
        
        bodyTarget.append(tab).append(pipeClass.getSimpleName()).append(".from(").append(pipeVarName).append(").validateGUID(FROM_GUID);\n");
       
        // Pipe.from(input).validateGUID(FROM_GUID);
        
        
        if (!from.hasSimpleMessagesOnly) {
          bodyTarget.append(tab).append("navState = new ").append(LowLevelStateManager.class.getSimpleName()).append("(").append(pipeClass.getSimpleName()).append(".from(").append(pipeVarName).append("));\n"); 
        }  
        
        for(int cursor = 0; cursor<from.tokens.length; cursor++) {
            String name = from.fieldNameScript[cursor];
            long id = from.fieldIdScript[cursor];
            int token = from.tokens[cursor];
            int type = TokenBuilder.extractType(token);
            if (TypeMask.TextASCII==type |
                TypeMask.TextASCIIOptional==type |
                TypeMask.TextUTF8==type |
                TypeMask.TextUTF8Optional==type) {
                
                preprocessTextfieldsAssign(name, id);
                 
            } else if (TypeMask.ByteArray==type | 
                       TypeMask.ByteArrayOptional==type) {
                
                preprocessBytefieldsAssign(name, id);

            }   
        }
        additionalTokens(bodyTarget);
        bodyTarget.append("}\n");
    }
    
    private void preprocessTextfieldsDef(String name, long id) throws IOException {    
        int i = workspacesDefinedCount;
        while (--i>=0) {
            if (id == workspacesDefined[i]) {
                return; //already defined
            }
        }
        
        appendWorkspaceName(bodyTarget.append("private StringBuilder "),id).append(";\n");        
        //.append(" = new StringBuilder(").append(pipeVarName).append(".maxAvgVarLen);\n");        
    
        workspacesDefined[workspacesDefinedCount++] = id;
    }

    private void preprocessBytefieldsDef(String name, long id) throws IOException {  
       
        int i = workspacesDefinedCount;
        while (--i>=0) {
            if (id == workspacesDefined[i]) {
                return; //already defined
            }
        }
        
        appendWorkspaceName(appendClass(bodyTarget.append("private "), DataInputBlobReader.class, schemaClass).append(" "),id).append(";\n");
        //.append(" = new ");
        //appendClass(bodyTarget, DataInputBlobReader.class, schemaClass).append("(").append(pipeVarName).append(");\n");
        
        workspacesDefined[workspacesDefinedCount++] = id;
    }

    private void preprocessTextfieldsAssign(String name, long id) throws IOException {    
        int i = workspacesDefinedCount;
        while (--i>=0) {
            if (id == workspacesDefined[i]) {
                return; //already defined
            }
        }
        appendWorkspaceName(bodyTarget.append(tab),id).append(" = new StringBuilder(").append(pipeVarName).append(".maxAvgVarLen);\n");        
    
        workspacesDefined[workspacesDefinedCount++] = id;
    }

    private void preprocessBytefieldsAssign(String name, long id) throws IOException {  
       
        int i = workspacesDefinedCount;
        while (--i>=0) {
            if (id == workspacesDefined[i]) {
                return; //already defined
            }
        }     
        appendClass(appendWorkspaceName(bodyTarget.append(tab),id).append(" = new "), DataInputBlobReader.class, schemaClass).append("(").append(pipeVarName).append(");\n");
        
        workspacesDefined[workspacesDefinedCount++] = id;
    }
    
    
    @Override
    protected void processCallerPrep() throws IOException {
                        
        FieldReferenceOffsetManager from = MessageSchema.from(schema);

        
        helperMethods(from);
        
        bodyTarget.append("\n");
        bodyTarget.append("@Override\n");
        bodyTarget.append("public void run() {\n");
        
//        if (!Pipe.hasContentToRead(input)) {
//            return;
//        }
        appendStaticCall(bodyTarget.append(tab).append("if (!"), pipeClass, "hasContentToRead").append(pipeVarName).append(")) {\n");
        bodyTarget.append(tab).append(tab).append("return;\n");
        bodyTarget.append(tab).append("}\n");
       
        
        bodyTarget.append(tab).append("int ").append(cursorVarName).append(";\n");
        
        if (hasSimpleMessagesOnly) {
            
            appendStaticCall(bodyTarget.append("if ("), pipeClass,"hasContentToRead").append(pipeVarName).append(")) {\n");            
            appendStaticCall(bodyTarget.append(cursorVarName).append(" = "), pipeClass, "takeMsgIdx").append(pipeVarName).append(");\n");
            
            
        } else {
            //if (LowLevelStateManager.isStartNewMessage(navState)) {
            bodyTarget.append(tab).append("if (").append(stageMgrClassName).append(".isStartNewMessage(").append(stageMgrVarName).append(")) {\n");
            //    cursor = Pipe.takeMsgIdx(input);            
            appendStaticCall(bodyTarget.append(tab).append(tab).append(cursorVarName).append(" = "), pipeClass, "takeMsgIdx").append(pipeVarName).append(");\n");
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
        
        appendStaticCall(bodyTarget.append(tab).append(tab).append(tab), pipeClass, "confirmLowLevelRead").append(pipeVarName).append(", ").append(Integer.toString(fragmentSizeLiteral)).append(" /* fragment size */);\n");
                                      
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
                appendMessageName(target, cursor, from);
            }
            target.append(from.fieldNameScript[cursor]);
        } else {
            //Go Up and find parent name then add cursor position to make it unique
            int msgCursor = appendMessageName(target, cursor, from);
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


    private int appendMessageName(Appendable target, int cursor, FieldReferenceOffsetManager from) throws IOException {
        int i = cursor;
        while (--i>=0 && (null==from.fieldNameScript[i] || Arrays.binarySearch(from.messageStarts, i)<0)  ) {}            
        target.append(from.fieldNameScript[i]);
        return i;
    }

    @Override
    protected void processCallerPost() throws IOException {
        
        bodyTarget.append(tab).append(tab).append("case -1:");
        
        appendStaticCall(bodyTarget.append(tab), pipeClass, "takeMsgIdx").append(pipeVarName).append(");\n");
        appendStaticCall(bodyTarget.append(tab), pipeClass, "takeValue").append(pipeVarName).append(");\n");

        bodyTarget.append(tab).append(tab).append("requestShutdown();\n");
        
        //TODO; consume message can call request shutdown.
        
        bodyTarget.append(tab).append("break;\n");
        
        bodyTarget.append(tab).append(tab).append("default:\n");
        bodyTarget.append(tab).append(tab).append(tab).append("throw new UnsupportedOperationException(\"Unknown message type, rebuid with the new schema.\");\n");

        
        bodyTarget.append(tab).append("}\n"); //close of the switch statement
               
        //Pipe.releaseReads{input);
        
        appendStaticCall(bodyTarget.append(tab), pipeClass, "releaseReads").append(pipeVarName).append(");\n");
                  
        bodyTarget.append("}\n");
        if (hasSimpleMessagesOnly) {
            bodyTarget.append("}\n");
        }
        bodyTarget.append("\n");        
    }
        

    @Override
    protected void processCalleeOpen(int cursor) throws IOException {
        
        bodyTarget.append("private void ");
        appendInternalMethodName(cursor).append("() {\n");
        fragmentParaCount = 0;
        fragmentBusinessState = 0;
        businessMethodStartCall(cursor);
        
    }
    
    @Override
    protected void processCalleeClose(int cursor) throws IOException {
        
        businessMethodCall();
        
        bodyTarget.append("}\n");
        bodyTarget.append("\n");
        
        businessMethodDefine();
    }

    private void businessMethodStartCall(int cursor) throws IOException {
        fragmentBusinessCursor = cursor;
        fragmentBusinessComma = false;
        if (inLinedMethod) {
            bodyTarget.append(tab).append("businessMethod");
            appendFragmentName(bodyTarget,fragmentBusinessCursor);
            bodyTarget.append("(\n");
        }
        
            
    }
    
    
    private void businessMethodCall() throws IOException {
        
        if ( 0 == fragmentBusinessState) {
            //must be positive to know that we have not already defined the call
            if (!inLinedMethod) {
                bodyTarget.append(tab).append("businessMethod");
                appendFragmentName(bodyTarget,fragmentBusinessCursor);
                bodyTarget.append("(");
                                
                for(int i=0;i<fragmentParaCount;i++) {
                    if (i>0) {
                        bodyTarget.append(", ");
                    }
                    bodyTarget.append(fragmentParaArgs[i]).append(fragmentParaSuff[i]);
                }
                
                bodyTarget.append(");\n");
            } else {
            //new codde for method inline
                bodyTarget.append(tab).append(");\n");
            }
            fragmentBusinessState = 1;
        }
    }
    
    private void businessMethodDefine() throws IOException {
        if (1==fragmentBusinessState) {
                       
            Appendable target = bodyTarget;
            //must be negative to know that we used the call
            target.append("protected void businessMethod");
            appendFragmentName(target,fragmentBusinessCursor);
            target.append("(");
            for(int i=0;i<fragmentParaCount;i++) {
                if (i>0) {
                    target.append(", ");
                }
                target.append(fragmentParaTypes[i]).append(' ').append(fragmentParaArgs[i]).append(fragmentParaSuff[i]);
            }
            target.append(") {\n");
          
            bodyBuilder(schema, fragmentBusinessCursor, fragmentParaCount,fragmentParaTypes,fragmentParaArgs,fragmentParaSuff);
            target.append("}\n");
            target.append("\n");
            
            fragmentParaCount = 0;
        }
    }


    //override this method !!
    protected void bodyBuilder(MessageSchema schema, int cursor, int fragmentParaCount, CharSequence[] fragmentParaTypes, CharSequence[] fragmentParaArgs, CharSequence[] fragmentParaSuff) {
        //This is example code only to be tossed.
        
        //The runtime code should call this once,  this comment is code to be generated but NOT here.
        //int[] intDictionary = from.newIntDefaultsDictionary()
        //intp[ prevDictionary = ....
        
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
                
        int curCursor = cursor;
                
        long activePmap = 0 ;
        for(int paramIdx = 0; paramIdx<fragmentParaCount; paramIdx++) {
            
            String varName = new StringBuilder().append(fragmentParaArgs[paramIdx]).append(fragmentParaSuff[paramIdx]).toString();
            String varType = new StringBuilder().append(fragmentParaTypes[paramIdx]).toString();
            int token = from.tokens[curCursor];
            
            
            
            //Good stuff goes here.
            //this comment is an example of what should be generated not executed here.
            //  activePmap = pmapBuilding( activePmap,  token.  <varName> , initDictionary[curCursor], prev??
             /// varName is the name of the variable that holds the cur value.
            //Which pmap should be called??? switch on varType.
            
            
            curCursor +=  TypeMask.scriptTokenSize[TokenBuilder.extractType(token)];
            
        }
        
        //now THE PMAP IS BUILT.
        
        
        //NOW GENERATE WRITE IT CODE
        
        
        
        
        
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
            CharSequence varName;
                        
            fragmentParaTypes[fragmentParaCount] = "int";
            fragmentParaArgs[fragmentParaCount] = varName = appendVar(new StringBuilder(), name);
            fragmentParaSuff[fragmentParaCount++] = SeqCountSuffix;
         
            if (!inLinedMethod) {
                appendSequenceCounterVar(bodyTarget.append(tab).append("int "), varName).append(" = ");
                
                appendStaticCall(bodyTarget, pipeClass, "takeValue").append(pipeVarName).append(");");
                if (0!=id) {
                    bodyTarget.append("/* id:").append(Long.toString(id));
                    bodyTarget.append("  */");
                }
                bodyTarget.append("\n");
                
                businessMethodCall();
                
                bodyTarget.append(tab).append(stageMgrClassName).append(".processGroupLength(").append(stageMgrVarName).append(", ").append(Integer.toString(fragmentCursor)).append(", ");
                appendSequenceCounterVar(bodyTarget, varName).append(");\n");
            } else {
                if (fragmentBusinessComma) {
                    bodyTarget.append(",\n");
                }
                bodyTarget.append(tab).append(tab);                
                
                bodyTarget.append(tab).append(stageMgrClassName).append(".processGroupLength(").append(stageMgrVarName).append(", ").append(Integer.toString(fragmentCursor)).append(", ");                
                appendStaticCall(bodyTarget, pipeClass, "takeValue").append(pipeVarName).append(")");
                if (0!=id) {
                    bodyTarget.append("/* id:").append(Long.toString(id));
                    bodyTarget.append("  */");
                }
                
                bodyTarget.append(")");
                fragmentBusinessComma = true;
                businessMethodCall();
                
            }
            
            

            
        }
    }


    private Appendable appendSequenceCounterVar(Appendable target, CharSequence varName) throws IOException {
        return target.append(varName).append(SeqCountSuffix);
    }

    private <A extends Appendable> A appendVar(A target, String name) throws IOException {
       return (A)target.append('p').append(name.replace(' ', '_')); //TODO: this replacement code should be doen in Appendable.
    }
    
    @Override
    protected void processFragmentClose(int cursor) throws IOException {
        
        if (!hasSimpleMessagesOnly) {
            businessMethodCall();
            //LowLevelStateManager.closeFragment(navState);
            appendStaticCall(bodyTarget.append(tab), LowLevelStateManager.class, "closeFragment").append(stageMgrVarName).append(");\n");
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
        
        if (!inLinedMethod) {
            appendWorkspaceName(bodyTarget.append("DataInput "), id).append(" =  ");
        } else {
            bodyTarget.append(tab).append(tab).append(tab);
        }
        appendWorkspaceName(bodyTarget.append("this."), id).append(".nullable()"); 
        
        if (!inLinedMethod) {            
            bodyTarget.append(";\n");
        }
                
    }

    @Override
    protected void processByteArray(String name, int idx, int fieldCursor, long id) throws IOException {
        
        fragmentParaTypes[fragmentParaCount] = "DataInput";
        fragmentParaArgs[fragmentParaCount] = WORKSPACE;
        fragmentParaSuff[fragmentParaCount++] = appendHexDigits(new StringBuilder(), id); 

        appendWorkspaceName(bodyTarget, id).append(".openLowLevelAPIField()"); 
        if (!inLinedMethod) {   
            bodyTarget.append(";\n");
        }
        
    }

    private void appendTextRead(long id, String methodName) throws IOException {
        
        if (!inLinedMethod) {
            appendWorkspaceName(bodyTarget.append(tab).append("StringBuilder "), id).append(" = ");
        } else {
            if (fragmentBusinessComma) {
                bodyTarget.append(",\n");
            }
            bodyTarget.append(tab).append(tab).append(tab);
        }
        appendStaticCall(bodyTarget, pipeClass, methodName). append(pipeVarName).append(", ");
        appendStaticCall(
                          appendStaticCall(
                                   appendWorkspaceName(appendStaticCall(bodyTarget, Appendables.class, "truncate").append("this.") , id).append(")").
                                   append(", "), 
                                   pipeClass, 
                                   "takeRingByteMetaData").
                          append(pipeVarName).append("), "), 
                          pipeClass, 
                          "takeRingByteLen").
        append(pipeVarName).append("))"); 
        if (!inLinedMethod) {   
            bodyTarget.append(";\n");
        } else {
            fragmentBusinessComma = true;
        }
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
        int absent32Value = FieldReferenceOffsetManager.getAbsent32Value(MessageSchema.from(schema));
        
        CharSequence varName;

        fragmentParaTypes[fragmentParaCount] = "Integer";
        fragmentParaArgs[fragmentParaCount] = varName = appendVar(new StringBuilder(), name);
        fragmentParaSuff[fragmentParaCount++] = exponentNameSuffix;        
        
        if (!inLinedMethod) {
            bodyTarget.append(tab).append("Integer ").append(varName).append(exponentNameSuffix).append(" = ");            
        } else {
            if (fragmentBusinessComma) {
                bodyTarget.append(",\n");
            }
            bodyTarget.append(tab).append(tab).append(tab);
        }
        appendStaticCall(bodyTarget, pipeClass, "takeOptionalValue").append(pipeVarName).append(',');
        Appendables.appendValue(bodyTarget, absent32Value).append(")");
        
        if (!inLinedMethod) {   
            bodyTarget.append(";/* id:").append(Long.toString(id)).append(" */\n");
        } else {
            fragmentBusinessComma = true;
        }                
        
        readMantissa(id, varName);
        
        if (!inLinedMethod) {   
            bodyTarget.append(tab).append("/* The decimal is 'null' if the exponent value is null */");
        }
        
    }

    @Override
    protected void processDecimal(String name, int idx, int fieldCursor, long id) throws IOException {
        
        CharSequence varName;
        
        fragmentParaTypes[fragmentParaCount] = "int";
        fragmentParaArgs[fragmentParaCount] = varName = appendVar(new StringBuilder(), name);
        fragmentParaSuff[fragmentParaCount++] = exponentNameSuffix;
                
        
        if (!inLinedMethod) {
            bodyTarget.append(tab).append("int ").append(varName).append(exponentNameSuffix).append(" = ");
        } else {
            if (fragmentBusinessComma) {
                bodyTarget.append(",\n");
            }
            bodyTarget.append(tab).append(tab).append(tab);
        }
        appendStaticCall(bodyTarget, pipeClass, "takeValue").append(pipeVarName).append(")"); 
        if (!inLinedMethod) {   
            bodyTarget.append(";/* id:").append(Long.toString(id)).append(" */\n"); 
        } else {
            fragmentBusinessComma = true;
        }      
        
        readMantissa(id, varName);
    }

    private void readMantissa(long id, CharSequence varName) throws IOException {
        fragmentParaTypes[fragmentParaCount] = "long";
        fragmentParaArgs[fragmentParaCount] = varName;
        fragmentParaSuff[fragmentParaCount++] = mantissaNameSuffix;
        if (!inLinedMethod) {
            bodyTarget.append(tab).append("long ").append(varName).append(mantissaNameSuffix).append(" = ");
        } else {
            if (fragmentBusinessComma) {
                bodyTarget.append(",\n");
            }
            bodyTarget.append(tab).append(tab).append(tab);
        }
        appendStaticCall( bodyTarget, pipeClass, "takeLong").append(pipeVarName).append(")");
        if (!inLinedMethod) {   
            bodyTarget.append(";/* id:").append(Long.toString(id)).append(" */\n");
        } else {
            fragmentBusinessComma = true;
        }
    }

    @Override
    protected void processLongUnsignedOptional(String name, int idx, int fieldCursor, long id) throws IOException {
        readOptionalLong(name, id);
    }

    @Override
    protected void processLongSignedOptional(String name, int idx, int fieldCursor, long id) throws IOException {
        readOptionalLong(name, id);
    }

    private void readOptionalLong(String name, long id) throws IOException {
        CharSequence varName;
        long absent64Value = FieldReferenceOffsetManager.getAbsent64Value(MessageSchema.from(schema));
        
        fragmentParaTypes[fragmentParaCount] = "Long";
        fragmentParaArgs[fragmentParaCount] = varName = appendVar(new StringBuilder(), name);
        fragmentParaSuff[fragmentParaCount++] = "";
        
        if (!inLinedMethod) {
            bodyTarget.append(tab).append("Long ").append(varName).append(" = ");
        } else {
            if (fragmentBusinessComma) {
                bodyTarget.append(",\n");
            }
            bodyTarget.append(tab).append(tab).append(tab);
        }
        appendStaticCall(bodyTarget, pipeClass, "takeOptionalLong").append(pipeVarName).append(','); 
        Appendables.appendValue(bodyTarget, absent64Value).append(")");
        
        if (!inLinedMethod) {   
            bodyTarget.append(";/* id:").append(Long.toString(id)).append(" */\n");
        } else {
            fragmentBusinessComma = true;
        }
    }

    @Override
    protected void processLongUnsigned(String name, int idx, int fieldCursor, long id) throws IOException {
        readRequiredPrimitive(name, id, "long", "takeLong");
    }

    @Override
    protected void processLongSigned(String name, int idx, int fieldCursor, long id) throws IOException {
        readRequiredPrimitive(name, id, "long", "takeLong");
    }

    @Override
    protected void processIntegerUnsignedOptional(String name, int i, int fieldCursor, long id) throws IOException {
        readOptionalInteger(name, id);
    }

    @Override
    protected void processIntegerSignedOptional(String name, int i, int fieldCursor, long id) throws IOException {
        readOptionalInteger(name, id);
    }

    public void readOptionalInteger(String name, long id) throws IOException {
        CharSequence varName;
        int absent32Value = FieldReferenceOffsetManager.getAbsent32Value(MessageSchema.from(schema));
        
        fragmentParaTypes[fragmentParaCount] = "Integer";
        fragmentParaArgs[fragmentParaCount] = varName = appendVar(new StringBuilder(), name);
        fragmentParaSuff[fragmentParaCount++] = "";
        
        if (!inLinedMethod) {
            bodyTarget.append(tab).append("Integer ").append(varName).append(" = ");
        } else {
            if (fragmentBusinessComma) {
                bodyTarget.append(",\n");
            }
            bodyTarget.append(tab).append(tab).append(tab);
        }
        appendStaticCall(bodyTarget, pipeClass, "takeOptionalValue").append(pipeVarName).append(','); 
        Appendables.appendValue(bodyTarget, absent32Value).append(")");
        
        if (!inLinedMethod) {   
            bodyTarget.append(";/* id:").append(Long.toString(id)).append(" */\n");
        } else {
            fragmentBusinessComma = true;
        }
    }

    @Override
    protected void processIntegerUnsigned(String name, int i, int fieldCursor, long id) throws IOException {
        readRequiredPrimitive(name, id, "int", "takeValue");
    }

    @Override
    protected void pronghornIntegerSigned(String name, int i, int fieldCursor, long id) throws IOException {
        readRequiredPrimitive(name, id, "int", "takeValue");
    }

    public void readRequiredPrimitive(String name, long id, String typeName, String methodName) throws IOException {
        CharSequence varName = appendVar(new StringBuilder(), name);
        
        fragmentParaTypes[fragmentParaCount] = typeName;
        fragmentParaArgs[fragmentParaCount] = varName;
        fragmentParaSuff[fragmentParaCount++] = "";
        
        //int justonemorequestion = Pipe.takeValue(input);
        if (!inLinedMethod) {
            bodyTarget.append(tab).append(typeName).append(' ').append(varName).append(" = ");
        } else {
            if (fragmentBusinessComma) {
                bodyTarget.append(",\n");
            }
            bodyTarget.append(tab).append(tab).append(tab);
        }
        appendStaticCall(bodyTarget, pipeClass, methodName).append(pipeVarName).append(")"); 
        if (!inLinedMethod) {   
            bodyTarget.append(";/* id:").append(Long.toString(id)).append(" */\n");
        } else {            
            fragmentBusinessComma = true;
        }
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
            businessMethodCall();
            bodyTarget.append(tab).append(stageMgrClassName).append(".closeFragment(").append(stageMgrVarName).append(");\n");  
        }

    }
    
    

    @Override
    protected boolean processSequenceInstanceClose(String name, long id, int cursor) throws IOException {
        if (hasSimpleMessagesOnly) {            
            throw new UnsupportedOperationException();            
        } else { 
            
            businessMethodCall();
            
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

    @Override
    protected void footerConstruction() throws IOException {
       bodyTarget.append("}\n");
    }

    @Override
    protected void headerConstruction() throws IOException {
        bodyTarget.append("package ").append(packageName).append(";\n");
        bodyTarget.append("import ").append(LowLevelStateManager.class.getCanonicalName()).append(";\n");
        bodyTarget.append("import ").append(Pipe.class.getCanonicalName()).append(";\n");
        bodyTarget.append("import ").append(FieldReferenceOffsetManager.class.getCanonicalName()).append(";\n");
        bodyTarget.append("import ").append(Appendables.class.getCanonicalName()).append(";\n");
        bodyTarget.append("import ").append(MessageSchemaDynamic.class.getCanonicalName()).append(";\n");
        bodyTarget.append("import ").append(DataInputBlobReader.class .getCanonicalName()).append(";\n");
        additionalImports(schema, bodyTarget);
        
        bodyTarget.append("public class ").append(className).append(" implements Runnable {\n");

        bodyTarget.append("\n");
        bodyTarget.append("private void requestShutdown() {};\n"); //only here so generated code passes compile.
    }

    protected void additionalImports(MessageSchema schema, Appendable target) {
    }
    
    @Override
    protected void defineMembers() throws IOException {
        final FieldReferenceOffsetManager from = MessageSchema.from(schema);
        
        workspace1 = new StringBuilder();
                
        //TODO added logic for building the startup();
   
        for(int cursor = 0; cursor<from.tokens.length; cursor++) {
            String name = from.fieldNameScript[cursor];
            long id = from.fieldIdScript[cursor];
            int token = from.tokens[cursor];
            int type = TokenBuilder.extractType(token);
            if (TypeMask.TextASCII==type |
                TypeMask.TextASCIIOptional==type |
                TypeMask.TextUTF8==type |
                TypeMask.TextUTF8Optional==type) {
                
                preprocessTextfieldsDef(name, id);
                 
            } else if (TypeMask.ByteArray==type | 
                       TypeMask.ByteArrayOptional==type) {
                
                preprocessBytefieldsDef(name, id);

            } 
        }
        if (!from.hasSimpleMessagesOnly) {
            bodyTarget.append("private LowLevelStateManager navState;\n");
        }
        appendClass(bodyTarget.append("private "), pipeClass, schema.getClass()).append(pipeVarName).append(";\n");
        bodyTarget.append("DataInputBlobReader<" + schema.getClass().getSimpleName() + "> " + readerName + ";");
        //put schema into code
        from.appendConstuctionSource(bodyTarget);



        
        
        
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
        
               
        from.appendGUID( bodyTarget.append("private static final int[] FROM_GUID = ")).append(";\n");
        bodyTarget.append("private static final long BUILD_TIME = ");
        Appendables.appendValue(bodyTarget, System.currentTimeMillis()).append("L;\n");
        //TODO: fix hexDigits it is out of bounds.

        
        
    }


}
