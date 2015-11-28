package com.ociweb.pronghorn.pipe.util.build;

import static com.ociweb.pronghorn.pipe.util.Appendables.appendClass;
import static com.ociweb.pronghorn.pipe.util.Appendables.appendStaticCall;
import static com.ociweb.pronghorn.pipe.util.Appendables.appendValue;

import java.io.IOException;
import java.util.Arrays;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;
import com.ociweb.pronghorn.pipe.util.Appendables;

public class TemplateProcessGeneratorLowLevelWriter extends TemplateProcessGenerator {
    
    private final Appendable bodyTarget;
    
    private final String tab = "    ";
    private final boolean hasSimpleMessagesOnly; //for simple messages there is no LowLevelStateManager
    private final String stageMgrClassName = LowLevelStateManager.class.getSimpleName();
    private final String stageMgrVarName = "navState"; 
    private final String pipeVarName;
    private final Class pipeClass;
    private final StringBuilder businessExampleWorkspace = new StringBuilder();
    private final StringBuilder writeToPipeSignatureWorkspace = new StringBuilder();
    private final StringBuilder writeToPipeBodyWorkspace = new StringBuilder();
    private static final String SeqCountSuffix = "Count";
    private final String cursorVarName = "cursor";
    private final String pipeId;
    private final String doNothingConstantValue = "-3";
    private final String doNothingConstant = "DO_NOTHING";
    
    private boolean firstField = true;
    
    
    public TemplateProcessGeneratorLowLevelWriter(MessageSchema schema, Appendable target, String pipeVarName) {
        super(schema);

        this.pipeId = "1";
        this.pipeVarName = pipeVarName;
        this.pipeClass = Pipe.class;
        this.bodyTarget = target;
        this.hasSimpleMessagesOnly = MessageSchema.from(schema).hasSimpleMessagesOnly;
        
    }
    
    public void processSchema() throws IOException {
        
        final FieldReferenceOffsetManager from = MessageSchema.from(schema);
     
        if (!from.hasSimpleMessagesOnly) {
            bodyTarget.append("private LowLevelStateManager navState;\n");
        }
        appendClass(bodyTarget.append("private "), pipeClass, schema.getClass()).append(pipeVarName).append(";\n");
        
        
        super.processSchema();
    }

    @Override
    protected void processCallerPrep() throws IOException {
      
        
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        
        from.appendGUID( bodyTarget.append("private final int[] FROM_GUID = ")).append(";\n");
        bodyTarget.append("private final long BUILD_TIME = ");
        Appendables.appendValue(bodyTarget, System.currentTimeMillis()).append("L;\n");
        bodyTarget.append("private static final int ").append(doNothingConstant).append(" = ").append(doNothingConstantValue).append(";\n");
        
        
        bodyTarget.append("\n");
        bodyTarget.append("protected int nextMessageIdx() {\n");
        bodyTarget.append(tab).append("/* Override as needed and put your business specific logic here */\n");
        bodyTarget.append(tab).append("return ").append(doNothingConstant).append(";\n");
        bodyTarget.append("}\n");
        
        bodyTarget.append("\n");
        bodyTarget.append("@Override\n");
        bodyTarget.append("public void run() {\n");

        //      if (!Pipe.hasRoomForWrite(input)) {
        //      return;
        //  }
        appendStaticCall(bodyTarget.append(tab).append("if (!"), pipeClass, "hasRoomForWrite").append(pipeVarName).append(")) {\n");
        bodyTarget.append(tab).append(tab).append("return;\n");
        bodyTarget.append(tab).append("}\n");

        ///
        ///
        
        bodyTarget.append(tab).append("int ").append(cursorVarName).append(";\n");
        bodyTarget.append("\n");
        
        if (hasSimpleMessagesOnly) {
            
            appendStaticCall(bodyTarget.append("if ("), pipeClass,"hasContentToRead").append(pipeVarName).append(")) {\n");            
            appendStaticCall(bodyTarget.append(cursorVarName).append(" = "), pipeClass, "takeMsgIdx").append(pipeVarName).append(");\n");
            
            
        } else {
            //if (LowLevelStateManager.isStartNewMessage(navState)) {
            bodyTarget.append(tab).append("if (").append(stageMgrClassName).append(".isStartNewMessage(").append(stageMgrVarName).append(")) {\n");
            //    cursor = Pipe.takeMsgIdx(input);            
            //appendStaticCall(bodyTarget.append(tab).append(tab).append(cursorVarName).append(" = "), pipeClass, "takeMsgIdx").append(pipeVarName).append(");\n");
            
            
            bodyTarget.append(tab).append(tab).append(cursorVarName).append(" = nextMessageIdx();\n");

            //} else {
            bodyTarget.append(tab).append("} else {\n");
            //    cursor = LowLevelStateManager.activeCursor(navState);
            bodyTarget.append(tab).append(tab).append(cursorVarName).append(" = ").append(stageMgrClassName).append(".activeCursor(").append(stageMgrVarName).append(");\n");
            //}
            bodyTarget.append(tab).append("}\n");
            
        }
        bodyTarget.append("\n");
        
        
        //switch(cursor)) {
        bodyTarget.append(tab).append("switch(").append(cursorVarName).append(") {\n");
        
        
        
        
    }

    
    //TODO: Solution to mutation of method names,
    //      Add annotation to every method we wish to use, have annotation record method name in lookup table with immutable constant id
    //      When we use the method names for code generation look them up from the immutable constant id.
    
    @Override
    protected void processCaller(int cursor) throws IOException {
        
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        
        bodyTarget.append(tab).append(tab).append("case ");
        
        appendCaseMsgIdConstant(cursor, schema);
        
        bodyTarget.append(":\n");
        bodyTarget.append(tab).append(tab).append(tab);
        appendBusinessMethodName(cursor).append("();\n");
                                       
                                       
        //Pipe.confirmLowLevelRead(input, 8);
        int fragmentSizeLiteral = from.fragDataSize[cursor];
        
        appendStaticCall(bodyTarget.append(tab).append(tab).append(tab), pipeClass, "confirmLowLevelWrite").append(pipeVarName).append(", ").append(Integer.toString(fragmentSizeLiteral)).append(" /* fragment size */);\n");
                                      
        bodyTarget.append(tab).append(tab).append("break;\n");        

    }
    
    private void appendCaseMsgIdConstant(int cursor, MessageSchema schema) throws IOException {
        FieldReferenceOffsetManager from = MessageSchema.from(schema);

       bodyTarget.append("/*");
       appendInternalWriteMethodName(bodyTarget, cursor);
       bodyTarget.append("*/");
       
        if (schema instanceof MessageSchemaDynamic || null==from.fieldNameScript[cursor]) {
            bodyTarget.append(Integer.toString(cursor));
        } else {
            bodyTarget.append(schema.getClass().getSimpleName()).append(".");
            bodyTarget.append(FieldReferenceOffsetManager.buildMsgConstName(from, cursor));
        }
        
    }

    @Override
    protected void processCallerPost() throws IOException {

        bodyTarget.append(tab).append(tab).append("case ").append(doNothingConstant).append(":\n");
        //TODO; consume message can call request shutdown.
        
        bodyTarget.append(tab).append(tab).append(tab).append("return;\n");
        
        bodyTarget.append(tab).append(tab).append("default:\n");
        bodyTarget.append(tab).append(tab).append(tab).append("throw new UnsupportedOperationException(\"Unknown message type, rebuid with the new schema.\");\n");
                
        bodyTarget.append(tab).append("}\n"); //close of the switch statement
               
        //Pipe.releaseReads{input);
        appendStaticCall(bodyTarget.append(tab), pipeClass, "publishWrites").append(pipeVarName).append(");\n");
                  
        bodyTarget.append("}\n");
        bodyTarget.append("\n"); 
        
    }
    

    @Override
    protected void processFragmentClose(int fragmentCursor) throws IOException {
        
        if (!hasSimpleMessagesOnly) {
          //  businessMethodCall();
            //LowLevelStateManager.closeFragment(navState);
            bodyTarget.append(tab).append(stageMgrClassName).append(".closeFragment(").append(stageMgrVarName).append(");\n");
        }   
    }

    @Override
    protected void processDictionary() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    protected void processByteArrayOptional(String name, int idx, int fieldCursor, long id) throws IOException {
        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(name, "null");

        //build arg list for method signature
        appendTypeSignatureForPipeWriter(name, "ByteBuffer"); 
        
        //build the line to add value into the the pipe, this will not be modified.
        appendWriteToPipe(name, "addByteBuffer");
        
        firstField = false;
    }

    @Override
    protected void processByteArray(String name, int idx, int fieldCursor, long id) throws IOException {
        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(name, "ByteBuffer.allocate(0)");

        //build arg list for method signature
        appendTypeSignatureForPipeWriter(name, "ByteBuffer"); 
        
        //build the line to add value into the the pipe, this will not be modified.
        appendWriteToPipe(name, "addByteBuffer");
        
        firstField = false;
   
    }

    @Override
    protected void processTextUTF8Optional(String name, int idx, int fieldCursor, long id) throws IOException {
        
        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(name, "\"\"");
        
        //build arg list for method signature
        appendTypeSignatureForPipeWriter(name, "CharSequence"); 
        
        //build the line to add value into the the pipe, this will not be modified.
        appendWriteToPipe(name, "addUTF8");
        
        firstField = false;
        
    }

    @Override
    protected void processTextUTF8(String name, int idx, int fieldCursor, long id) throws IOException {
        
        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(name, "\"\"");
        
        //build arg list for method signature
        appendTypeSignatureForPipeWriter(name, "CharSequence"); 
        
        //build the line to add value into the the pipe, this will not be modified.
        appendWriteToPipe(name, "addUTF8");
        
        firstField = false;
    }

    @Override
    protected void processTextASCIIOptional(String name, int idx, int fieldCursor, long id) throws IOException {
        
        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(name, "\"\"");
        
        //build arg list for method signature
        appendTypeSignatureForPipeWriter(name, "CharSequence"); 
        
        //build the line to add value into the the pipe, this will not be modified.
        appendWriteToPipe(name, "addASCII");
        
        firstField = false;
        
    }

    @Override
    protected void processTextASCII(String name, int idx, int fieldCursor, long id) throws IOException {
        
        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(name, "\"\"");
        
        //build arg list for method signature
        appendTypeSignatureForPipeWriter(name, "CharSequence"); 
        
        //build the line to add value into the the pipe, this will not be modified.
        appendWriteToPipe(name, "addASCII");
        
        firstField = false;

    }


    @Override
    protected void processDecimalOptional(String name, int idx, int fieldCursor, long id) throws IOException {
        int nullLiteral = FieldReferenceOffsetManager.getAbsent32Value(MessageSchema.from(schema));
       
        String e = name+"E";
        String m = name+"M";
               
        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(e, "0");
        appendArgumentForBusinessCall(m, "0");
        
        //build arg list for method signature
        appendTypeSignatureForPipeWriter(e, "int");
        appendTypeSignatureForPipeWriter(m, "long");   
        
        //build the line to add value into the the pipe, this will not be modified.
        appendWirteOptionalToPipe(e, nullLiteral, "addIntValue");
        appendWriteToPipe(m, "addLongValue");
        
        firstField = false;

    }

    @Override
    protected void processDecimal(String name, int idx, int fieldCursor, long id) throws IOException {

        String e = name+"E";
        String m = name+"M";
        
        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(e, "0");
        appendArgumentForBusinessCall(m, "0");
        
        //build arg list for method signature
        appendTypeSignatureForPipeWriter(e, "int");
        appendTypeSignatureForPipeWriter(m, "long");   
        
        //build the line to add value into the the pipe, this will not be modified.
        appendWriteToPipe(e, "addIntValue"); 
        appendWriteToPipe(m, "addLongValue");
        
        firstField = false;

    }

    @Override
    protected void processLongUnsignedOptional(String name, int idx, int fieldCursor, long id) throws IOException {
        long nullLiteral = FieldReferenceOffsetManager.getAbsent64Value(MessageSchema.from(schema));

        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(name, "null");
        
        //build arg list for method signature
        appendTypeSignatureForPipeWriter(name, "Long");
        
        //    Pipe.addIntValue(null==source? nullLiteral : source, output);
        //build the line to add value into the the pipe, this will not be modified.
        appendWirteOptionalToPipe(name, nullLiteral, "addLongValue");
        
        firstField = false;
    }

    @Override
    protected void processLongSignedOptional(String name, int idx, int fieldCursor, long id) throws IOException {
        long nullLiteral = FieldReferenceOffsetManager.getAbsent64Value(MessageSchema.from(schema));

        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(name, "null");
        
        //build arg list for method signature
        appendTypeSignatureForPipeWriter(name, "Long");
        
        //    Pipe.addIntValue(null==source? nullLiteral : source, output);
        //build the line to add value into the the pipe, this will not be modified.
        appendWirteOptionalToPipe(name, nullLiteral, "addLongValue");
        
        firstField = false;
    }

    @Override
    protected void processLongUnsigned(String name, int idx, int fieldCursor, long id) throws IOException {

        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(name, "0");
        
        //build arg list for method signature
        appendTypeSignatureForPipeWriter(name, "long");
        
        //build the line to add value into the the pipe, this will not be modified.
        appendWriteToPipe(name, "addLongValue");
        
        firstField = false;
    }

    @Override
    protected void processLongSigned(String name, int idx, int fieldCursor, long id) throws IOException {

        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(name, "0");
        
        //build arg list for method signature
        appendTypeSignatureForPipeWriter(name, "long");   
        
        //build the line to add value into the the pipe, this will not be modified.
        appendWriteToPipe(name, "addLongValue");
        
        firstField = false;
   
    }

    @Override
    protected void processIntegerUnsignedOptional(String name, int i, int fieldCursor, long id) throws IOException {
        int nullLiteral = FieldReferenceOffsetManager.getAbsent32Value(MessageSchema.from(schema));

        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(name, "null");
        
        //build arg list for method signature
        appendTypeSignatureForPipeWriter(name, "Integer"); 
        
        //    Pipe.addIntValue(null==source? nullLiteral : source, output);
        //build the line to add value into the the pipe, this will not be modified.
        appendWirteOptionalToPipe(name, nullLiteral, "addIntValue");
        
        firstField = false;
    }
   
    @Override
    protected void processIntegerSignedOptional(String name, int i, int fieldCursor, long id) throws IOException {
        
        int nullLiteral = FieldReferenceOffsetManager.getAbsent32Value(MessageSchema.from(schema));

        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(name, "null");
        
        //build arg list for method signature
        appendTypeSignatureForPipeWriter(name, "Integer");  
        
        //    Pipe.addIntValue(null==source? nullLiteral : source, output);
        //build the line to add value into the the pipe, this will not be modified.
        appendWirteOptionalToPipe(name, nullLiteral, "addIntValue");
        
        firstField = false;
    }

    @Override
    protected void processIntegerUnsigned(String name, int i, int fieldCursor, long id) throws IOException {
        
        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(name, "0");
        
        //build arg list for method signature
        appendTypeSignatureForPipeWriter(name, "int");
        
        //build the line to add value into the the pipe, this will not be modified.
        appendWriteToPipe(name, "addIntValue"); 
        
        firstField = false;
    }
    
    @Override
    protected void pronghornIntegerSigned(String name, int i, int fieldCursor, long id) throws IOException {
        
        //build the argument for calling, this will be modified for specific business logic.
        appendArgumentForBusinessCall(name, "0");
        
        //build arg list for method signature
        appendTypeSignatureForPipeWriter(name, "int");        
        
        //build the line to add value into the the pipe, this will not be modified.
        appendWriteToPipe(name, "addIntValue");
        
        firstField = false;
    }


    private void appendArgumentForBusinessCall(String name, String defaultValue) throws IOException {
        appendVar(appendComma(businessExampleWorkspace.append(tab).append(tab).append(tab)).append(defaultValue).append(" /*"),name).append("*/\n");
    }

    private void appendWriteToPipe(String name, String method) throws IOException {
        appendVar(appendStaticCall(writeToPipeBodyWorkspace.append(tab), pipeClass, method), name).append(',').append(pipeVarName).append(");\n");
    }

    private void appendTypeSignatureForPipeWriter(String name, String type) throws IOException {
        appendVar(appendComma(writeToPipeSignatureWorkspace).append(type).append(' '),name);
    }

    private void appendWirteOptionalToPipe(String name, long nullLiteral, String methodName) throws IOException {
        appendVar(               
                appendValue( 
                        appendVar(
                                appendStaticCall(writeToPipeBodyWorkspace, pipeClass, methodName).append("null=="),name).append("?"), nullLiteral).append("L:"), name).
        append(',').
        append(pipeVarName).
        append(");\n");
    }

    private Appendable appendComma(Appendable target) throws IOException {
        if (!firstField) {
            target.append(',');            
        } else {
            target.append(' ');
        }
        return target;
    }
        
    private <A extends Appendable> A appendVar(A target, String name) throws IOException {
        return (A)target.append('p').append(name.replace(' ', '_')); //TODO: this replacement code should be doen in Appendable.
    }
    
    private Appendable appendInternalWriteMethodName(Appendable target, int cursor) throws IOException {
        return appendFragmentName(target.append("processPipe").append(pipeId).append("Write"), cursor);
    }
    
    private Appendable appendBusinessMethodName(int cursor) throws IOException {
        return appendFragmentName(bodyTarget.append("process"), cursor);
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

    @Override //group length, last field in fragment and marks start of new sequence
    protected void processSequenceOpen(int fragmentCursor, String name, int idx, int fieldCursor, long id) throws IOException {
        if (hasSimpleMessagesOnly) {
            
            throw new UnsupportedOperationException();
            
        } else {
               CharSequence varName = appendVar(new StringBuilder(), name);  

               appendSequenceCounterVar(appendComma(businessExampleWorkspace.append(tab).append(tab).append(tab)).append("0").append(" /*"),varName).append("*/\n");
               
               appendSequenceCounterVar(appendComma(writeToPipeSignatureWorkspace).append("int").append(' '),varName);            

                if (0!=id) {
                    writeToPipeBodyWorkspace.append("/* id:").append(Long.toString(id));
                    writeToPipeBodyWorkspace.append("  */");
                }
                writeToPipeBodyWorkspace.append("\n");
                                
                writeToPipeBodyWorkspace.append(tab).append(stageMgrClassName).append(".processGroupLength(").append(stageMgrVarName).append(", ").append(Integer.toString(fragmentCursor)).append(", ");
                appendSequenceCounterVar(writeToPipeBodyWorkspace, varName).append(");\n");

            firstField = false;

            
        }
    }


    private Appendable appendSequenceCounterVar(Appendable target, CharSequence varName) throws IOException {
        return target.append(varName).append(SeqCountSuffix);
    }
    
    @Override //set cursor after end of sequence
    protected void postProcessSequence(int fieldCursor) throws IOException {
        if (hasSimpleMessagesOnly) {            
            throw new UnsupportedOperationException();            
        } else {        
            writeToPipeBodyWorkspace.append(tab).append(stageMgrClassName).append(".continueAtThisCursor(").append(stageMgrVarName).append(", ");
            writeToPipeBodyWorkspace.append("/*");
            appendInternalWriteMethodName(writeToPipeBodyWorkspace, fieldCursor);
            writeToPipeBodyWorkspace.append("*/");
            writeToPipeBodyWorkspace.append(Integer.toString(fieldCursor)).append(");\n");
        }
    }

    @Override 
    protected void processMessageClose(String name, long id, boolean needsToCloseFragment) throws IOException {
        if (!hasSimpleMessagesOnly && needsToCloseFragment) {    
            //businessMethodCall(); //DO we need this hook?
            writeToPipeBodyWorkspace.append(tab).append(stageMgrClassName).append(".closeFragment(").append(stageMgrVarName).append(");\n");  
        }
    }

    @Override //count down for end of sequence
    protected boolean processSequenceInstanceClose(String name, long id, int fieldCursor) throws IOException {

        if (hasSimpleMessagesOnly) {            
            throw new UnsupportedOperationException();            
        } else { 
            
         //   businessMethodCall(); //DO we need this hook?
            
            writeToPipeBodyWorkspace.append(tab).append("if (!").append(stageMgrClassName).append(".closeSequenceIteration(").append(stageMgrVarName).append(")) {\n");
            writeToPipeBodyWorkspace.append(tab).append(tab).append("return; /* Repeat this fragment*/\n");
            writeToPipeBodyWorkspace.append(tab).append("}\n");
            
         //   processEndOfSequence(name,id); ///DO we need this hook?
                
            writeToPipeBodyWorkspace.append(tab).append(stageMgrClassName).append(".closeFragment(").append(stageMgrVarName).append(");\n");
            
        }
        
        //this is fixed because we are doing code generation
        return false;
        
        
    }

    @Override //fragment group open, not called for message open
    protected void processFragmentOpen(String name, int fieldCursor, long id) throws IOException {
    }

    @Override //This is the beginning of a new fragment
    protected void processCalleeOpen(int cursor) throws IOException {
                
        firstField = true;
        writeToPipeSignatureWorkspace.setLength(0);
        writeToPipeBodyWorkspace.setLength(0);
        businessExampleWorkspace.setLength(0);
        
    }
    
    @Override //this is the end of a fragment
    protected void processCalleeClose(int cursor) throws IOException {
        
        
        bodyTarget.append("protected void ");
        appendBusinessMethodName(cursor).append("() {\n");
        
        bodyTarget.append('\n');
        bodyTarget.append(tab).append("/* Override as needed and put your business specific logic here */\n");
        bodyTarget.append('\n');
        
        bodyTarget.append(tab);
        appendInternalWriteMethodName(bodyTarget, cursor).append("(\n");        
        bodyTarget.append(businessExampleWorkspace).append(tab).append(");\n");
        
        bodyTarget.append("}\n");
        bodyTarget.append('\n');
        
        
        bodyTarget.append("protected void ");
        appendInternalWriteMethodName(bodyTarget, cursor).append("(").append(writeToPipeSignatureWorkspace).append(") {\n");
        bodyTarget.append(writeToPipeBodyWorkspace);
        bodyTarget.append("}\n");
        bodyTarget.append('\n');
        
    }
    
    
}
