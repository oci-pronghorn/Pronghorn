package com.ociweb.pronghorn.pipe.util.build;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;

public abstract class TemplateProcessGenerator {

    
    private static final Logger log = LoggerFactory.getLogger(TemplateProcessGenerator.class);
    
    protected final MessageSchema schema;
    
    public TemplateProcessGenerator(MessageSchema schema) {
        this.schema = schema;
    }

    
    

    public void processSchema() throws IOException {
        
        headerConstruction();
                
        defineMembers();
               
        
        final FieldReferenceOffsetManager from = MessageSchema.from(schema);
        
        //Build top level entry point
        processCallerPrep();
        for(int cursor =0; cursor<from.fragScriptSize.length; cursor++) {
            boolean isFragmentStart = 0!=from.fragScriptSize[cursor];
            if (isFragmentStart) {
                processCaller(cursor);
            }
            
        }
        processCallerPost();
        
        //Build fragment consumption methods
        for(int cursor = 0; cursor<from.fragScriptSize.length; cursor++) {
            boolean isFragmentStart = 0!=from.fragScriptSize[cursor];
            
            if (isFragmentStart) {
                processCalleeOpen(cursor);                    
                
                boolean isMessageStart = FieldReferenceOffsetManager.isTemplateStart(from, cursor);
               
                if (isMessageStart) {
                    processFragment(1, cursor, from);
                } else {
                    processFragment(0, cursor, from);
                }
                
                processCalleeClose(cursor);
            }
        }
        
        footerConstruction();
        
    }
    



    private void processFragment(int startPos, final int fragmentCursor, final FieldReferenceOffsetManager from) throws IOException {
        
        final int fieldsInFragment = from.fragScriptSize[fragmentCursor];
        final String[] fieldNameScript = from.fieldNameScript;
        final long[] fieldIdScript = from.fieldIdScript;
        final int[] depth = from.fragDepth;
        
        int i = startPos;
        int idx = 0;
        while (i < fieldsInFragment) {
            int fieldCursor = fragmentCursor+i++;
            
            switch (TokenBuilder.extractType(from.tokens[fieldCursor])) {
                case TypeMask.Group:
                    if (FieldReferenceOffsetManager.isGroupOpen(from, fieldCursor)) {
                        processFragmentOpen(fieldNameScript[fieldCursor], fieldCursor, fieldIdScript[fieldCursor]);
                    } else {
                        
                        
                        //process group close
                        final int fieldLimit =fieldCursor+(fieldsInFragment-i); 
                        final int len = from.tokens.length;
                        
                        do {//close this member of the sequence or template
                            final String name = fieldNameScript[fieldCursor];
                            final long id = fieldIdScript[fieldCursor];
                            
                            //if this was a close of sequence count down so we now when to close it.
                            if (FieldReferenceOffsetManager.isGroupSequence(from, fieldCursor)) {
                                if (processSequenceInstanceClose(name, id, fieldCursor)) {   
                                    //jump out if we need to process yet another fragment
                                    return;
                                }
                                //else this is the end of the sequence and close the nested groups as needed.
                            } else {
                                //group close that is not a sequence.
                                if (fieldCursor<=fieldLimit) {
                                    processMessageClose(name, id, depth[fieldCursor]>0); 
                                }
                            }
                        } while (++fieldCursor<len && FieldReferenceOffsetManager.isGroupClosed(from, fieldCursor) );
                                                
                        //if the stack is empty set the continuation for fields that appear after the sequence
                        if (fieldCursor<len && !FieldReferenceOffsetManager.isGroup(from, fieldCursor)) {
                            postProcessSequence(fieldCursor);
                        }
                        return;//this is always the end of a fragment
                    }                   
                    break;
                case TypeMask.GroupLength:      
                    assert(i==fieldsInFragment) :" this should be the last field";
   
                    processSequenceOpen(fragmentCursor, fieldNameScript[fieldCursor+1], idx, fieldCursor, fieldIdScript[fieldCursor+1]);                                    
                    idx++;
                    return;                     
                case TypeMask.IntegerSigned:
                    pronghornIntegerSigned(fieldNameScript[fieldCursor], idx++, fieldCursor, fieldIdScript[fieldCursor]);
                    break;
                case TypeMask.IntegerUnsigned: //Java does not support unsigned int so we pass it as a long being careful not to get it signed.
                    processIntegerUnsigned(fieldNameScript[fieldCursor], idx++, fieldCursor, fieldIdScript[fieldCursor]);
                    break;
                case TypeMask.IntegerSignedOptional:
                    processIntegerSignedOptional(fieldNameScript[fieldCursor], idx++, fieldCursor, fieldIdScript[fieldCursor]);
                    break;
                case TypeMask.IntegerUnsignedOptional:
                    processIntegerUnsignedOptional(fieldNameScript[fieldCursor], idx++, fieldCursor, fieldIdScript[fieldCursor]);
                    break;
                case TypeMask.LongSigned:
                    processLongSigned(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);  
                    idx+=2;
                    break;  
                case TypeMask.LongUnsigned:
                    processLongUnsigned(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);    
                    idx+=2;
                    break;  
                case TypeMask.LongSignedOptional:
                    processLongSignedOptional(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);  
                    idx+=2;
                    break;      
                case TypeMask.LongUnsignedOptional:
                    processLongUnsignedOptional(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);    
                    idx+=2;
                    break;
                case TypeMask.Decimal:
                    processDecimal(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
                    idx+=3;
                    i++;//add 1 extra because decimal takes up 2 slots in the script
                    break;  
                case TypeMask.DecimalOptional:
                    processDecimalOptional(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
                    idx+=3;
                    i++;//add 1 extra because decimal takes up 2 slots in the script
                    break;  
                case TypeMask.TextASCII:
                    processTextASCII(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
                    idx+=2;
                    break;
                case TypeMask.TextASCIIOptional:
                    processTextASCIIOptional(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
                    idx+=2;
                    break;
                case TypeMask.TextUTF8:
                    processTextUTF8(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
                    idx+=2;
                    break;                      
                case TypeMask.TextUTF8Optional:
                    processTextUTF8Optional(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
                    idx+=2;
                    break;
                case TypeMask.ByteVector:
                    processByteArray(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
                    idx+=2;
                    break;  
                case TypeMask.ByteVectorOptional:
                    processByteArrayOptional(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
                    idx+=2;
                    break;
                case TypeMask.Dictionary:
                    processDictionary();
                    idx++;
                    break;  
                default: 
                    log.error("unknown token type:"+TokenBuilder.tokenToString(from.tokens[fieldCursor]));
            }
        }
        
        //we are here because it did not exit early with close group or group length therefore this
        //fragment is one of those that is not wrapped by a group open/close and we should do the close logic.
        //LowLevelStateManager.closeFragment(navState); 
        processFragmentClose(fragmentCursor);
        
    }

        


    protected abstract void headerConstruction() throws IOException;
    protected abstract void defineMembers() throws IOException;
    protected abstract void footerConstruction() throws IOException;
    
    protected abstract void processCallerPrep() throws IOException;
    protected abstract void processCaller(int cursor) throws IOException;
    protected abstract void processCallerPost() throws IOException;
    
    protected abstract void processFragmentClose(int fragmentCursor) throws IOException;
    protected abstract void processDictionary() throws IOException;
    protected abstract void processByteArrayOptional(String name, int idx, int fieldCursor, long id) throws IOException;
    protected abstract void processByteArray(String name, int idx, int fieldCursor, long id) throws IOException;
    protected abstract void processTextUTF8Optional(String name, int idx, int fieldCursor, long id) throws IOException;
    protected abstract void processTextUTF8(String name, int idx, int fieldCursor, long id) throws IOException;
    protected abstract void processTextASCIIOptional(String name, int idx, int fieldCursor, long id) throws IOException;
    protected abstract void processTextASCII(String name, int idx, int fieldCursor, long id) throws IOException;
    protected abstract void processDecimalOptional(String name, int idx, int fieldCursor, long id) throws IOException;
    protected abstract void processDecimal(String name, int idx, int fieldCursor, long id) throws IOException;
    protected abstract void processLongUnsignedOptional(String name, int idx, int fieldCursor, long id) throws IOException;
    protected abstract void processLongSignedOptional(String name, int idx, int fieldCursor, long id) throws IOException;
    protected abstract void processLongUnsigned(String name, int idx, int fieldCursor, long id) throws IOException;
    protected abstract void processLongSigned(String name, int idx, int fieldCursor, long id) throws IOException;
    protected abstract void processIntegerUnsignedOptional(String name, int i, int fieldCursor, long id) throws IOException;
    protected abstract void processIntegerSignedOptional(String name, int i, int fieldCursor, long id) throws IOException;
    protected abstract void processIntegerUnsigned(String name, int i, int fieldCursor, long id) throws IOException;
    protected abstract void pronghornIntegerSigned(String name, int i, int fieldCursor, long id) throws IOException;
    protected abstract void processSequenceOpen(int fragmentCursor, String string, int idx, int fieldCursor, long id) throws IOException;
    protected abstract void postProcessSequence(int fieldCursor) throws IOException;
    protected abstract void processMessageClose(String name, long id, boolean needsToCloseFragment) throws IOException;
    protected abstract boolean processSequenceInstanceClose(String name, long id, int fieldCursor) throws IOException;
    protected abstract void processFragmentOpen(String name, int fieldCursor, long id) throws IOException;
    
    protected abstract void processCalleeOpen(int cursor) throws IOException;
    protected abstract void processCalleeClose(int cursor) throws IOException;
    
}
