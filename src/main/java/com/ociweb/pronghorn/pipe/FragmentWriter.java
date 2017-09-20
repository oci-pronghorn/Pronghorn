package com.ociweb.pronghorn.pipe;

public class FragmentWriter {

    public static <S extends MessageSchema<S>> void write(Pipe<S> pipe, int msgIdx) {
    	assert(FieldReferenceOffsetManager.isValidMsgIdx(Pipe.from(pipe), msgIdx));
    	assert(2==Pipe.from(pipe).fragDataSize[msgIdx]) : "This constant does not this fragment size";
    	
    	int size = Pipe.addMsgIdx(pipe, msgIdx);
    	Pipe.confirmLowLevelWrite(pipe, size);
    	Pipe.publishWrites(pipe);        
    }
    
    public static <S extends MessageSchema<S>> void writeL(Pipe<S> pipe, int msgIdx, long field1) {
    	assert(FieldReferenceOffsetManager.isValidMsgIdx(Pipe.from(pipe), msgIdx));
    	assert(4==Pipe.from(pipe).fragDataSize[msgIdx]) : "This constant does not this fragment size";
    	
    	int size = Pipe.addMsgIdx(pipe, msgIdx);
    	Pipe.addLongValue(field1, pipe);
    	Pipe.confirmLowLevelWrite(pipe, size);
    	Pipe.publishWrites(pipe);        
    }
    
    public static <S extends MessageSchema<S>> void writeLV(Pipe<S> pipe, int msgIdx, long field1,
    		                                 byte[] field2Backing, int field2Position, int field2Length) {
    	assert(FieldReferenceOffsetManager.isValidMsgIdx(Pipe.from(pipe), msgIdx));
    	assert(4==Pipe.from(pipe).fragDataSize[msgIdx]) : "This constant does not this fragment size";
    	
    	int size = Pipe.addMsgIdx(pipe, msgIdx);
    	Pipe.addLongValue(field1, pipe);
    	Pipe.addByteArray(field2Backing, field2Position, field2Length, pipe);
    	Pipe.confirmLowLevelWrite(pipe, size);
    	Pipe.publishWrites(pipe);        
    }
    
    
    public static <S extends MessageSchema<S>> void writeLL(Pipe<S> pipe, int msgIdx, long field1, long field2) {
    	assert(FieldReferenceOffsetManager.isValidMsgIdx(Pipe.from(pipe), msgIdx));
    	assert(6==Pipe.from(pipe).fragDataSize[msgIdx]) : "This constant does not this fragment size";
    	
    	int size = Pipe.addMsgIdx(pipe, msgIdx);
    	Pipe.addLongValue(field1, pipe);
    	Pipe.addLongValue(field2, pipe);
    	Pipe.confirmLowLevelWrite(pipe, size);
    	Pipe.publishWrites(pipe);        
    }
    
    public static <S extends MessageSchema<S>> void writeLI(Pipe<S> pipe, int msgIdx, long field1, int field2) {
    	assert(FieldReferenceOffsetManager.isValidMsgIdx(Pipe.from(pipe), msgIdx));
    	assert(5==Pipe.from(pipe).fragDataSize[msgIdx]) : "This constant does not this fragment size";

    	int size = Pipe.addMsgIdx(pipe, msgIdx);
    	Pipe.addLongValue(field1, pipe);
    	Pipe.addIntValue(field2, pipe);
    	Pipe.confirmLowLevelWrite(pipe, size);
    	Pipe.publishWrites(pipe);        
    }
    
    public static <S extends MessageSchema<S>> void writeII(Pipe<S> pipe, int msgIdx, int field1, int field2) {
    	assert(FieldReferenceOffsetManager.isValidMsgIdx(Pipe.from(pipe), msgIdx));
    	assert(4==Pipe.from(pipe).fragDataSize[msgIdx]) : "This constant does not this fragment size";

    	int size = Pipe.addMsgIdx(pipe, msgIdx);
    	Pipe.addIntValue(field1, pipe);
    	Pipe.addIntValue(field2, pipe);
    	Pipe.confirmLowLevelWrite(pipe, size);
    	Pipe.publishWrites(pipe);        
    }
    
    public static <S extends MessageSchema<S>> void writeLII(Pipe<S> pipe, int msgIdx, long field1, int field2, int field3) {
    	assert(FieldReferenceOffsetManager.isValidMsgIdx(Pipe.from(pipe), msgIdx));
    	assert(6==Pipe.from(pipe).fragDataSize[msgIdx]) : "This constant does not this fragment size";
    	
    	int size = Pipe.addMsgIdx(pipe, msgIdx);
    	Pipe.addLongValue(field1, pipe);
    	Pipe.addIntValue(field2, pipe);
    	Pipe.addIntValue(field3, pipe);
    	Pipe.confirmLowLevelWrite(pipe, size);
    	Pipe.publishWrites(pipe);        
    }
    
    public static <S extends MessageSchema<S>> void writeI(Pipe<S> pipe, int msgIdx, int field1) {
    	assert(FieldReferenceOffsetManager.isValidMsgIdx(Pipe.from(pipe), msgIdx));
    	
    	int messageSize = Pipe.from(pipe).fragDataSize[msgIdx];
    	assert(3==Pipe.from(pipe).fragDataSize[msgIdx]) : "This constant does not this fragment size";
    	
    	int size = Pipe.addMsgIdx(pipe, msgIdx);
    	Pipe.addIntValue(field1, pipe);
    	Pipe.confirmLowLevelWrite(pipe, size);
    	Pipe.publishWrites(pipe);        
    }
}
