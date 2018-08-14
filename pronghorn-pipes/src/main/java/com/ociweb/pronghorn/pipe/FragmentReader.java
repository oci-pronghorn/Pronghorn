package com.ociweb.pronghorn.pipe;

public class FragmentReader {

	
    public static <S extends MessageSchema<S>> int readI(Pipe<S> pipe) {

    	int msgIdx = Pipe.takeMsgIdx(pipe);
    	assert(3==Pipe.from(pipe).fragDataSize[msgIdx]) : "This constant does not this fragment size";
    	int value = Pipe.takeInt(pipe);
     	Pipe.confirmLowLevelRead(pipe, 3);
     	Pipe.releaseReadLock(pipe);
    	return value;
    	        
    }
    
    public static <S extends MessageSchema<S>> long readL(Pipe<S> pipe) {

    	int msgIdx = Pipe.takeMsgIdx(pipe);
    	assert(3==Pipe.from(pipe).fragDataSize[msgIdx]) : "This constant does not this fragment size";
    	long value = Pipe.takeLong(pipe);
     	Pipe.confirmLowLevelRead(pipe, 3);
     	Pipe.releaseReadLock(pipe);
    	return value;
    	        
    }
	
}
