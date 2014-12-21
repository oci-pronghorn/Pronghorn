package com.ociweb.jfast;

import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTReaderReactor;
import com.ociweb.pronghorn.ring.RingBuffers;

public class FAST {
	
	static int DEFAULT_BUFFER_SIZE = 4096;

	public static FASTReaderReactor inputReactor(FASTInput fastInput, byte[] catBytes, RingBuffers ringBuffers) {
	    FASTDecoder readerDispatch = DispatchLoader.loadDispatchReader(catBytes, ringBuffers); 
	    PrimitiveReader reader = new PrimitiveReader(DEFAULT_BUFFER_SIZE, fastInput, readerDispatch.maxPMapCountInBytes);
	    return new FASTReaderReactor(readerDispatch,reader);
	}
	
	public static FASTReaderReactor inputReactor(int bufferSize, FASTInput fastInput, byte[] catBytes, RingBuffers ringBuffers) {
	    FASTDecoder readerDispatch = DispatchLoader.loadDispatchReader(catBytes, ringBuffers); 
	    PrimitiveReader reader = new PrimitiveReader(bufferSize, fastInput, readerDispatch.maxPMapCountInBytes);
	    return new FASTReaderReactor(readerDispatch,reader);
	}
	
	public static FASTReaderReactor inputReactorDebug(FASTInput fastInput, byte[] catBytes, RingBuffers ringBuffers) {
	    FASTDecoder readerDispatch = DispatchLoader.loadDispatchReaderDebug(catBytes, ringBuffers); 
	    PrimitiveReader reader = new PrimitiveReader(DEFAULT_BUFFER_SIZE, fastInput, readerDispatch.maxPMapCountInBytes);
	    return new FASTReaderReactor(readerDispatch,reader);
	}
	
	public static FASTReaderReactor inputReactorDebug(int bufferSize, FASTInput fastInput, byte[] catBytes, RingBuffers ringBuffers) {
	    FASTDecoder readerDispatch = DispatchLoader.loadDispatchReaderDebug(catBytes, ringBuffers); 
	    PrimitiveReader reader = new PrimitiveReader(bufferSize, fastInput, readerDispatch.maxPMapCountInBytes);
	    return new FASTReaderReactor(readerDispatch,reader);
	}

}
