package com.ociweb.jfast;

import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTInputReactor;

public class FAST {
	
	static int DEFAULT_BUFFER_SIZE = 4096;

	public static FASTInputReactor inputReactor(FASTInput fastInput, byte[] catBytes) {
	    FASTDecoder readerDispatch = DispatchLoader.loadDispatchReader(catBytes); 
	    PrimitiveReader reader = new PrimitiveReader(DEFAULT_BUFFER_SIZE, fastInput, readerDispatch.maxPMapCountInBytes);
	    FASTInputReactor reactor = new FASTInputReactor(readerDispatch,reader);
		return reactor;
	}

}
