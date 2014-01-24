package com.ociweb.jfast.context;

import com.ociweb.jfast.stream.FASTReaderDispatch;

/**
 * Primary class for interactions with this library.  The FASTContext holds all the 
 * supported templates and provides readers/writers to client code upon request.
 * 
 * @author NathanTippy
 *
 */
public interface FASTContext {

	
	FASTReaderDispatch staticReader();
	FASTReaderDispatch staticWriter();
	
	//FASTDynamicReader dynamicReader();
	
	
}
