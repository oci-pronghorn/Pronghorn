package com.ociweb.jfast.context;

import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.stream.FASTReaderDispatch;
import com.ociweb.jfast.stream.FASTWriterDispatch;

/**
 * 
 * For use in situations where template files are not in use.
 * 
 *  * When the template is declaratively built (in non dynamic situations)
 *  * When testing
 * 
 * @author NathanTippy
 *
 */
public class FASTContextDefault implements FASTContext {

	private final FASTReaderDispatch staticReader;
	private final FASTWriterDispatch staticWriter;
	
	public FASTContextDefault(DictionaryFactory dcr) {
		staticReader = null;//new FASTStaticReader();
		staticWriter = null;
	}
	
	@Override
	public FASTReaderDispatch staticReader() {
		return staticReader;
	}

	@Override
	public FASTReaderDispatch staticWriter() {
		return null;//staticWriter;
	}

}
