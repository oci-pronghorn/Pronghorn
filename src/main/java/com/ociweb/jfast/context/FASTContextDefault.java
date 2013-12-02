package com.ociweb.jfast.context;

import com.ociweb.jfast.stream.DictionaryFactory;
import com.ociweb.jfast.stream.FASTStaticReader;
import com.ociweb.jfast.stream.FASTStaticWriter;

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

	private final FASTStaticReader staticReader;
	private final FASTStaticWriter staticWriter;
	
	public FASTContextDefault(DictionaryFactory dcr) {
		staticReader = null;//new FASTStaticReader();
		staticWriter = null;
	}
	
	@Override
	public FASTStaticReader staticReader() {
		return staticReader;
	}

	@Override
	public FASTStaticReader staticWriter() {
		return null;//staticWriter;
	}

}
