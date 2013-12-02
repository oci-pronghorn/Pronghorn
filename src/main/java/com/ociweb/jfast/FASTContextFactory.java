package com.ociweb.jfast;

import java.io.File;

import com.ociweb.jfast.context.FASTContext;
import com.ociweb.jfast.context.FASTContextDefault;
import com.ociweb.jfast.context.FASTContextTemplated;
import com.ociweb.jfast.stream.DictionaryFactory;

/**
 * Creates new contexts.  It is recommended that the contexts get reused
 * when possible and appropriate.  This factory will never return the same
 * context.
 * 
 * @author NathanTippy
 *
 */
public class FASTContextFactory {

	
	public static FASTContext newTemplatedContext(File localStorage) {
		return new FASTContextTemplated(localStorage);
	}
	
	public static FASTContext newContext(DictionaryFactory dcr) {
		return new FASTContextDefault(dcr);
	}
	
}
