package com.ociweb.jfast.generator;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;
import com.ociweb.pronghorn.ring.RingBuffers;

public class DispatchLoader {

    private static final boolean FORCE_COMPILE = true;
    private static final Logger log = LoggerFactory.getLogger(DispatchLoader.class);

    public static FASTDecoder loadDispatchReader(byte[] catalog, RingBuffers ringBuffers) {
        //always try to load the generated reader because it will be faster 
        try {
            return loadGeneratedReaderDispatch(catalog, FASTClassLoader.READER, ringBuffers);
        } catch (Exception e) {
        	log.error("Attempted to load dispatch reader.", e);
            return new FASTReaderInterpreterDispatch(catalog, ringBuffers);
        }
    }
    
    public static FASTDecoder loadDispatchReaderDebug(byte[] catalog, RingBuffers ringBuffers) {
        ///NOTE: when there is only 1 template the compiled dispatch may continue to work if the templateId is wrong
        //       because it does not do extra checking.  The interpreted one will use the templateId as defined.
        return new FASTReaderInterpreterDispatch(catalog, ringBuffers);
    }

    public static FASTEncoder loadDispatchWriter(byte[] catalog) {
        //always try to load the generated reader because it will be faster 
        try {
            return loadGeneratedWriterDispatch(catalog, FASTClassLoader.WRITER);
        } catch (Exception e) {
        	log.error("Attempted to load dispatch reader.", e);
            return loadDispatchWriterDebug(catalog);
        }
    }
    
    public static FASTEncoder loadDispatchWriterDebug(byte[] catBytes) {
		return new FASTWriterInterpreterDispatch(new TemplateCatalogConfig(catBytes));
    }
    
    public static <T> T loadGeneratedReaderDispatch(byte[] catBytes, String type, RingBuffers ringBuffers)
            throws ReflectiveOperationException, SecurityException {
        
        ClassLoader parentClassLoader = FASTDecoder.class.getClassLoader();
        
        try {
            Class generatedClass = new FASTClassLoader(catBytes, parentClassLoader).loadClass(type);
            
            int[] catHash = (int[])generatedClass.getField("hashedCat").get(null);
            int[] expectedHash = GeneratorData.hashCatBytes(catBytes);
            
            if (!Arrays.equals(catHash, expectedHash)) {
            	log.trace("Catalog mistmatch, attempting source regeneration and recompile.");
                //the templates catalog this was generated for does not match the current value so force a recompile
                generatedClass = new FASTClassLoader(catBytes, parentClassLoader, FORCE_COMPILE).loadClass(type);
            }
                       
            
            return (T)generatedClass.getConstructor(catBytes.getClass(), ringBuffers.getClass()).newInstance(catBytes, ringBuffers);
        } catch (Throwable t) {
        	log.error("Error in creating instance, attempting source regeneration and recompile.", t);
            //can not create instance because the class is no longer compatible with the rest of the code base so force a recompile
            Class generatedClass = new FASTClassLoader(catBytes, parentClassLoader, FORCE_COMPILE).loadClass(type);
            return (T)generatedClass.getConstructor(catBytes.getClass(), ringBuffers.getClass()).newInstance(catBytes, ringBuffers);
        }
    }
    
    public static <T> T loadGeneratedWriterDispatch(byte[] catBytes, String type)
            throws ReflectiveOperationException, SecurityException {
        
        ClassLoader parentClassLoader = FASTEncoder.class.getClassLoader();
        
        try {
            Class generatedClass = new FASTClassLoader(catBytes, parentClassLoader).loadClass(type);
            
            int[] catHash = (int[])generatedClass.getField("hashedCat").get(null);
            int[] expectedHash = GeneratorData.hashCatBytes(catBytes);
            
            if (!Arrays.equals(catHash, expectedHash)) {
            	log.trace("Catalog mistmatch, attempting source regeneration and recompile.");
                //the templates catalog this was generated for does not match the current value so force a recompile
                generatedClass = new FASTClassLoader(catBytes, parentClassLoader, FORCE_COMPILE).loadClass(type);
            }
                       
            
            return (T)generatedClass.getConstructor(catBytes.getClass()).newInstance(catBytes);
        } catch (Throwable t) {
        	log.trace("Error in creating instance, attempting source regeneration and recompile.", t);
            //can not create instance because the class is no longer compatible with the rest of the code base so force a recompile
            Class generatedClass = new FASTClassLoader(catBytes, parentClassLoader, FORCE_COMPILE).loadClass(type);
            return (T)generatedClass.getConstructor(catBytes.getClass()).newInstance(catBytes);
        }
    }


}
