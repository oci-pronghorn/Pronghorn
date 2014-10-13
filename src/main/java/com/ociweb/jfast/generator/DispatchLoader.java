package com.ociweb.jfast.generator;

import java.util.Arrays;

import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;

public class DispatchLoader {

    private static final boolean FORCE_COMPILE = true;

    public static FASTDecoder loadDispatchReader(byte[] catalog) {
        //always try to load the generated reader because it will be faster 
        try {
            return loadGeneratedDispatch(catalog, FASTClassLoader.READER);
        } catch (Exception e) {
            Supervisor.err("Attempted to load dispatch reader.", e);
            return new FASTReaderInterpreterDispatch(catalog);
        }
    }
    
    public static FASTDecoder loadDispatchReaderDebug(byte[] catalog) {
        return new FASTReaderInterpreterDispatch(catalog);
    }

    public static FASTEncoder loadDispatchWriter(byte[] catalog) {
        //always try to load the generated reader because it will be faster 
        try {
            return loadGeneratedDispatch(catalog, FASTClassLoader.WRITER);
        } catch (Exception e) {
            Supervisor.err("Attempted to load dispatch reader.", e);
            return new FASTWriterInterpreterDispatch(catalog);
        }
    }
    
    public static <T> T loadGeneratedDispatch(byte[] catBytes, String type)
            throws ReflectiveOperationException, SecurityException {
        
        ClassLoader parentClassLoader = FASTDecoder.class.getClassLoader();
        
        try {
            Class generatedClass = new FASTClassLoader(catBytes, parentClassLoader).loadClass(type);
            
            byte[] catBytesFromClass = (byte[])generatedClass.getField("catBytes").get(null);
            if (!Arrays.equals(catBytesFromClass, catBytes)) {
                Supervisor.log("Catalog mistmatch, attempting source regeneration and recompile.");
                //the templates catalog this was generated for does not match the current value so force a recompile
                generatedClass = new FASTClassLoader(catBytes, parentClassLoader, FORCE_COMPILE).loadClass(type);
            }
                       
            return (T)generatedClass.newInstance();
        } catch (Throwable t) {
            Supervisor.err("Error in creating instance, attempting source regeneration and recompile.", t);
            //can not create instance because the class is no longer compatible with the rest of the code base so force a recompile
            Class generatedClass = new FASTClassLoader(catBytes, parentClassLoader, FORCE_COMPILE).loadClass(type);
            return (T)generatedClass.newInstance();
        }
    }


}
