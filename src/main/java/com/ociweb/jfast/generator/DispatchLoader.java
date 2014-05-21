package com.ociweb.jfast.generator;

import java.util.Arrays;

import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;

public class DispatchLoader {

    private static final boolean FORCE_COMPILE = true;

    public static FASTDecoder loadDispatchReader(byte[] catalog) {
        //always try to load the generated reader because it will be faster 
        try {
            return loadGeneratedDispatchReader(catalog);
        } catch (Exception e) {
            Supervisor.err("Attempted to load dispatch reader.", e);
            return new FASTReaderInterpreterDispatch(catalog);
        }
    }

    public static FASTDecoder loadGeneratedDispatchReader(byte[] catBytes)
            throws ReflectiveOperationException, SecurityException {
        
        ClassLoader parentClassLoader = FASTDecoder.class.getClassLoader();
        
        try {
            Class generatedClass = new FASTClassLoader(catBytes, parentClassLoader).loadClass(FASTClassLoader.READER);
            
            byte[] catBytesFromClass = (byte[])generatedClass.getField("catBytes").get(null);
            if (!Arrays.equals(catBytesFromClass, catBytes)) {
                Supervisor.log("Catalog mistmatch, attempting source regeneration and recompile.");
                //the templates catalog this was generated for does not match the current value so force a recompile
                generatedClass = new FASTClassLoader(catBytes, parentClassLoader, FORCE_COMPILE).loadClass(FASTClassLoader.READER);
            }
                       
            return (FASTDecoder)generatedClass.newInstance();
        } catch (Throwable t) {
            Supervisor.err("Error in creating instance, attempting source regeneration and recompile.", t);
            //can not create instance because the class is no longer compatible with the rest of the code base so force a recompile
            Class generatedClass = new FASTClassLoader(catBytes, parentClassLoader, FORCE_COMPILE).loadClass(FASTClassLoader.READER);
            return (FASTDecoder)generatedClass.newInstance();
        }
    }


}
