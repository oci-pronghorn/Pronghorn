package com.ociweb.jfast.generator;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.FASTReaderDispatchBase;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;


    public class FASTDispatchClassLoader extends ClassLoader{

        final static String SIMPLE_READER_NAME = "FASTReaderDispatchGenExample";
        public static final String READER = "com.ociweb.jfast.stream."+SIMPLE_READER_NAME;
        
        public static final String WRITER = "generated.FASTWriterDispatch";
        
        
        final byte[] catalogBytes;

        public FASTDispatchClassLoader(byte[] catalogBytes, ClassLoader parent) {
            super(parent);
            this.catalogBytes = catalogBytes;
        }

        @Override
        public Class loadClass(String name) throws ClassNotFoundException {
            //if we want a normal class use the normal class loader
            if(!(READER.equals(name) || WRITER.equals(name))) {
                return super.loadClass(name);
            }
           
            //if we have a compiler then regenerate the source and class based on the catalog.
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            if (null!=compiler) {
                FASTReaderDispatchGenerator readerDispatch = new FASTReaderDispatchGenerator(catalogBytes);
                readerDispatch.createWriteSourceClassFiles(compiler);
            }

            //check if the class is available.
            //if class file is not found go back to the default behavior
            File classFile = new File(FASTReaderDispatchGenerator.workingFolder(),SIMPLE_READER_NAME+".class");
            if (!classFile.exists()) {
                throw new ClassNotFoundException("unable to load "+name);
            } else {
                //class does exist so load it, but if anything goes wrong fall back to interpreter
                try {
                    byte[] classData = new byte[(int)classFile.length()];
                    FileInputStream fist = new FileInputStream(classFile);
                    fist.read(classData);
                    fist.close();
                    
                    Class result = defineClass(name, classData , 0, classData.length);

                    return result;
//                    Field field = result.getField("catBytes"); //TODO: B, for larger scale this array must be lz4 compressed
//                    field.setAccessible(true);
//                    byte[] catBytes = (byte[])field.get(null);
//                    if (Arrays.equals(catBytes,catalogBytes)) {
//                        return result;
//                    } else {
//                        throw new ClassNotFoundException("Found old class that was no longer compatible at "+name);
//                    }
                    
                } catch (Exception e) {
                    throw new ClassNotFoundException("unable to load "+name, e);
                }
            }
        }

        //TODO: C, Copy this when we need to buidl the comiler writer.
        public static FASTReaderDispatchBase loadDispatchReader(PrimitiveReader reader, byte[] catalogBytes) {
            try {
                return loadDispatchReaderGenerated(reader, catalogBytes);
            } catch (Exception e) {
                //TODO: C, inform the monitor system,  log this but continue working.
                
                return new FASTReaderInterpreterDispatch(reader, new TemplateCatalog(new PrimitiveReader(catalogBytes,0)));
            }
        }

        public static FASTReaderDispatchBase loadDispatchReaderGenerated(PrimitiveReader reader, byte[] catalogBytes)
                throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException,
                InvocationTargetException {
            FASTDispatchClassLoader classLoader = new FASTDispatchClassLoader(catalogBytes, FASTReaderDispatchBase.class.getClassLoader());
            Class generatedClass = classLoader.loadClass(READER);
            Constructor constructor = generatedClass.getConstructor(PrimitiveReader.class,TemplateCatalog.class);
            
            //TODO: A, catalog is internal should not need this!!
            return (FASTReaderDispatchBase)constructor.newInstance(reader,new TemplateCatalog(new PrimitiveReader(catalogBytes,0)));
            //return (FASTReaderDispatchBase)(generatedClass.newInstance());
        }
        

    }