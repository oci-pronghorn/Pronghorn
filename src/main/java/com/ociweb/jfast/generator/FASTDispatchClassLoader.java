package com.ociweb.jfast.generator;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.lang.model.SourceVersion;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.loader.TemplateLoader;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.FASTReaderDispatchBase;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;

    //TODO: A, A clean this up as the basis for a quick talk at the staff meeting. Finish by Sunday night for code talk on this subject

    //* state the problem then walk the solution.

    //* do not review source generation unless we have time at the end
    //* Show class replacement, source compile, shared based class. public members due to my odd code?
    //* show how at runtime the class can be replaced with a new implementation.
    //* log each stage to know that it is done?
    //* remove rabin dependency to make it easier to test

    public class FASTDispatchClassLoader extends ClassLoader{

        public static final String GENERATED_PACKAGE = "com.ociweb.jfast.stream";
        public static final String SIMPLE_READER_NAME = "FASTReaderDispatchGenExample";
        public static final String SIMPLE_WRITER_NAME = "FASTWriterDispatchGenExample";
        
        public static final String READER = GENERATED_PACKAGE+'.'+SIMPLE_READER_NAME;
        public static final String WRITER = GENERATED_PACKAGE+'.'+SIMPLE_WRITER_NAME;
        
        final byte[] catBytes;// serialized catalog for the desired templates XML
        final boolean forceCompile;

        static final File classFolder = new File(new File(System.getProperty("java.io.tmpdir")),"jFAST");
        static {
            classFolder.mkdirs();
        }
        
        public FASTDispatchClassLoader(byte[] catBytes, ClassLoader parent) {
            this(catBytes,parent,false);
        }
        
        public FASTDispatchClassLoader(byte[] catBytes, ClassLoader parent, boolean forceCompile) {
            super(parent);
            this.catBytes = catBytes;
            this.forceCompile = forceCompile;
        }
        

        @Override
        public Class loadClass(String name) throws ClassNotFoundException {
            //if we want a normal class use the normal class loader
            if(!(READER.equals(name) || WRITER.equals(name))) {
                return super.loadClass(name);
            }
            
            //if class is found and matches use it.
            File classFile = new File(classFolder,GENERATED_PACKAGE.replace('.', File.separatorChar)+File.separatorChar+SIMPLE_READER_NAME+".class");
            if (!forceCompile && classFile.exists()) {
                
                byte[] classData = new byte[(int)classFile.length()];
                try {
                    
                    FileInputStream input = new FileInputStream(classFile);
                    input.read(classData);
                    input.close(); 
                                        
                    //returning with defineClass helps reduce the risk that we may try to define the name again.
                    return defineClass(name, classData , 0, classData.length);                    

                } catch (Exception e) {
                   throw new ClassNotFoundException("Unable to read class file.", e);
                }
            }
                        
            //if we have a compiler then regenerate the source and class based on the templates.
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            if (null!=compiler && compiler.getSourceVersions().contains(SourceVersion.RELEASE_6)) {

                //write class file
                List<JavaFileObject> toCompile = new ArrayList<JavaFileObject>();
                toCompile.add(new GeneratedReaderFileObject(catBytes));
                
                DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
                StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null);
                
                List<String> optionList = new ArrayList<String>();
                optionList.addAll(Arrays.asList("-classpath", System.getProperty("java.class.path"),
                                                "-d", classFolder.toString()
                                                ));
                JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, optionList, null, toCompile);
                
                if (!task.call()) {
                    //did not compile due to error
                    Iterator<Diagnostic<? extends JavaFileObject>> iter = diagnostics.getDiagnostics().iterator();
                    if (iter.hasNext()) {
                        throw new ClassNotFoundException(iter.next().toString());
                    } else {
                        throw new ClassNotFoundException();
                    }
                                        
                }
                System.err.println("success we will now use the new class ");
                byte[] classData = new byte[(int)classFile.length()];
                try {
                    FileInputStream input = new FileInputStream(classFile);
                    input.read(classData);
                    input.close();
                    return defineClass(name, classData , 0, classData.length);//TODO: should pass in protection domain.
                } catch (Exception e) {
                   throw new ClassNotFoundException("Unable to read class file.", e);
                }
            }
            throw new ClassNotFoundException();
        }

        
        public static FASTReaderDispatchBase loadDispatchReader(PrimitiveReader reader, File templates) {
            byte[] catBytes = buildRawCatalogData(templates);
            
            try {
                return loadDispatchReaderGenerated(reader, catBytes);
            } catch (Exception e) {
                e.printStackTrace();// log this but continue working. TODO: we do not want to log if there was no compiler?
                
                return new FASTReaderInterpreterDispatch(reader, catBytes);
            }
        }

        public static FASTReaderDispatchBase loadDispatchReaderGenerated(PrimitiveReader reader, byte[] catBytes)
                throws ReflectiveOperationException, SecurityException {
            
            Class generatedClass = new FASTDispatchClassLoader(catBytes, FASTReaderDispatchBase.class.getClassLoader()).loadClass(READER);
            
            byte[] catBytesFromClass = (byte[])generatedClass.getField("catBytes").get(null);
            if (!Arrays.equals(catBytesFromClass,catBytes)) {
                System.err.println("attempt recompile due to change in templates.");
                //the templates catalog this was generated for does not match the current value so force a recompile
                generatedClass = new FASTDispatchClassLoader(catBytes, FASTReaderDispatchBase.class.getClassLoader(),true).loadClass(READER);
            }
                                    
            //TODO: A, catalog is internal should not need this!! REmove all the arguments here!!!
            try {
                Constructor<FASTReaderDispatchBase> constructor = generatedClass.getConstructor(PrimitiveReader.class,TemplateCatalog.class);
                return constructor.newInstance(reader,new TemplateCatalog(new PrimitiveReader(catBytes,0)));
            } catch (Throwable t) {
                t.printStackTrace();
                System.err.println("attempting recompile");
                //can not create instance because the class is no longer compatible with the rest of the code base so force a recompile
                generatedClass = new FASTDispatchClassLoader(catBytes, FASTReaderDispatchBase.class.getClassLoader(),true).loadClass(READER);
                Constructor<FASTReaderDispatchBase> constructor = generatedClass.getConstructor(PrimitiveReader.class,TemplateCatalog.class);
                return constructor.newInstance(reader,new TemplateCatalog(new PrimitiveReader(catBytes,0)));
            }
        }
        
        static byte[] buildRawCatalogData(File templates) { //TODO: need XML parse service to cache this.

            ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
            try {
                TemplateLoader.buildCatalog(catalogBuffer, templates);
            } catch (Exception e) {
                e.printStackTrace();
            }

            assertTrue("Catalog must be built.", catalogBuffer.size() > 0);

            byte[] catalogByteArray = catalogBuffer.toByteArray();
            return catalogByteArray;
        }

    }