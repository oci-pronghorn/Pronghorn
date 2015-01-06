package com.ociweb.jfast.generator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.lang.model.SourceVersion;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.error.FASTException;
import com.ociweb.pronghorn.ring.util.hash.IntHashTable;

    public class FASTClassLoader extends ClassLoader{

    	private static final Logger log = LoggerFactory.getLogger(FASTClassLoader.class);
    	
        public static final String GENERATED_PACKAGE = "com.ociweb.jfast.generator";
        public static final String SIMPLE_READER_NAME = "FASTReaderGeneratedDispatch";
        public static final String SIMPLE_WRITER_NAME = "FASTWriterGeneratedDispatch";
        
        public static final String READER = GENERATED_PACKAGE+'.'+SIMPLE_READER_NAME;
        public static final String WRITER = GENERATED_PACKAGE+'.'+SIMPLE_WRITER_NAME;
                
        final byte[] catBytes;// serialized catalog for the desired templates XML
        final boolean forceCompile;
        final boolean exportSource;
        public static boolean fullCompiled = false;
        
        static final File workingFolder = new File(new File(System.getProperty("java.io.tmpdir")),"jFAST");
        static {
            workingFolder.mkdirs();
        }
        
        public FASTClassLoader(byte[] catBytes, ClassLoader parent) {
            this(catBytes,parent,false);
        }
        
        public FASTClassLoader(byte[] catBytes, ClassLoader parent, boolean forceCompile) {
            super(parent);
            this.catBytes = catBytes;
            this.exportSource = Boolean.getBoolean("FAST.exportSource");
            this.forceCompile = forceCompile | exportSource | Boolean.getBoolean("FAST.forceCompile");
            log.trace("Created new FASTClassLoader forceCompile:"+forceCompile+" exportSource:"+exportSource);
            
        }        
        
        @SuppressWarnings("boxing")
        @Override
        public Class loadClass(String name) throws ClassNotFoundException {
            //if we want a normal class use the normal class loader
            if(!name.contains("GeneratedDispatch") || !name.contains("jfast.generator.FAST")) {
                return super.loadClass(name);
            }
            
            //if class is found and matches use it.
            String simpleClassName = name.substring(name.lastIndexOf('.')+1);
            
            
            File classFile = targetFile(simpleClassName, "class");
            
            //CAUTION: only use force compile when you need deep testing it can be very slow.
            if ((fullCompiled || !forceCompile) && classFile.exists()) {
                log.trace("Reading class from: {}",classFile);
                
                byte[] classData = readClassBytes(classFile);
                                    
                //returning with defineClass helps reduce the risk that we may try to define the name again.
                return defineClass(name, classData , 0, classData.length);                    
            }
                        
            //if we have a compiler then regenerate the source and class based on the templates.
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            if (null!=compiler && compiler.getSourceVersions().contains(SourceVersion.RELEASE_6)) {
                log.trace("Compile class to: {}",classFile);
                
                List<String> optionList = new ArrayList<String>();
                optionList.addAll(Arrays.asList("-classpath", System.getProperty("java.class.path"),
                                                "-d", workingFolder.toString(),
                                                "-target","1.6",
                                                "-source","1.6"
                                                ));                

                List<JavaFileObject> toCompile = new ArrayList<JavaFileObject>();

                {//scoped to help GC 
                	FASTReaderDispatchGenerator readGenerator = new FASTReaderDispatchGenerator(catBytes, toCompile, TemplateCatalogConfig.buildRingBuffers(new TemplateCatalogConfig(catBytes), (byte)8, (byte)18));
                	SimpleSourceFileObject sourceReaderFileObject = new SimpleSourceFileObject(FASTClassLoader.SIMPLE_READER_NAME,
                													   						   readGenerator.generateFullSource(new StringBuilder()));
                	toCompile.add(sourceReaderFileObject);
                	
                }
                
                {//scoped to help GC
                	FASTWriterDispatchGenerator writeGenerator = new FASTWriterDispatchGenerator(catBytes, new TemplateCatalogConfig(catBytes), toCompile);
                	SimpleSourceFileObject sourceWriterFileObject = new SimpleSourceFileObject(FASTClassLoader.SIMPLE_WRITER_NAME,
									             										       writeGenerator.generateFullSource(new StringBuilder()));
					toCompile.add(sourceWriterFileObject);
                }
                System.err.println("Begin full compile of "+toCompile.size()+" files");
                
                if (exportSource) {
                	for(JavaFileObject jfo:toCompile) {
                        try {
                            exportSourceToClassFolder(jfo.getName(),jfo.getCharContent(false).toString());
                        } catch (IOException e) {
                            throw new FASTException(e);
                        }	                		
                	}
                }
                DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
                
                
                if (compiler.getTask(null, null, diagnostics, optionList, null, toCompile).call()) {
                    byte[] classData = readClassBytes(classFile);
                    Class result =  defineClass(name, classData , 0, classData.length);
                    fullCompiled = true; //only set after success
                    System.err.println("Finished full compile");
                    return result;
                } else {
                    reportCompileError(diagnostics.getDiagnostics());      
                }
            }
            throw new ClassNotFoundException();
        }

        private void reportCompileError(List<Diagnostic<? extends JavaFileObject>> diagnosticList)
                throws ClassNotFoundException {
        	
            FASTClassLoader.logCompileError(diagnosticList);
            //did not compile due to error
            if (!diagnosticList.isEmpty()) {
                throw new ClassNotFoundException(diagnosticList.get(0).toString());
            } else {
                throw new ClassNotFoundException("Compiler error");
            }
        }

        private byte[] readClassBytes(File classFile) throws ClassNotFoundException {
            byte[] classData = new byte[(int)classFile.length()];
            try {
                FileInputStream input = new FileInputStream(classFile);
                input.read(classData);
                input.close(); 
            } catch (Exception e) {
                throw new ClassNotFoundException("Unable to read class file.", e);
            }
            return classData;
        }


        private void exportSourceToClassFolder(String name, String content) {
            try {
                File sourceFile = targetFile(name, "java");
               // System.err.println("Wrote source to: "+sourceFile);
                FileWriter out = new FileWriter(sourceFile);
                out.write(content);
                out.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        
        public static void logCompileError(List<Diagnostic<? extends JavaFileObject>> diagnostics) {
		    Iterator<Diagnostic<? extends JavaFileObject>> iter = diagnostics.iterator();
		    while (iter.hasNext()) {
		        System.err.println(iter.next());
		    }
		}

		private static File targetFile(String name, String ext) {
            return new File(workingFolder,GENERATED_PACKAGE.replace('.', File.separatorChar)+File.separatorChar+name+"."+ext);
        }

        public static void deleteFiles() {
            targetFile(SIMPLE_READER_NAME,"class").delete();
            targetFile(SIMPLE_READER_NAME,"java").delete();
            
            targetFile(SIMPLE_WRITER_NAME,"class").delete();
            targetFile(SIMPLE_WRITER_NAME,"java").delete(); 
        }
        

    }