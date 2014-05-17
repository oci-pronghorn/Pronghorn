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

    //TODO: A, A clean this up as the basis for a quick talk at the staff meeting. Finish by Sunday night for code talk on this subject

    //* state the problem then walk the solution.
    //* Focus on ClassLoader, FileObject and how its used.

    //* Show class replacement, source compile, shared based class. public members due to my odd code?
    //* show how at runtime the class can be replaced with a new implementation.
    //* log each stage to know that it is done?

//Slides: (10 days to wrap up)
//    1. problem set (show java linking path, show what I want to accomplish with replacements)
//    2. APIs we will be using to solve the problem. (Compiler, FileObject, ClassLoader)
//    3. Demo - show the unit tests, walk thru the code

//* do not review source generation unless we have time at the end

    public class FASTClassLoader extends ClassLoader{

        public static final String GENERATED_PACKAGE = "com.ociweb.jfast.generator";
        public static final String SIMPLE_READER_NAME = "FASTReaderGeneratedDispatch";
        public static final String SIMPLE_WRITER_NAME = "FASTWriterGeneratedDispatch";
        
        public static final String READER = GENERATED_PACKAGE+'.'+SIMPLE_READER_NAME;
        public static final String WRITER = GENERATED_PACKAGE+'.'+SIMPLE_WRITER_NAME;
        
        final byte[] catBytes;// serialized catalog for the desired templates XML
        final boolean forceCompile;
        final boolean exportSource;

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
            
        }        

        //TODO: build unit test that can replace class behavior on the fly.
        
        @Override
        public Class loadClass(String name) throws ClassNotFoundException {
            //if we want a normal class use the normal class loader
            if(!(READER.equals(name) || WRITER.equals(name))) {
                return super.loadClass(name);
            }
            
            //if class is found and matches use it.
            File classFile = new File(workingFolder,GENERATED_PACKAGE.replace('.', File.separatorChar)+File.separatorChar+SIMPLE_READER_NAME+".class");
            System.out.println("Read class from: "+classFile+" force compile: "+forceCompile);
            if (!forceCompile && classFile.exists()) {
                
                byte[] classData = new byte[(int)classFile.length()];
                try {
                    FileInputStream input = new FileInputStream(classFile);
                    input.read(classData);
                    input.close(); 
                } catch (Exception e) {
                    throw new ClassNotFoundException("Unable to read class file.", e);
                }
                                    
                //returning with defineClass helps reduce the risk that we may try to define the name again.
                return defineClass(name, classData , 0, classData.length);                    
            }
                        
            //if we have a compiler then regenerate the source and class based on the templates.
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            if (null!=compiler && compiler.getSourceVersions().contains(SourceVersion.RELEASE_6)) {
                System.err.println("recompile");
                DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
                
                List<String> optionList = new ArrayList<String>();
                optionList.addAll(Arrays.asList("-classpath", System.getProperty("java.class.path"),
                                                "-d", workingFolder.toString(),
                                                "-target","1.6",
                                                "-source","1.6"
                                                ));
                

                List<JavaFileObject> toCompile = new ArrayList<JavaFileObject>();
                FASTReaderSourceFileObject sourceFileObject = new FASTReaderSourceFileObject(catBytes);
                
                if (exportSource) {          
                    exportSourceToClassFolder(sourceFileObject);
                }
                
                toCompile.add(sourceFileObject);
                JavaCompiler.CompilationTask task = compiler.getTask(null, null, diagnostics, optionList, null, toCompile);
                
                if (!task.call()) {
                    List<Diagnostic<? extends JavaFileObject>> diagnosticList = diagnostics.getDiagnostics();
                    Supervisor.logCompileError(diagnosticList);
                    //did not compile due to error
                    if (!diagnosticList.isEmpty()) {
                        throw new ClassNotFoundException(diagnosticList.get(0).toString());
                    } else {
                        throw new ClassNotFoundException("Compiler error");
                    }                 
                } 
                byte[] classData = new byte[(int)classFile.length()];
                try {
                    FileInputStream input = new FileInputStream(classFile);
                    input.read(classData);
                    input.close();
                    return defineClass(name, classData , 0, classData.length);
                } catch (Exception e) {
                   throw new ClassNotFoundException("Unable to read class file.", e);
                }
            }
            throw new ClassNotFoundException();
        }

        private void exportSourceToClassFolder(FASTReaderSourceFileObject sourceFileObject) {
            try {
                String sourcePath = GENERATED_PACKAGE.replace('.', File.separatorChar)+File.separatorChar+SIMPLE_READER_NAME+".java";
                File sourceFile = new File(workingFolder,sourcePath);
                System.out.println("Wrote source to: "+sourceFile);
                FileWriter out = new FileWriter(sourceFile);
                out.write(sourceFileObject.getCharContent(false).toString());
                out.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        

    }