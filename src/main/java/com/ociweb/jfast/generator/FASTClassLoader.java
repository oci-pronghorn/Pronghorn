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
            Supervisor.log("Created new FASTClassLoader forceCompile:"+forceCompile+" exportSource:"+exportSource);
        }        
        
        @Override
        public Class loadClass(String name) throws ClassNotFoundException {
            //if we want a normal class use the normal class loader
            if(!(READER.equals(name) || WRITER.equals(name))) {
                return super.loadClass(name);
            }
            
            //if class is found and matches use it.
            File classFile = targetFile("class");
            if (!forceCompile && classFile.exists()) {
                Supervisor.log("Reading class from: "+classFile);
                
                byte[] classData = readClassBytes(classFile);
                                    
                //returning with defineClass helps reduce the risk that we may try to define the name again.
                return defineClass(name, classData , 0, classData.length);                    
            }
                        
            //if we have a compiler then regenerate the source and class based on the templates.
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            if (null!=compiler && compiler.getSourceVersions().contains(SourceVersion.RELEASE_6)) {
                Supervisor.log("Compile class to: "+classFile);
                
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
                DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
                JavaCompiler.CompilationTask task = compiler.getTask(null, null, diagnostics, optionList, null, toCompile);
                
                if (task.call()) {
                    byte[] classData = readClassBytes(classFile);
                    return defineClass(name, classData , 0, classData.length);
                } else {
                    reportCompileError(diagnostics.getDiagnostics());      
                }
            }
            throw new ClassNotFoundException();
        }

        private void reportCompileError(List<Diagnostic<? extends JavaFileObject>> diagnosticList)
                throws ClassNotFoundException {
            Supervisor.logCompileError(diagnosticList);
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


        private void exportSourceToClassFolder(FASTReaderSourceFileObject sourceFileObject) {
            try {
                File sourceFile = targetFile("java");
                Supervisor.log("Wrote source to: "+sourceFile);
                FileWriter out = new FileWriter(sourceFile);
                out.write(sourceFileObject.getCharContent(false).toString());
                out.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        
        private static File targetFile(String ext) {
            return new File(workingFolder,GENERATED_PACKAGE.replace('.', File.separatorChar)+File.separatorChar+SIMPLE_READER_NAME+"."+ext);
        }

        public static void deleteFiles() {
            targetFile("class").delete();
            targetFile("java").delete();
            
        }
        

    }