package com.ociweb.pronghorn.pipe.build;


import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;
import com.ociweb.pronghorn.pipe.util.build.SimpleSourceFileObject;
import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGeneratorLowLevelReader;
import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGeneratorLowLevelWriter;

public class TemplateProcessGeneratorTest {

    static final File workingFolder = new File(new File(System.getProperty("java.io.tmpdir")),"jFAST");
    static {
        workingFolder.mkdirs();
    }
    
    
    private void reportCompileError(List<Diagnostic<? extends JavaFileObject>> diagnosticList)
            throws ClassNotFoundException {
        
        Iterator<Diagnostic<? extends JavaFileObject>> iter = diagnosticList.iterator();
        while (iter.hasNext()) {
            System.err.println(iter.next());
        }
        //did not compile due to error
        if (!diagnosticList.isEmpty()) {
            throw new ClassNotFoundException(diagnosticList.get(0).toString());
        } else {
            throw new ClassNotFoundException("Compiler error");
        }
    }
    
    private byte[] readClassBytes(File classFile) throws ClassNotFoundException {
        int c = (int)classFile.length();
        int p = 0;
        byte[] classData = new byte[c];
        try {
            FileInputStream input = new FileInputStream(classFile);
            do {
                int count = input.read(classData, p, c);
                if (count>=0) {
                    p += count;
                    c -= count;
                }
            } while (c>0);
            
            input.close(); 
        } catch (Exception e) {
            throw new ClassNotFoundException("Unable to read class file.", e);
        }
        return classData;
    }
    
    private byte[] readClassBytes(InputStream input) throws ClassNotFoundException, IOException {
        int c = (int)input.available();
        int p = 0;
        byte[] classData = new byte[c];
        try {
            do {
                int count = input.read(classData, p, c);
                if (count>=0) {
                    p += count;
                    c -= count;
                }
            } while (c>0);
            
            input.close(); 
        } catch (Exception e) {
            throw new ClassNotFoundException("Unable to read class file.", e);
        }
        return classData;
    }
    
    @Test
    public void loadExistingBytes() {
        
        ///NOTE that for some files with business logic this will not work and a 
        //      static field holding the FROM will need to be populated.
        
        Class c = this.getClass();
        
        try {
            //this method will be used by Pronghorn to check when the class must be re-compiled.
            byte[] foundClassData = loadExistingBytes(c);
            assertTrue(foundClassData.length>0);   
            confirmEndOfBytesLoaded(foundClassData);                       
            
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            fail();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        
        
    }

    private void confirmEndOfBytesLoaded(byte[] foundClassData) {
        //check last bytes and confirm they are are not zero
        byte[] temp = foundClassData;
        int i = foundClassData.length;
        int limit = Math.max(0, i-10);
        int v = 0;
        while (--i >= limit) {
            v |= temp[i];
        }
        assertTrue(v!=0);
    }
    
    @Test
    public void testGenerateLowLevelReaderCleanCompile() {
        
        try {
            FieldReferenceOffsetManager from = TemplateHandler.loadFrom("/template/smallExample.xml");
            MessageSchema schema = new MessageSchemaDynamic(from);
                        
            String className = "TestLowLevelReader";
            String pipeName = "input";
            
            StringBuilder target = buildDummyHeader(className);
                        
            TemplateProcessGeneratorLowLevelReader simple = new TemplateProcessGeneratorLowLevelReader(schema, target, pipeName);
            
            simple.processSchema();
            
            target.append("}\n");
    //        System.out.println(target);
            
            validateCleanCompile(className, target);
            
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
            fail();
        } catch (SAXException e) {
            e.printStackTrace();
            fail();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    
    @Test
    public void testGenerateLowLevelWriterCleanCompile() {
        
        try {
            FieldReferenceOffsetManager from = TemplateHandler.loadFrom("/template/smallExample.xml");
            MessageSchema schema = new MessageSchemaDynamic(from);
                        
            String className = "TestLowLevelWriter";
            String pipeName = "output";
            
            StringBuilder target = buildDummyHeader(className);
                        
            TemplateProcessGeneratorLowLevelWriter simple = new TemplateProcessGeneratorLowLevelWriter(schema, target, pipeName);
            
            simple.processSchema();
            
            target.append("}\n");
    //        System.out.println(target);
            
            validateCleanCompile(className, target);
            
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
            fail();
        } catch (SAXException e) {
            e.printStackTrace();
            fail();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }
    
    
    private StringBuilder buildDummyHeader(String className) {
        StringBuilder target = new StringBuilder();
        
        target.append("package com.ociweb.pronghorn.pipe.build;\n");
        target.append("import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;\n");
        target.append("import com.ociweb.pronghorn.pipe.Pipe;\n");
        target.append("import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;\n");
        target.append("import com.ociweb.pronghorn.pipe.util.Appendables;\n");
        target.append("import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;\n");
        target.append("public class ").append(className).append(" implements Runnable {\n");

        target.append("\n");
        target.append("private void requestShutdown() {};\n"); //only here so generated code passes compile.
        return target;
    }

    private void validateCleanCompile(String className, StringBuilder target) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        
        List<String> optionList = new ArrayList<String>();
        optionList.addAll(Arrays.asList("-classpath", System.getProperty("java.class.path"),
                                        "-d", workingFolder.toString(),
                                        "-target","1.7",
                                        "-source","1.7"
                                        ));                

        SimpleSourceFileObject sourceObj = new SimpleSourceFileObject(className, target);
        List<JavaFileObject> toCompile = new ArrayList<JavaFileObject>();
        toCompile.add(sourceObj);

        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
                    
        if (compiler.getTask(null, null, diagnostics, optionList, null, toCompile).call()) {
            try {
                File classFile = new File(workingFolder, "com/ociweb/pronghorn/pipe/build/"+className+".class");
                byte[] classData = readClassBytes(classFile);
                assertTrue(classData.length>0);
                confirmEndOfBytesLoaded(classData);
//                Class result =  defineClass(name, classData , 0, classData.length);
                
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                fail();
            }

        } else {
            
            try {
                reportCompileError(diagnostics.getDiagnostics());
            } catch (ClassNotFoundException e) {

                System.out.println( target.toString() );
                      
                e.printStackTrace();
                fail();
            }                 
        }
    }

    private byte[] loadExistingBytes(Class c) throws ClassNotFoundException, IOException {
        byte[] foundClassData = readClassBytes(c.getClassLoader().getResourceAsStream(c.getName().replace('.', '/') + ".class"));
        return foundClassData;
    }
    
}
