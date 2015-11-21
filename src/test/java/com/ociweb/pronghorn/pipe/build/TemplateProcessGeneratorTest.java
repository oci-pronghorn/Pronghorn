package com.ociweb.pronghorn.pipe.build;


import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
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
    
    @Test
    public void test() {
        
        try {
            FieldReferenceOffsetManager from = TemplateHandler.loadFrom("/template/smallExample.xml");
            MessageSchema schema = new MessageSchemaDynamic(from);
            
            StringBuilder target = new StringBuilder();
            
            target.append("package com.ociweb.pronghorn.pipe.build;\n");
            target.append("import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;\n");
            target.append("import com.ociweb.pronghorn.pipe.Pipe;\n");
            
            target.append("public class TestLowLevelReader {\n");
            target.append(" Pipe input;\n"); //TODO these need imorts
            target.append(" LowLevelStateManager navState;\n");
            
            TemplateProcessGeneratorLowLevelReader simple = new TemplateProcessGeneratorLowLevelReader(schema, target);
            
            simple.processSchema();
            
            target.append("}\n");
            
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            
            List<String> optionList = new ArrayList<String>();
            optionList.addAll(Arrays.asList("-classpath", System.getProperty("java.class.path"),
                                            "-d", workingFolder.toString(),
                                            "-target","1.6",
                                            "-source","1.6"
                                            ));                

            String className = "TestLowLevelReader";
            SimpleSourceFileObject sourceObj = new SimpleSourceFileObject(className, target);
            List<JavaFileObject> toCompile = new ArrayList<JavaFileObject>();
            toCompile.add(sourceObj);

            DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
            
      //      String simpleClassName = "NewClass";
      //      File classFile = targetFile(simpleClassName, "class");
            
            if (compiler.getTask(null, null, diagnostics, optionList, null, toCompile).call()) {
//                byte[] classData = readClassBytes(classFile);
//                Class result =  defineClass(name, classData , 0, classData.length);
//                fullCompiled = true; //only set after success
//                log.debug("Finished full compile");
//                return result;
            } else {
                
                try {
                    reportCompileError(diagnostics.getDiagnostics());
                } catch (ClassNotFoundException e) {

                    System.out.println( target.toString() );
                          
                    e.printStackTrace();
                    fail();
                }      
                
            }
            
            
            
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
    
}
