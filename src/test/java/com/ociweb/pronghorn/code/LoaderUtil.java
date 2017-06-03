package com.ociweb.pronghorn.code;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.util.build.SimpleSourceFileObject;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.test.FuzzGeneratorGeneratorTest;
import com.ociweb.pronghorn.stage.test.TestClassLoader;

public class LoaderUtil {

    public static byte[] readClassBytes(File classFile) throws ClassNotFoundException {
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

    public static File genSourceFolder(Class<FuzzGeneratorGeneratorTest> clazz, String rootName) {
        try {
            String location = clazz.getProtectionDomain().getCodeSource().getLocation().toURI().toString();
            String prefix = "file:";
            int idx = location.indexOf("/target/");
            
            if (location.startsWith(prefix) & idx>0) {
                String root = location.substring(prefix.length(), idx);
                String sourceRoot = root+"/target/generated-sources/"+rootName+"/";
                
                File f = new File(sourceRoot);
                f.mkdirs();
                
                return f;
            }
            
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    public static File genClassesFolder(Class<FuzzGeneratorGeneratorTest> clazz) {
        try {
            String location = clazz.getProtectionDomain().getCodeSource().getLocation().toURI().toString();
            String prefix = "file:";
            int idx = location.indexOf("/target/");
            
            if (location.startsWith(prefix) & idx>0) {
                String root = location.substring(prefix.length(), idx);
                String classesRoot = root+"/target/classes/";
                
                File f = new File(classesRoot);
                f.mkdirs();
                
                return f;
            } else {
                new Exception("unable to recognize location: "+location).printStackTrace();
            }
            
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Constructor generateClassConstructor(String packageName, String className, StringBuilder target, Class clazz) throws ClassNotFoundException, NoSuchMethodException {
        
        return generateClass(packageName, className, target, clazz).getConstructor(GraphManager.class, Pipe.class);
                 

    }

    public static Constructor generateThreeArgConstructor(String packageName, String className, StringBuilder target, Class clazz) throws ClassNotFoundException, NoSuchMethodException {

        return generateClass(packageName, className, target, clazz).getConstructor(GraphManager.class, Pipe.class, Pipe.class);


    }

    public static Class generateClass(String packageName, String className, StringBuilder target, Class clazz) throws ClassNotFoundException, NoSuchMethodException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        
        File baseFolder = genSourceFolder(clazz,"pronghorn");
        File classesFolder = genClassesFolder(clazz);
                
        List<String> optionList = new ArrayList<String>();
        optionList.addAll(Arrays.asList("-classpath", System.getProperty("java.class.path"),
                                        "-d", classesFolder.toString()
                                     //   "-target","1.7",
                                     //   "-source","1.7"
                                        ));                
    
        List<JavaFileObject> toCompile = new ArrayList<JavaFileObject>();
        SimpleSourceFileObject source = new SimpleSourceFileObject(className, target);
        toCompile.add(source);
        
        String packagePath = packageName.replace('.', '/');
        String cannonicalName = packagePath+"/"+className;
        
        
        if (null!=baseFolder) {
            new File(baseFolder, packagePath).mkdirs();
            
            File sourceFile = new File(baseFolder, cannonicalName+".java");
            
            try {
                FileOutputStream fost = new FileOutputStream(sourceFile);
                PrintStream ps = new PrintStream(fost);
                ps.append(target);
                ps.close();
                
                
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            
            
           // System.err.println("write source to :"+sourceFile);
        
        }
    
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
                    
        if (compiler.getTask(null, null, diagnostics, optionList, null, toCompile).call()) {
                File classFile = new File(classesFolder, cannonicalName+".class");

               // System.err.println("write class to :"+ classFile);
                
                String name = packageName+"."+className;
                byte[] classData = readClassBytes(classFile);
                
                return new TestClassLoader(name, classData).loadClass(name);

                 
        } else {
            
                List<Diagnostic<? extends JavaFileObject>> diagnosticList = diagnostics.getDiagnostics();
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
    }

    
}
