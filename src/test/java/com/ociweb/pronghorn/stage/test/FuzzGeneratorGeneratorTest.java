package com.ociweb.pronghorn.stage.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.util.build.SimpleSourceFileObject;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class FuzzGeneratorGeneratorTest {

    
    @Test
    public void fuzzGeneratorBuildTest() {
        
        StringBuilder target = new StringBuilder();
        FuzzGeneratorGenerator ew = new FuzzGeneratorGenerator(PipeMonitorSchema.instance, target);

        try {
            ew.processSchema();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        
        System.out.println(target);
        
        validateCleanCompile(ew.getPackageName(), ew.getClassName(), target);

    }
    
    @Test
    public void fuzzGeneratorUsageTest() {
        
        StringBuilder target = new StringBuilder();
        FuzzGeneratorGenerator ew = new FuzzGeneratorGenerator(PipeMonitorSchema.instance, target);

        try {
            ew.processSchema();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        
        File workingFolder = new File(new File(System.getProperty("java.io.tmpdir")),"pronghorn");
        workingFolder.mkdirs();
        
        
        try {
            Constructor constructor =  generateClassConstructor(ew.getPackageName(), ew.getClassName(), target, workingFolder);
            
            
            GraphManager gm = new GraphManager();
            
            PipeConfig<PipeMonitorSchema> config = new PipeConfig<PipeMonitorSchema>(PipeMonitorSchema.instance, 20000);
            Pipe<PipeMonitorSchema> pipe = new Pipe<PipeMonitorSchema>(config);
            
            
            constructor.newInstance(gm, pipe);
            
            
            ConsoleSummaryStage dump = new ConsoleSummaryStage(gm, pipe);
            
            GraphManager.enableBatching(gm);
            MonitorConsoleStage.attach(gm);
            
            ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
            scheduler.playNice=false;
            scheduler.startup();
            
            Thread.sleep(1000);
            
            scheduler.shutdown();
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
            
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    
    
    
    private static void validateCleanCompile(String packageName, String className, StringBuilder target) {
        try {

        //TODO: change this to the maven folder structure.
        File workingFolder = new File(new File(System.getProperty("java.io.tmpdir")),"pronghorn");
        workingFolder.mkdirs();
        
        
        Constructor constructor =  generateClassConstructor(packageName, className, target, workingFolder);
        assertNotNull(constructor);
        
        
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            fail();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            fail();
        } catch (SecurityException e) {
            e.printStackTrace();
            fail();
        }
        
    }
    
    //TODO: methods below this point need to be moved to static helper class.


    private static Constructor generateClassConstructor(String packageName, String className, StringBuilder target, File workingFolder) throws ClassNotFoundException, NoSuchMethodException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        
        List<String> optionList = new ArrayList<String>();
        optionList.addAll(Arrays.asList("-classpath", System.getProperty("java.class.path"),
                                        "-d", workingFolder.toString(),
                                        "-target","1.7",
                                        "-source","1.7"
                                        ));                

        List<JavaFileObject> toCompile = new ArrayList<JavaFileObject>();
        SimpleSourceFileObject source = new SimpleSourceFileObject(className, target);
        toCompile.add(source);
        
        String folders = packageName.replace('.', '/');
        String cannonicalName = folders+"/"+className;
        
        Class<FuzzGeneratorGeneratorTest> clazz = FuzzGeneratorGeneratorTest.class;
        File baseFolder = genSourceFolder(clazz,"pronghorn");
        
        if (null!=baseFolder) {
            File temp = new File(baseFolder, folders);

            System.out.println("exits:" + temp.exists()+"  "+temp);
            System.out.println("isDir:" + temp.isDirectory());
            temp.mkdirs();
            
            File sourceFile = new File(baseFolder, cannonicalName+".java");
            
            try {
                FileOutputStream fost = new FileOutputStream(sourceFile);
                PrintStream ps = new PrintStream(fost);
                ps.append(target);
                ps.close();
                
                
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            
            
            System.out.println("write to :"+sourceFile);
        
        }
   
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
                    
        if (compiler.getTask(null, null, diagnostics, optionList, null, toCompile).call()) {
                File classFile = new File(workingFolder, cannonicalName+".class");
                String name = packageName+"."+className;
                byte[] classData = readClassBytes(classFile);
                
                Class generatedClass = new TestClassLoader(name, classData).loadClass(name);
                
               // System.out.println("source : "+        generatedClass.getProtectionDomain().getCodeSource());
                
                return generatedClass.getConstructor(GraphManager.class, Pipe.class);
                 
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
    
    
    private static byte[] readClassBytes(File classFile) throws ClassNotFoundException {
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
    
    
}
