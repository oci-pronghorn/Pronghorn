package com.ociweb.jfast.generator;

import java.util.Iterator;
import java.util.List;

import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

public class Supervisor {
    
    public static void err(String message, Throwable t){
        System.err.println(message);
        t.printStackTrace();
    }
    
    public static void log(String message){//TODO: convert to specific calls
     //   System.out.println(message);
    }

    public static void logCompileError(List<Diagnostic<? extends JavaFileObject>> diagnostics) {
        Iterator<Diagnostic<? extends JavaFileObject>> iter = diagnostics.iterator();
        while (iter.hasNext()) {
            System.err.println(iter.next());
        }
    }

    public static void templateSource(String templates) {
        System.out.println("Templates:"+templates);
    }

}
