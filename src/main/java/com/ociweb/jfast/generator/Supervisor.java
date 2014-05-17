package com.ociweb.jfast.generator;

import java.util.Iterator;
import java.util.List;

import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

public class Supervisor {
    
    public static void log(String message, Throwable t){
        
    }
    
    public static void log(String message){
        
    }

    public static void logCompileError(List<Diagnostic<? extends JavaFileObject>> diagnostics) {
        Iterator<Diagnostic<? extends JavaFileObject>> iter = diagnostics.iterator();
        while (iter.hasNext()) {
            System.err.println(iter.next());
        }
    }

}
