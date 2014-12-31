package com.ociweb.jfast.generator;


public class Supervisor {
    
	//TODO: AA, remove this class in favor of SLF4J
	
    public static void err(String message, Throwable t){
        System.err.println(message);
        t.printStackTrace();
    }
    
    public static void log(String message){
     //   System.out.println(message);
    }

    public static void templateSource(String templates) {
        System.out.println("Templates:"+templates);
    }

}
