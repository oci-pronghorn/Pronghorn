package com.ociweb.pronghorn.stage.test;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGenerator;
import com.ociweb.pronghorn.stage.PronghornStage;

public class StageGenerator {
    
    private final String packageName;
    private final String className;
    
    private final TemplateProcessGenerator tpg;
    private Appendable target;
    
    public StageGenerator(TemplateProcessGenerator tpg) {
        this.tpg = tpg;
        this.className = "MyClass";
        this.packageName = "com.ociweb.pronghorn.pipe.build";
        this.target = new StringBuilder();
    }
            
    
    public void generate() throws IOException {        
        
        try {
                        
            tpg.processSchema();
            
            
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
        
        System.out.println(target);
        
        
    }
    
    
    
}
