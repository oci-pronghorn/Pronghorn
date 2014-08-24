package com.ociweb.jfast.catalog.generator;

import java.util.ArrayList;
import java.util.List;

public class CatalogGenerator implements ItemGenerator {
    static final String HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+
                                 "<templates xmlns=\"http://www.fixprotocol.org/ns/fast/td/1.1\">\n";
    static final String FOOTER = "</templates>";
    
    List<TemplateGenerator> templates = new ArrayList<TemplateGenerator>();
    
    
    //used to initialize the test files as needed, then binarys can be used for each run.
    public CatalogGenerator(){        
    }
    
    public TemplateGenerator addTemplate(String name, int id, boolean reset, String dictionary) { //return new empty template on which we will add fields.
        TemplateGenerator tg = new TemplateGenerator(name, id, reset, dictionary);
        templates.add(tg);
        return tg;
    }
        
    public String toString() {
        return appendTo(new StringBuilder()).toString();
    }

    public StringBuilder appendTo(StringBuilder result) {
        result.append(HEADER);
        for(TemplateGenerator g:templates) {
            g.appendTo(result);
        }
        result.append(FOOTER);
        return result;
    }
}
