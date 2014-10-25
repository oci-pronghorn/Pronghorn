package com.ociweb.jfast.catalog.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CatalogGenerator implements ItemGenerator {
    public static final String HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+
                                 "<templates xmlns=\"http://www.fixprotocol.org/ns/fast/td/1.1\">\n";
    public static final String FOOTER = "</templates>";
    
    List<TemplateGenerator> templates = new ArrayList<TemplateGenerator>();
    
    
    //used to initialize the test files as needed, then binarys can be used for each run.
    public CatalogGenerator(){        
    }
    
    public TemplateGenerator addTemplate(String name, int id, boolean reset, String dictionary) { //return new empty template on which we will add fields.
        TemplateGenerator tg = new TemplateGenerator(name, id, reset, dictionary);
        addTemplate(tg);
        return tg;
    }

    public void addTemplate(TemplateGenerator tg) {
        templates.add(tg);
    }
    
        
    public String toString() {
        try {
			return appendTo("    ",new StringBuilder()).toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    }

    public Appendable appendTo(String tab, Appendable result) throws IOException {
        result.append(HEADER);
        for(TemplateGenerator g:templates) {
            g.appendTo(tab, result);
        }
        result.append(FOOTER);
        return result;
    }

}
