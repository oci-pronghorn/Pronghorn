package com.ociweb.pronghorn.ring.schema.generator;

import java.io.IOException;

public interface ItemGenerator {
    
    Appendable appendTo(String tab, Appendable result) throws IOException;
    
}
