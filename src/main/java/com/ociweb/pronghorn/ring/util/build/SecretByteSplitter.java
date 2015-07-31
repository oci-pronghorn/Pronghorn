package com.ociweb.pronghorn.ring.util.build;

import java.io.IOException;

public class SecretByteSplitter {

    
    public static <T extends Appendable> T scramble(CharSequence source, T target) {
        
        int i = source.length();
        while (--i>=0) {
            int x = (int)source.charAt(i);
            if ( x<0 | x>127 ) {
                throw new UnsupportedOperationException();
            }
            //we know that x is a small value 7 bit number            
            try {
                target.append((char)(48+(0xF&(x>>4))));
                target.append((char)(48+(0xF&x) ));                
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return target;
    }
    
    public static <T extends Appendable> T assemble(CharSequence source, T target) {
        int i = source.length();
        while (--i>=0) {
            int low = source.charAt(i);
            int high= source.charAt(--i);
            
            try {
                target.append((char) ( ((high-48)<<4) | (low-48) )  );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }  
        }
        
        return target;
    }
    
    
}
