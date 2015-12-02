package com.ociweb.pronghorn.code;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.util.Appendables;

public class Literal extends Code implements SingleResult {

    private final CharSequence literal;
    
    public Literal(CharSequence literal) {
        super(null,1,true);//IdGen is not used.
        this.literal = literal;
    }
    
    public Literal(int literal) throws IOException {
        super(null);//IdGen is not used.
        this.literal = Appendables.appendValue(new StringBuilder(), literal);
    }

    @Override
    protected void upFrontDefinition(Appendable target) throws IOException {
        //Nothing should be done for this.
    }

    @Override
    protected void singleResult(Appendable target) throws IOException {
       target.append(literal);
    }
    
}
