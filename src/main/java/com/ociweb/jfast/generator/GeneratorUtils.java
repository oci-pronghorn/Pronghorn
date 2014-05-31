package com.ociweb.jfast.generator;

import java.io.IOException;
import java.util.Arrays;

import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTRingBuffer;

public class GeneratorUtils {

    public static void generateHead(SourceTemplates templates, byte[] origCatBytes, Appendable target, String name, String base) throws IOException {
        target.append("package "+FASTClassLoader.GENERATED_PACKAGE+";\n"); //package
        target.append("\n");
        target.append(templates.imports()); //imports
        target.append("\n");
        target.append("public final class "+name+" extends "+base+" {"); //open class
        target.append("\n");
        target.append("public static byte[] catBytes = new byte[]"+(Arrays.toString(origCatBytes).replace('[', '{').replace(']', '}'))+";\n"); //static constant
        target.append("\n");
        target.append("public "+name+"() {super(new TemplateCatalog(catBytes));}");//constructor
        target.append("\n");
    }

    public static int complexity(CharSequence seq) {
        int complexity = 0;
        int i = seq.length();
        while (--i>=0) {
            char c = seq.charAt(i);
            if ('.'==c || //deref 
                '['==c || //array ref
                '+'==c || //add
                '-'==c || //subtract
                '*'==c || //multiply
                '&'==c || //and
                '|'==c || //or
                '?'==c ) { //ternary 
                complexity++;
            }
        }
        return complexity;
    }

    public static void generateTail(Appendable target) throws IOException {
        target.append('}');
    }

}
