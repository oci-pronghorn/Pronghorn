package com.ociweb.json.encode;

import com.ociweb.json.appendable.StringOut;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class JSONRootIntegerTests {
    private StringOut out;

    @Before
    public void init() {
        out = new StringOut();
    }

    @Test
    public void testRootInteger() {
        JSONRenderer<Integer> json = new JSONRenderer<Integer>()
                .integer(o -> o);
        json.render(out, 9);
        assertEquals("9", out.toString());
    }

    @Test
    public void testRootIntegerNul_lNull() {
        JSONRenderer<Integer> json = new JSONRenderer<Integer>()
                .nullableInteger((o, v) -> v.visit(o != null ? o : 0, o == null));
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testRootIntegerNull_Value() {
        JSONRenderer<Integer> json = new JSONRenderer<Integer>()
                .nullableInteger((o, v) -> v.visit(o != null ? o : 0, o == null));
        json.render(out, 9);
        assertEquals("9", out.toString());
    }
}
