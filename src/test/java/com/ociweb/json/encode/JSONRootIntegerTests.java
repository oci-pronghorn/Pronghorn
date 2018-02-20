package com.ociweb.json.encode;

import com.ociweb.json.appendable.StringBuilderWriter;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JSONRootIntegerTests {
    private StringBuilderWriter out;

    @Before
    public void init() {
        out = new StringBuilderWriter();
    }

    @Test
    public void testRootInteger() {
        JSONRenderer<Integer> json = new JSONRenderer<Integer>()
                .integer(o -> o);
        assertTrue(json.isLocked());
        json.render(out, 9);
        assertEquals("9", out.toString());
    }

    @Test
    public void testRootIntegerNul_lNull() {
        JSONRenderer<Integer> json = new JSONRenderer<Integer>()
                .nullableInteger((o, v) -> v.visit(o != null ? o : 0, o == null));
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testRootIntegerNull_Value() {
        JSONRenderer<Integer> json = new JSONRenderer<Integer>()
                .nullableInteger((o, v) -> v.visit(o != null ? o : 0, o == null));
        assertTrue(json.isLocked());
        json.render(out, 9);
        assertEquals("9", out.toString());
    }
}
