package com.ociweb.json.encode;

import org.junit.Before;
import org.junit.Test;

import com.ociweb.pronghorn.util.StringBuilderWriter;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JSONRootStringTests {
    private StringBuilderWriter out;

    @Before
    public void init() {
        out = new StringBuilderWriter();
    }

    @Test
    public void testRootString() {
        JSONRenderer<String> json = new JSONRenderer<String>()
                .string((o,t) -> t.append(o));
        assertTrue(json.isLocked());
        json.render(out, "Hello");
        assertEquals("\"Hello\"", out.toString());
    }

    @Test
    public void testRootStringrNull_Null() {
        JSONRenderer<String> json = new JSONRenderer<String>()
                .nullableString((o,t) -> t.append(o));
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testRootStringrNull_Value() {
        JSONRenderer<String> json = new JSONRenderer<String>()
                .nullableString((o,t) -> t.append(o));
        assertTrue(json.isLocked());
        json.render(out, "Hello");
        assertEquals("\"Hello\"", out.toString());
    }
}
