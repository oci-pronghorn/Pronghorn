package com.ociweb.json.encode;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.util.StringBuilderWriter;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JSONSelectTests {
    private StringBuilderWriter out;

    @Before
    public void init() {
        out = new StringBuilderWriter();
    }

    @Test
    public void testNoCases_Declared() {
        JSONRenderer<Boolean> json = new JSONRenderer<Boolean>()
                .beginSelect()
                .endSelect();
        assertTrue(json.isLocked());
        json.render(out, true);
        assertEquals("", out.toString());
    }

    @Test
    public void testNoCases_AllFalse() {
        JSONRenderer<Boolean> json = new JSONRenderer<Boolean>()
                .beginSelect()
                    .tryCase(o->false).constantNull()
                    .tryCase(o->false).integer(o->6)
                .endSelect();
        assertTrue(json.isLocked());
        json.render(out, true);
        assertEquals("", out.toString());
    }

    @Test
    public void testCases_FirstTrue() {
        JSONRenderer<Boolean> json = new JSONRenderer<Boolean>()
                .beginSelect()
                    .tryCase(o->false).constantNull()
                    .tryCase(o->true).integer(o->6)
                    .tryCase(o->true).string((o,t)->t.append("hello"))
                .endSelect();
        assertTrue(json.isLocked());
        json.render(out, true);
        assertEquals("6", out.toString());
    }

    @Test
    public void testCases_Recursion_WithNothing() {
        JSONRenderer<Boolean> json = new JSONRenderer<Boolean>()
                .beginSelect()
                    .tryCase(o->o).recurseRoot(o->!o)
                .endSelect();
        assertTrue(json.isLocked());
        json.render(out, true);
        assertEquals("", out.toString());
    }

    @Test
    public void testCases_Recursion_WithSomething() {
        JSONRenderer<Boolean> json = new JSONRenderer<Boolean>()
                .beginObject()
                    .bool("value", o->o)
                    .beginSelect("recurse")
                        .tryCase(o->o).recurseRoot(o->!o)
                    .endSelect()
                .endObject();
        assertTrue(json.isLocked());
        json.render(out, true);
        assertEquals("{\"value\":true,\"recurse\":{\"value\":false}}", out.toString());
    }

    @Test
    @Ignore
    public void testCases_ArrayElementSelect() {
        JSONRenderer<Boolean[]> json = new JSONRenderer<Boolean[]>()
                .basicArray(o->o)
                    .beginSelect()
                        .tryCase((o,i)->o[i]).integer((o,i)->o[i]?42:43)
                        .tryCase((o,i)->!o[i]).string((o,i)->o[i]?"hello":"there")
                    .endSelect();
        assertTrue(json.isLocked());
        json.render(out, new Boolean[] {true, true, false, true});
        assertEquals("[42,42,\"there\",42]", out.toString());
    }
}
