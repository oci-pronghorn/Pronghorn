package com.ociweb.json.encode;

import com.ociweb.json.appendable.StringBuilderWriter;
import com.ociweb.json.encode.function.IterMemberFunction;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JSONRecursiveTests {
    private StringBuilderWriter out;

    @Before
    public void init() {
        out = new StringBuilderWriter();
    }

    @Test
    public void testRecurseObjectMember() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .beginObject()
                    .integer("i", o->o.i)
                    .recurseRoot("m", o->o.m)
                .endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj(new BasicObj(66)));
        assertEquals("{\"i\":9,\"m\":{\"i\":66,\"m\":null}}", out.toString());
    }

    @Test
    public void testRecurseObjectMember_Null() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .beginObject()
                    .integer("i", o->o.i)
                    .recurseRoot("m", o->o.m)
                .endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj(null));
        assertEquals("{\"i\":9,\"m\":null}", out.toString());
    }

    @Test
    public void testRecurseRenderer_NoOp() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
            .recurseRoot(o->o);
        assertTrue(json.isLocked());
        json.render(out, new BasicObj(null));
        assertEquals("", out.toString());
    }
}
