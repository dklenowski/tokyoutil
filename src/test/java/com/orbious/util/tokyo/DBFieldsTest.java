package com.orbious.util.tokyo;

import com.orbious.util.tokyo.DBFields;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import java.util.HashMap;
import com.orbious.util.Bytes;
import com.orbious.util.Loggers;

public class DBFieldsTest {

  @Before
  public void before() throws Exception {
    Loggers.init();
  }

	@Test
	public void type() throws Exception {
		assertThat(DBFields.class, notNullValue());
	}

	@Test
	public void set() throws Exception {
	  DBFields fields;
	  String key;
	  String key1;
	  byte[] b;

	  fields = new DBFields();
	  key = "key";
	  key1 = "key1";

	  fields.set(key);
	  fields.set(key1);

	  b = Bytes.strToBytes(key);
	  assertTrue(fields.contains(b));

	  b = Bytes.strToBytes(key1);
    assertTrue(fields.contains(b));
	}

	@Test
	public void set2() throws Exception {
	  DBFields fields;
	  String key;
	  String key1;
	  byte[] b;

	  fields = new DBFields();
	  key = "key";
	  key1 = "key1";

	  fields.set(key, "val");
	  fields.set(key1, "val1");

	  b = Bytes.strToBytes(key);
	  assertTrue(fields.contains(b));

	  b = Bytes.strToBytes(key1);
	  assertTrue(fields.contains(b));
	}

  @Test
  public void entries() throws Exception {
    DBFields fields;
    String key, val;
    String key1, val1;
    DBFields.KeyBytes kb;
    byte[] b;
    HashMap<DBFields.KeyBytes, byte[]> hm;

    fields = new DBFields();
    key = "key";
    val = "val";

    key1 = "key1";
    val1 = "val1";

    fields.set(key, val);
    fields.set(key1, val1);

    hm = fields.entries();
    assertThat(hm.size(), is(equalTo(2)));

    kb = fields.new KeyBytes(Bytes.strToBytes(key));
    b = Bytes.strToBytes(val);
    assertThat(hm.get(kb), is(equalTo(b)));

    kb = fields.new KeyBytes(Bytes.strToBytes(key1));
    b = Bytes.strToBytes(val1);
    assertThat(hm.get(kb), is(equalTo(b)));
  }

}
