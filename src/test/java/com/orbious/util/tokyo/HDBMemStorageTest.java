package com.orbious.util.tokyo;

import com.orbious.util.tokyo.HDBMemStorage;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import java.io.File;
import com.orbious.util.Loggers;
import com.orbious.util.config.Config;
import com.orbious.util.config.IConfig;

public class HDBMemStorageTest {

  @Before
  public void before() throws Exception {
    Config.setDefaults(TestConfig.class);
    Loggers.init();
  }

	@Test
	public void type() throws Exception {
		assertThat(HDBMemStorage.class, notNullValue());
	}

	@Test
	public void init() throws Exception {
	  File f;
	  HDBMemStorage<String, Integer> hdbm;
	  Integer actual;

	  f = File.createTempFile("HDBMemStorageTest", ".hdb");
	  hdbm = new HDBMemStorage<String, Integer>(f, String.class, Integer.class, false);

	  hdbm.open();
	  for ( int i = 0; i < 10; i++ ) {
	    hdbm.put(Integer.toString(i), i);
	  }
	  hdbm.close();

	  hdbm.open();
	  for ( int i = 0; i < 10; i++ ) {
	    actual = hdbm.get(Integer.toString(i));
	    assertThat(actual, is(equalTo(new Integer(i))));
	  }

	}

  enum TestConfig implements IConfig {
    app_version("5.0"),
    log_realm("sentence-extractor"),
    log_config("com/orbious/util/tokyo/log4j.xml");

    private String svalue = null;
    private int ivalue = -1;
    private float fvalue = Float.NaN;
    private double dvalue = Double.NaN;
    private boolean bvalue = false;

    private TestConfig(String value) { this.svalue = value; }
    private TestConfig(int value) { this.ivalue = value; }
    private TestConfig(double value) { this.dvalue = value; }
    private TestConfig(boolean value) { this.bvalue = value; }

    public boolean isString() { return (svalue != null) ? true : false; }
    public String asString() { return svalue; }

    public boolean isInt() { return (ivalue != -1) ? true : false; }
    public int asInt() { return ivalue; }

    public boolean isFloat() { return (fvalue != Float.NaN) ? true : false; }
    public float asFloat() { return fvalue; }

    public boolean isDouble() { return (dvalue != Double.NaN) ? true : false; }
    public double asDouble() { return dvalue; }

    public boolean isBool() { return true; }
    public boolean asBool() { return bvalue; }

    public String getName() { return this.name(); }
  }
}

