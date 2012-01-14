package com.orbious.util.tokyo;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import java.io.File;
import java.util.Iterator;
import java.util.Map.Entry;
import org.junit.Test;
import com.orbious.util.Bytes;
import com.orbious.util.Loggers;
import tokyocabinet.HDB;

public class HDBLRUMapTest {

  public HDBLRUMapTest() {
    Loggers.init();
  }

  private void createTmpTokyoFile(File f) {
    HDB hdb;
    hdb = new HDB();
    hdb.open(f.toString(), HDB.OWRITER | HDB.OCREAT);

    for ( int i = 0; i < 1000; i++ )
      hdb.put(Bytes.strToBytes(Integer.toString(i)), Bytes.convert(i, Integer.class));

    hdb.put(Bytes.intToBytes(10), Bytes.intToBytes(100));
    hdb.close();
  }

  @Test
  public void writemultiple() throws Exception {
    File f;
    HDBLRUMap<String, Integer> map;

    f = File.createTempFile("HDBLRUMapTest", ".hdb");
    map = new HDBLRUMap<String, Integer>(f, String.class, Integer.class, 10, 10, false);
    map.load();

    map.put("a string", 1);
    map.writecfg("cfgstring", "a configstring");

    map.close();

    map = new HDBLRUMap<String, Integer>(f, String.class, Integer.class, 10, 10, true);
    map.load();

    assertThat(map.get("a string"), is(equalTo(1)));
    assertThat(map.readcfg("cfgstring"), is(equalTo("a configstring")));

    map.close();
  }

  @Test
  public void writer() throws Exception {
    File f;
    HDBLRUMap<String, Integer> map;

    f = File.createTempFile("HDBLRUMapTest", ".hdb");
    map = new HDBLRUMap<String, Integer>(f, String.class, Integer.class, 10, 10, false);
    map.load();

    for ( int i = 0; i < 100; i++ )
      map.put(Integer.toString(i), i);

    map.close();

    map = new HDBLRUMap<String, Integer>(f, String.class, Integer.class, 10, 10, true);
    map.load();

    for ( int i = 0; i < 100; i++ )
      assertThat(map.get(Integer.toString(i)), notNullValue());

    map.close();
  }

  @Test
  public void writer2() throws Exception {
    File f;
    HDBLRUMap<int[], Short> map;
    int[] a;

    f = File.createTempFile("HDBLRUMapTest", ".hdb");
    map = new HDBLRUMap<int[], Short>(f, int[].class, Short.class, 10, 10, false);
    map.load();

    for ( int i = 0; i < 100; i++ ) {
      a = new int[10];
      for ( int j = 0; j < 10; j++ ) a[j] = j*i;
      map.put(a, (short)i);
    }

    for ( int i = 0; i < 100; i++ ) {
      a = new int[10];
      for ( int j = 0; j < 10; j++ ) a[j] = j*i;
      assertThat(map.get(a), is(equalTo((short)i)));
    }

    map.close();

    map = new HDBLRUMap<int[], Short>(f, int[].class, Short.class, 10, 10, true);
    map.load();

    for ( int i = 1; i < 100; i++ ) {
      a = new int[10];
      for ( int j = 0; j < 10; j++ ) a[j] = j*i;
      assertThat(map.get(a), is(equalTo((short)i)));
    }
  }

  @Test
  public void reader() throws Exception {
    File f;
    HDBLRUMap<String, Integer> map;

    f = File.createTempFile("HDBLRUMapTest", ".hdb");
    createTmpTokyoFile(f);

    map = new HDBLRUMap<String, Integer>(f, String.class, Integer.class, 10, 10, true);
    map.load();

    for ( int i = 0; i < 100; i++ )
      assertThat(map.get(Integer.toString(i)), notNullValue());

    map.close();
  }

  @Test
  public void keys() throws Exception {
    File f;
    HDBLRUMap<String, Integer> map;

    f = File.createTempFile("HDBLRUMapTest", ".hdb");
    map = new HDBLRUMap<String, Integer>(f, String.class, Integer.class, 10, 10, false);
    map.load();

    map.put("a string", 1);
    map.writecfg("cfgstring", "a configstring");

    assertThat(map.keys(new String[] { "cfgstring" }).size(), is(equalTo(1)));

    map.close();

    map = new HDBLRUMap<String, Integer>(f, String.class, Integer.class, 10, 10, true);
    map.load();

    assertThat(map.keys(new String[] { "cfgstring" }).size(), is(equalTo(1)));

    map.close();
  }

  // NOTE for some reason map.keySet().iterator() throws a
  // ConcurrentModificationException whenever it is used?
  @Test
  public void iterator() throws Exception {
    File f = File.createTempFile("HDBLRUMapTest", ".hdb");

    HDBLRUMap<Integer, Integer> map;
    map = new HDBLRUMap<Integer, Integer>(f, Integer.class, Integer.class, 10, 10, false);
    map.load();

    for ( int i = 0; i < 1000; i++ ) {
      map.put(i, i);
    }

    Iterator<Entry<Integer, Integer>> it = map.entrySet().iterator();
    Entry<Integer, Integer> e;
    while ( it.hasNext() ) {
      e = it.next();
      assertThat(e.getKey(), is(equalTo(e.getValue())));
    }

    map.close();
  }
}
