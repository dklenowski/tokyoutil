package com.orbious.util.tokyo;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import java.io.File;
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

    for ( int i = 0; i < 1000; i++ ) {
      hdb.put(Bytes.strToBytes(Integer.toString(i)), Bytes.convert(i, Integer.class));
    }
    hdb.put(Bytes.intToBytes(10), Bytes.intToBytes(100));
    hdb.close();
  }

  @Test
  public void type() throws Exception {
    assertThat(HDBLRUMap.class, notNullValue());
  }

  @Test
  public void writer() throws Exception {
    File f;
    HDBLRUMap<String, Integer> map;

    f = File.createTempFile("HDBLRUMapTest", ".hdb");
    map = new HDBLRUMap<String, Integer>(f, String.class, Integer.class, 10, 10, false);
    map.load();

    for ( int i = 0; i < 100; i++ ) {
      map.put(Integer.toString(i), i);
    }
    map.close();

    map = new HDBLRUMap<String, Integer>(f, String.class, Integer.class, 10, 10, true);
    map.load();

    for ( int i = 0; i < 100; i++ ) {
      assertThat(map.get(Integer.toString(i)), notNullValue());
    }
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
      for ( int j = 0; j < 10; j++ ) {
        a[j] = j*i;
      }

      map.put(a, (short)i);
    }

    for ( int i = 0; i < 100; i++ ) {
      a = new int[10];
      for ( int j = 0; j < 10; j++ ) {
        a[j] = j*i;
      }

      assertThat(map.get(a), is(equalTo((short)i)));
    }

    map.close();

    map = new HDBLRUMap<int[], Short>(f, int[].class, Short.class, 10, 10, true);
    map.load();

    for ( int i = 1; i < 100; i++ ) {
      a = new int[10];
      for ( int j = 0; j < 10; j++ ) {
        a[j] = j*i;
      }

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

    for ( int i = 0; i < 100; i++ ) {
      assertThat(map.get(Integer.toString(i)), notNullValue());
    }
    map.close();
  }
}
