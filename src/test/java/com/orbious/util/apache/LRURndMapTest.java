package com.orbious.util.apache;

import com.orbious.util.Loggers;
import com.orbious.util.tokyo.apache.LRURndMap;
import static org.junit.Assert.*;
import java.util.Random;
import org.junit.Test;

public class LRURndMapTest {

  public LRURndMapTest() {
    Loggers.init();
  }

  @Test
  public void random() {
    LRURndMap<Integer, String> map = new LRURndMap<Integer, String>();
    for ( int i = 0; i < 100; i++ ) {
      if ( i%2 == 0 ) {
        map.put(i, String.valueOf(i));
      }

    }

    int j;
    Random rnd = new Random();
    for ( int i = 0; i < 200; i++ ) {
      j = map.random(rnd);
      if ( (j < 0) || (j > 100) )
        fail("Invalid j: 0 > " + j + " > 100");
    }
  }
}
