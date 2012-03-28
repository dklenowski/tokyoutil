package com.orbious.util.tokyo.apache;

import java.util.Random;

import org.apache.commons.collections15.map.LRUMap;

public class LRURndMap<K, V> extends LRUMap<K, V> {

  private static final long serialVersionUID = 1L;

  public LRURndMap() {
    super();
  }

  public LRURndMap(int maxSize) {
    super(maxSize);
  }

  // this allows us to get a random key
  public K random(Random rnd) {
    if ( size() == 0 )
      return null;

    int i = rnd.nextInt(data.length);

    if ( data[i] != null )
      return data[i].getKey();

    boolean reverse = rnd.nextBoolean();
    if ( reverse ) {
      while ( data[i] == null ) {
        if ( i-1 >= 0 )
          i--;
        else
          break;
      }

      if ( data[i] != null )
        return data[i].getKey();
    }

    // try forward
    while ( data[i] == null ) {
      if ( i+1 < data.length )
        i++;
      else
        break;
    }

    if ( data[i] != null )
      return data[i].getKey();

    // fallback to reverse
    while ( data[i] == null ) {
      if ( i-1 >= 0 )
        i--;
      else
        break;
    }

    if ( data[i] != null )
      return data[i].getKey();

    // we should not get here..
    return null;
  }
}
