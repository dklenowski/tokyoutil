package com.orbious.util.tokyo;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import com.orbious.util.apache.LRURndMapTest;

@RunWith(Suite.class)
@SuiteClasses({
  DBFieldsTest.class,
  FDBFileTest.class,
  HDBFileTest.class,
  HDBLRUMapTest.class,
  HDBStorageTest.class,
  HDBMemStorageTest.class,
  LRURndMapTest.class
})

public class AllTokyoTests {
  public AllTokyoTests() { }
}
