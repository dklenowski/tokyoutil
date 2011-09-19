package com.orbious.util.tokyo;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
  FDBFileTest.class,
  HDBFileTest.class,
  HDBLRUMapTest.class,
  HDBStorageTest.class
})

public class AllTokyoTests {
  public AllTokyoTests() { }
}
