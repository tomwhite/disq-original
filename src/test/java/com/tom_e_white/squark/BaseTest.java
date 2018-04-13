package com.tom_e_white.squark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class BaseTest {
  protected static JavaSparkContext jsc;

  @BeforeClass
  public static void setup() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    jsc = new JavaSparkContext("local", "myapp", sparkConf);
  }

  @AfterClass
  public static void teardown() {
    jsc.stop();
  }
}
