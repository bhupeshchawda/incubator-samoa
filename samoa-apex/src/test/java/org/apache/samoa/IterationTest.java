package org.apache.samoa;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.LocalMode;

public class IterationTest
{
  @Test
  public void testIteration()
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.loggers.level","com.datatorrent.*:DEBUG");

    try {
      lma.prepareDAG(new IterationExample(), conf);
    } catch (Exception e1) {
      e1.printStackTrace();
    }
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    lc.runAsync();
    try {
      Thread.sleep(100000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    lc.shutdown();

  }
}
