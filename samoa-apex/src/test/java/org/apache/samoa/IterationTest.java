package org.apache.samoa;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2016 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
