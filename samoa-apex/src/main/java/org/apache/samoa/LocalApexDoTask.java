package org.apache.samoa;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2015 Apache Software Foundation
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import org.apache.samoa.topology.impl.ApexTask;
import org.apache.samoa.topology.impl.ApexTopology;

/**
 * The main class to execute a SAMOA task in LOCAL mode in Storm.
 * 
 * @author Arinto Murdopo
 * 
 */
public class LocalApexDoTask {

	private static final Logger logger = LoggerFactory.getLogger(LocalApexDoTask.class);

	/**
	 * The main method.
	 * 
	 * @param args
	 *          the arguments
	 */
	public static void main(String[] args) {

		LocalMode lma = LocalMode.newInstance();
		Configuration conf = new Configuration(false);

	    conf.set("com.datatorrent.apex.testParam","true");

	    try {
			lma.prepareDAG(new ApexTask(), conf);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	    
	    DAG dag = lma.getDAG();
	    ApexTopology topo = new ApexTopology(dag, "Dag");
	    
	    
	    
	    // Now validate dag and continue
	    LocalMode.Controller lc = lma.getController();
	    lc.setHeartbeatMonitoringEnabled(false);

	    lc.runAsync();

	}
}
