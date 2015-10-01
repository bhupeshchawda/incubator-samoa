package org.apache.samoa.topology.impl;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

public class ApexTask implements StreamingApplication{

	@Override
	public void populateDAG(DAG dag, Configuration conf) {
		ApexTopology apexTopo = new ApexTopology(dag, "Test");
		dag = apexTopo.getDAG();
	}

}
