package org.apache.samoa.topology.impl;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;

public class ApexTask implements StreamingApplication{

	LogicalPlan dag;

	public ApexTask(ApexTopology apexTopo) {
		this.dag = (LogicalPlan) apexTopo.getDAG();
		System.out.println("Dag Set in Apex Task" + this.dag);
	}

	@Override
	public void populateDAG(DAG dag, Configuration arg1) {
		for(OperatorMeta o: this.dag.getAllOperators()){

			dag.addOperator(o.getName(), o.getOperator());
		}
		for(StreamMeta s: this.dag.getAllStreams()) {
			for(InputPortMeta i: s.getSinks()) {
				Operator.OutputPort<Object> op = (OutputPort<Object>) s.getSource().getPortObject();
				Operator.InputPort<Object> ip = (InputPort<Object>) i.getPortObject();
				dag.addStream(s.getName(), op, ip);
			}
		}
		System.out.println("Dag Set in Populate DAG" + dag);
	}
}
