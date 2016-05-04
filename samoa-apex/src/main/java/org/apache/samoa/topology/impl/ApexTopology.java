package org.apache.samoa.topology.impl;

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

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

import org.apache.samoa.topology.IProcessingItem;
import org.apache.samoa.topology.AbstractTopology;
import org.apache.samoa.topology.Stream;

/**
 * Adaptation of SAMOA topology in samoa-storm
 * 
 * @author Arinto Murdopo
 * 
 */
public class ApexTopology extends AbstractTopology {

	private DAG dag;

	public ApexTopology(DAG dag, String name){
		super(name);
		this.dag = dag;
	}
	protected ApexTopology(String name) {
		super(name);
		dag = new LogicalPlan();
	}

	@Override
	public void addProcessingItem(IProcessingItem procItem, int parallelismHint) {

	    ApexTopologyNode apexNode = (ApexTopologyNode) procItem;
	    apexNode.addToTopology(this, parallelismHint);
	    super.addProcessingItem(procItem, parallelismHint);
	}

	public DAG getDAG() {
		return dag;
	}
}
