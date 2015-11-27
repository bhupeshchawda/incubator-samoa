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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.common.util.BaseOperator;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.topology.AbstractProcessingItem;
import org.apache.samoa.topology.ProcessingItem;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.impl.ApexStream.InputStreamId;
import org.apache.samoa.utils.PartitioningScheme;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * ProcessingItem implementation for Storm.
 * 
 * @author Arinto Murdopo
 * 
 */
class ApexProcessingItem extends AbstractProcessingItem implements ApexTopologyNode {
	private final ProcessingItemOperator piOperator;

	// TODO: should we put parallelism hint here?
	// imo, parallelism hint only declared when we add this PI in the topology
	// open for dicussion :p

	ApexProcessingItem(Processor processor, int parallelismHint) {
		this(processor, UUID.randomUUID().toString(), parallelismHint);
	}

	ApexProcessingItem(Processor processor, String friendlyId, int parallelismHint) {
		super(processor, parallelismHint);
		this.piOperator = new ProcessingItemOperator(processor);
		this.setName(friendlyId);
	}
	
	@Override
	protected ProcessingItem addInputStream(Stream inputStream, PartitioningScheme scheme) {
		ApexStream apexStream = (ApexStream) inputStream;
		InputStreamId inputId = apexStream.getInputId();
		return this;
	}

	@Override
	public void addToTopology(ApexTopology topology, int parallelismHint) {
		DAG dag = topology.getDAG();
		dag.addOperator(this.getName(), this.piOperator);
		// add num partitions
	}

	@Override
	public ApexStream createStream() {
		return piOperator.createStream(this.getName());
	}

	@Override
	public String getId() {
		return this.getName();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(super.toString());
		sb.insert(0, String.format("id: %s, ", this.getName()));
		return sb.toString();
	}

	private final static class ProcessingItemOperator extends BaseOperator {

		private static final long serialVersionUID = -6637673741263199198L;

		private final Set<ApexOperatorStream> streams;
		private final Processor processor;

		private DefaultOutputPort<ContentEvent> collector;

		ProcessingItemOperator(Processor processor) {
			this.streams = new HashSet<ApexOperatorStream>();
			this.processor = processor;
		}

		ApexStream createStream(String piId) {
			ApexOperatorStream stream = new ApexOperatorStream(piId);
			streams.add(stream);
			return stream;
		}
	}
}
