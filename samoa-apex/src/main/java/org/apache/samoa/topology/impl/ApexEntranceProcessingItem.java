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

import java.util.Map;
import java.util.UUID;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.topology.AbstractEntranceProcessingItem;
import org.apache.samoa.topology.EntranceProcessingItem;
import org.apache.samoa.topology.Stream;

/**
 * EntranceProcessingItem implementation for Storm.
 */
class ApexEntranceProcessingItem extends AbstractEntranceProcessingItem implements ApexTopologyNode {
	private final ApexEntranceOperator piOperator;

	ApexEntranceProcessingItem(EntranceProcessor processor) {
		this(processor, UUID.randomUUID().toString());
	}

	ApexEntranceProcessingItem(EntranceProcessor processor, String friendlyId) {
		super(processor);
		this.setName(friendlyId);
		this.piOperator = new ApexEntranceOperator(processor);
	}

	@Override
	public EntranceProcessingItem setOutputStream(Stream stream) {
		piOperator.setOutputStream((ApexStream) stream);
		return this;
	}

	@Override
	public Stream getOutputStream() {
		return piOperator.getOutputStream();
	}

	@Override
	public void addToTopology(ApexTopology topology, int parallelismHint) {
		topology.getDAG().addOperator(this.getName(), piOperator);
		//add num partitions
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

	/**
	 * Resulting Spout of StormEntranceProcessingItem
	 */
	final static class ApexEntranceOperator implements InputOperator {

		// private final Set<StormSpoutStream> streams;
		private final EntranceProcessor entranceProcessor;
		private ApexStream outputStream;

		private DefaultOutputPort<ContentEvent> collector = new DefaultOutputPort<ContentEvent>();

		ApexEntranceOperator(EntranceProcessor processor) {
			this.entranceProcessor = processor;
		}

		public ApexStream getOutputStream() {
			return outputStream;
		}

		public void setOutputStream(ApexStream stream) {
			this.outputStream = stream;
		}

		ApexStream createStream(String piId) {
			ApexStream stream = new ApexOperatorStream(piId);
			return stream;
		}

		@Override
		public void setup(OperatorContext context) {
			this.entranceProcessor.onCreate(context.getId());
		}

		@Override
		public void beginWindow(long arg0) {

		}

		@Override
		public void endWindow() {

		}

		@Override
		public void teardown() {

		}

		@Override
		public void emitTuples() {
			if(entranceProcessor.hasNext()){
				collector.emit(entranceProcessor.nextEvent());
			}
		}    
	}
}
