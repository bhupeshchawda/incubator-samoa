package org.apache.samoa.topology.impl;

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

import java.io.Serializable;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.EntranceProcessor;

import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

@DefaultSerializer(JavaSerializer.class)
public class ApexInputOperator extends BaseOperator implements InputOperator, Serializable {

  private static final long NUM_TUPLES_IN_WINDOW = 1000;
  private static final long serialVersionUID = 4255026962166445721L;
  private final EntranceProcessor entranceProcessor;
  private final DefaultOutputPortSerializable<ContentEvent> outputPort = new DefaultOutputPortSerializable<ContentEvent>();
  private int numTuples;

  public ApexInputOperator() {
    entranceProcessor = null;
  }

  ApexInputOperator(EntranceProcessor processor) {
    this.entranceProcessor = processor;
  }

  ApexStream createStream(String piId) {
    ApexStream stream = new ApexStream(piId);
    stream.outputPort = this.outputPort;
    return stream;
  }

  @Override
  public void setup(OperatorContext context) {
    this.entranceProcessor.onCreate(context.getId());
  }

  @Override
  public void beginWindow(long windowId) {
    super.beginWindow(windowId);
    numTuples = 0;
  }

  @Override
  public void emitTuples() {
    if (entranceProcessor.hasNext() && numTuples < NUM_TUPLES_IN_WINDOW) {
      outputPort.emit(entranceProcessor.nextEvent());
      numTuples++;
    }
  }
}
