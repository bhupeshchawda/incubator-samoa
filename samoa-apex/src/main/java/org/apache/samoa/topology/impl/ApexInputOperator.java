package org.apache.samoa.topology.impl;

import java.io.Serializable;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.EntranceProcessor;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;

@DefaultSerializer(JavaSerializer.class)
public class ApexInputOperator extends BaseOperator implements InputOperator, Serializable {

  private final EntranceProcessor entranceProcessor;
  private final DefaultOutputPortSerializable<ContentEvent> outputPort = new DefaultOutputPortSerializable<ContentEvent>();

  public ApexInputOperator()
  {
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
  public void emitTuples() {
    if(entranceProcessor.hasNext()){
      outputPort.emit(entranceProcessor.nextEvent());
    }
  }
}
