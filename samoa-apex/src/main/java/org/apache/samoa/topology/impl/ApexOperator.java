package org.apache.samoa.topology.impl;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.codec.DefaultStatefulStreamCodec;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.google.common.collect.Maps;

@DefaultSerializer(JavaSerializer.class)
public class ApexOperator extends BaseOperator implements Serializable {

  private static final long serialVersionUID = -6637673741263199198L;
  public final Processor processor;
  public int instances = 1; // Default
  
  public boolean[] usedInputPorts = new boolean[]{false, false, false, false, false};
  public boolean[] usedOutputPorts = new boolean[]{false, false, false, false, false};

  public ApexOperator()
  {
    processor = null;
  }

  @InputPortFieldAnnotation(optional=true)
  public DefaultInputPortSerializable<ContentEvent> inputPort0 = new DefaultInputPortSerializable<ContentEvent>() {
    @Override
    public void process(ContentEvent tuple) {
      processor.process(tuple);
    }
  };
  @InputPortFieldAnnotation(optional=true)
  public DefaultInputPortSerializable<ContentEvent> inputPort1 = new DefaultInputPortSerializable<ContentEvent>() {
    @Override
    public void process(ContentEvent tuple) {
      processor.process(tuple);
    }
  };
  @InputPortFieldAnnotation(optional=true)
  public DefaultInputPortSerializable<ContentEvent> inputPort2 = new DefaultInputPortSerializable<ContentEvent>() {
    @Override
    public void process(ContentEvent tuple) {
      processor.process(tuple);
    }
  };
  @InputPortFieldAnnotation(optional=true)
  public DefaultInputPortSerializable<ContentEvent> inputPort3 = new DefaultInputPortSerializable<ContentEvent>() {
    @Override
    public void process(ContentEvent tuple) {
      processor.process(tuple);
    }
  };
  @InputPortFieldAnnotation(optional=true)
  public DefaultInputPortSerializable<ContentEvent> inputPort4 = new DefaultInputPortSerializable<ContentEvent>() {
    @Override
    public void process(ContentEvent tuple) {
      processor.process(tuple);
    }
  };

  @OutputPortFieldAnnotation(optional=true)
  public DefaultOutputPortSerializable<ContentEvent> outputPort0 = new DefaultOutputPortSerializable<ContentEvent>();
  @OutputPortFieldAnnotation(optional=true)
  public DefaultOutputPortSerializable<ContentEvent> outputPort1 = new DefaultOutputPortSerializable<ContentEvent>();
  @OutputPortFieldAnnotation(optional=true)
  public DefaultOutputPortSerializable<ContentEvent> outputPort2 = new DefaultOutputPortSerializable<ContentEvent>();
  @OutputPortFieldAnnotation(optional=true)
  public DefaultOutputPortSerializable<ContentEvent> outputPort3 = new DefaultOutputPortSerializable<ContentEvent>();
  @OutputPortFieldAnnotation(optional=true)
  public DefaultOutputPortSerializable<ContentEvent> outputPort4 = new DefaultOutputPortSerializable<ContentEvent>();

  ApexOperator(Processor processor, int parallelismHint) {
    this.processor = processor;
    this.instances = parallelismHint;
  }
  
  @Override
  public void setup(OperatorContext context)
  {
    processor.onCreate(context.getId());
  }
  
  public ApexStream createStream(String id) {
    ApexStream stream = new ApexStream(id);
    if(!usedOutputPorts[0]) {
      stream.outputPort = outputPort0;
      usedOutputPorts[0] = true;
    }
    else if(!usedOutputPorts[1]) {
      stream.outputPort = outputPort1;
      usedOutputPorts[1] = true;
    }
    else if(!usedOutputPorts[2]) {
      stream.outputPort = outputPort2;
      usedOutputPorts[2] = true;
    }
    else if(!usedOutputPorts[3]) {
      stream.outputPort = outputPort3;
      usedOutputPorts[3] = true;
    }
    else if(!usedOutputPorts[4]) {
      stream.outputPort = outputPort4;
      usedOutputPorts[4] = true;
    }
    else {
      throw new RuntimeException("Need more input ports for ApexOperator");
    }
    return stream;
  }

  public void addInputStream(ApexStream stream) {
    if(!usedInputPorts[0]) {
      stream.inputPort = inputPort0;
      usedInputPorts[0] = true;
    }
    else if(!usedInputPorts[1]) {
      stream.inputPort = inputPort1;
      usedInputPorts[1] = true;
    }
    else if(!usedInputPorts[2]) {
      stream.inputPort = inputPort2;
      usedInputPorts[2] = true;
    }
    else if(!usedInputPorts[3]) {
      stream.inputPort = inputPort3;
      usedInputPorts[3] = true;
    }
    else if(!usedInputPorts[4]) {
      stream.inputPort = inputPort4;
      usedInputPorts[4] = true;
    }
    else {
      throw new RuntimeException("Need more input ports for ApexOperator");
    }
  }
}
