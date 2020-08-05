package org.apache.ratis.netty;

import org.apache.ratis.client.ClientStreamApi;
import org.apache.ratis.client.StreamClientFactory;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedStreamType;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.DataStreamServer;
import org.apache.ratis.server.impl.ServerStreamFactory;

public class NettyDataStreamFactory implements ServerStreamFactory, StreamClientFactory {
  public NettyDataStreamFactory(Parameters parameters){}

  @Override
  public SupportedStreamType getStreamType() {
    return SupportedStreamType.NETTY;
  }

  @Override
  public ClientStreamApi newClientStreamApi(ClientId clientId, RaftProperties properties) {
    return null;
  }

  @Override
  public DataStreamServer newServerStreamApi(RaftServer server) {
    return null;
  }
}
