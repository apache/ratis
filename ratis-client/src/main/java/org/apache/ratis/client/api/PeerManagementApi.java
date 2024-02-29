package org.apache.ratis.client.api;

import org.apache.ratis.protocol.PeerInfoReply;

import java.io.IOException;

public interface PeerManagementApi {

  /** Get the information of current term,
   * last commit index, last applied index,
   * last snapshot index of the peer.*/
  PeerInfoReply info() throws IOException;

}
