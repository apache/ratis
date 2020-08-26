package org.apache.ratis.server.protocol;

import java.io.IOException;
import org.apache.ratis.proto.RaftProtos.PauseUnpauseRequestProto;
import org.apache.ratis.proto.RaftProtos.PauseUnpauseReplyProto;
import org.apache.ratis.protocol.RaftClientReply;

public interface RaftServerPauseUnpauseProtocol {

  RaftClientReply requestPauseUnpause(PauseUnpauseRequestProto request) throws IOException;

}
