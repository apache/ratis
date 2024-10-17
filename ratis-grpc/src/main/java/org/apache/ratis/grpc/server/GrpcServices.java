/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.grpc.server;

import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;

import java.util.EnumSet;

/** The gRPC services extending {@link RaftServerRpc}. */
public interface GrpcServices extends RaftServerRpc {
  /** The type of the services. */
  enum Type {ADMIN, CLIENT, SERVER}

  /**
   * To customize the services.
   * For example, add a custom service.
   */
  interface Customizer {
    /** The default NOOP {@link Customizer}. */
    class Default implements Customizer {
      private static final Default INSTANCE = new Default();

      @Override
      public NettyServerBuilder customize(NettyServerBuilder builder, EnumSet<GrpcServices.Type> types) {
        return builder;
      }
    }

    static Customizer getDefaultInstance() {
      return Default.INSTANCE;
    }

    /**
     * Customize the given builder for the given types.
     *
     * @return the customized builder.
     */
    NettyServerBuilder customize(NettyServerBuilder builder, EnumSet<GrpcServices.Type> types);
  }
}
