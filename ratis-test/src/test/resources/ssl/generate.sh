# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

MY_PASSWORD="myPass1234"

insert_asf_header() {
  local FILE="$1"
  local TMP=${FILE}.tmp
  cat asf_header.txt "${FILE}" > "${TMP}"
  mv "${TMP}" "${FILE}"
}

rm ca.* client.* server.*jt

LOCALHOST_CNF=localhost.cnf
set -ex

# generate ca files
CA_KEY="ca.key"
echo Generate CA key: ${CA_KEY}
openssl genrsa -passout pass:${MY_PASSWORD} -aes256 -out ${CA_KEY} 4096
insert_asf_header ${CA_KEY}

CA_CRT="ca.crt"
echo Generate CA certificate: ${CA_CRT}
openssl req -passin pass:${MY_PASSWORD} -new -x509 -days 3650 -key ${CA_KEY} -out ${CA_CRT} -subj "/CN=Ratis Testing CA"
insert_asf_header ${CA_CRT}

# generate server files
SERVER_KEY="server.key"
echo Generate server key: ${SERVER_KEY}
openssl genrsa -passout pass:${MY_PASSWORD} -aes256 -out ${SERVER_KEY} 4096

SERVER_CSR="server.csr"
echo Generate server Certificate Signing Request: ${SERVER_CSR}
openssl req -passin pass:${MY_PASSWORD} -new -key ${SERVER_KEY} -config ${LOCALHOST_CNF} -out ${SERVER_CSR}
insert_asf_header ${SERVER_CSR}

SERVER_CRT="server.crt"
echo Sign server certificate: ${SERVER_CRT}
openssl x509 -req -passin pass:${MY_PASSWORD} -days 3650 -in ${SERVER_CSR}  \
  -CA ${CA_CRT} -CAkey ${CA_KEY} -set_serial 01 -out ${SERVER_CRT} \
  -extfile ${LOCALHOST_CNF} -extensions req_ext
insert_asf_header ${SERVER_CRT}

echo Remove passphrase from server key: ${SERVER_KEY}
openssl rsa -passin pass:${MY_PASSWORD} -in ${SERVER_KEY} -out ${SERVER_KEY}
insert_asf_header ${SERVER_KEY}

SERVER_PEM="server.pem"
echo Convert server private key to PEM file: ${SERVER_PEM}
openssl pkcs8 -topk8 -nocrypt -in ${SERVER_KEY} -out ${SERVER_PEM}
insert_asf_header ${SERVER_PEM}

# generate client files
CLIENT_KEY=client.key
echo Generate client key: ${CLIENT_KEY}
openssl genrsa -passout pass:${MY_PASSWORD} -aes256 -out ${CLIENT_KEY} 4096

CLIENT_CSR="client.csr"
echo Generate server Certificate Signing Request: ${CLIENT_CSR}
openssl req -passin pass:${MY_PASSWORD} -new -key ${CLIENT_KEY} -config ${LOCALHOST_CNF} -out ${CLIENT_CSR}
insert_asf_header ${CLIENT_CSR}

CLIENT_CRT="client.crt"
echo Sign client certificate: ${CLIENT_CRT}
openssl x509 -passin pass:${MY_PASSWORD} -req -days 3650 -in ${CLIENT_CSR} \
  -CA ${CA_CRT} -CAkey ${CA_KEY} -set_serial 01 -out ${CLIENT_CRT}
insert_asf_header ${CLIENT_CRT}

echo Remove passphrase from client key: ${CLIENT_KEY}
openssl rsa -passin pass:${MY_PASSWORD} -in ${CLIENT_KEY} -out ${CLIENT_KEY}
insert_asf_header ${CLIENT_KEY}

CLIENT_PEM="client.pem"
echo Convert client private key to PEM file: ${CLIENT_PEM}
openssl pkcs8 -topk8 -nocrypt -in ${CLIENT_KEY} -out ${CLIENT_PEM}
insert_asf_header ${CLIENT_PEM}
