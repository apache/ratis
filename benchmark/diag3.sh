#!/usr/bin/env bash
# ============================================================================
#  diag3.sh — ile z kosztu zestawienia polaczenia to podpis RSA?
#
#  PYTANIE: QUIC robi 1 RTT, TCP robi 2, oba podpisuja tym samym kluczem —
#  a mimo to polaczenie QUIC kosztuje 37.2 ms, a TCP 26.3 ms. To sie nie zgadza
#  z teoria. Podejrzenie: podpis RSA-4096 robia DWIE ROZNE biblioteki
#  (quiche-BoringSSL vs tcnative-BoringSSL 2.0.74) i moga miec rozny koszt.
#  Systemowy `openssl speed` (16.8 ms) nie mierzy zadnej z nich.
#
#  METODA: powtorzyc caly pomiar na certach ECDSA P-256, gdzie podpis kosztuje
#  ~0.07 ms zamiast ~17 ms. Kryptografia znika z rownania PO OBU STRONACH,
#  zostaje czysty koszt transportu.
#
#  CZYTANIE WYNIKU (koszt polaczenia = conn A p50 - conn B p50):
#    QUIC ~20, TCP ~10  -> narzut QUIC prawdziwy, szukamy dalej
#    QUIC ~5,  TCP ~10  -> cala roznica byla w implementacji RSA, QUIC wygrywa
#    oba ~10            -> parytet, conn A bylo zdominowane przez krypto
#
#  Kod NIE jest zmieniany — podmieniane sa wylacznie pliki certow serwera.
#  CA zostaje bez zmian, wiec klient nie wymaga nowego zaufania.
#
#  Uzycie:  bash ~/diag3.sh        |  REPS=5 bash ~/diag3.sh
#  Wyniki:  ~/raft-results/diag3/
# ============================================================================
set -u

JAVA="${JAVA:-$HOME/jdk21/bin/java}"
JAR="${JAR:-$HOME/ratis.jar}"
QUICHE="${QUICHE:-$HOME/netty-quiche-linux.jar}"
CONF="${CONF:-$HOME/conf.properties}"
SSL="${SSL:-$HOME/ratis-test/src/test/resources/ssl}"
SERVERS="${SERVERS:-dcc-1 dcc-2 dcc-3}"
NSRV=$(echo $SERVERS | wc -w | tr -d ' ')
CLIENT="${CLIENT:-dcc-4}"
REPS="${REPS:-3}"
REQ="${REQ:-100}"
OUT="$HOME/raft-results/diag3"
CP="$JAR:$QUICHE"

mkdir -p "$OUT"
exec > >(tee "$OUT/diag3.log") 2>&1

hr()  { printf '\n============================================================\n%s\n============================================================\n' "$1"; }
med() { tr ' ' '\n' | grep -v '^$' | sort -n | awk '{a[NR]=$1} END{if(NR)printf "%.2f", a[int((NR+1)/2)]}'; }

# Certy MUSZA wrocic do RSA, nawet gdy skrypt padnie w polowie.
restore_rsa() {
  if [ -f "$SSL/server.crt.rsa" ]; then
    cp "$SSL/server.crt.rsa" "$SSL/server.crt"
    cp "$SSL/server.pem.rsa" "$SSL/server.pem"
    echo "[trap] certy przywrocone do RSA"
  fi
}
trap restore_rsa EXIT INT TERM

hr "0. PRZYGOTOWANIE CERTOW"
[ -f "$SSL/ca.key" ] || { echo "BRAK $SSL/ca.key — bez klucza CA nie podpisze certu EC"; exit 1; }

# kopia zapasowa oryginalow (tylko raz)
[ -f "$SSL/server.crt.rsa" ] || cp "$SSL/server.crt" "$SSL/server.crt.rsa"
[ -f "$SSL/server.pem.rsa" ] || cp "$SSL/server.pem" "$SSL/server.pem.rsa"

# cert EC podpisany istniejacym CA (CA zostaje RSA — weryfikacja lancucha jest tania)
if [ ! -f "$SSL/server.crt.ec" ]; then
  ( cd "$SSL" && \
    openssl ecparam -name prime256v1 -genkey -noout -out server-ec.key && \
    openssl req -new -key server-ec.key -out server-ec.csr -subj "/CN=localhost" && \
    openssl x509 -req -passin pass:1111 -days 3650 -in server-ec.csr \
      -CA ca.crt -CAkey ca.key -set_serial 02 -out server.crt.ec && \
    openssl pkcs8 -topk8 -nocrypt -in server-ec.key -out server.pem.ec ) >/dev/null 2>&1
fi
[ -f "$SSL/server.crt.ec" ] || { echo "nie udalo sie wygenerowac certu EC"; exit 1; }

echo "RSA: $(openssl x509 -in "$SSL/server.crt.rsa" -noout -text | grep -m1 'Public-Key')"
echo "EC : $(openssl x509 -in "$SSL/server.crt.ec"  -noout -text | grep -m1 'Public-Key') $(openssl x509 -in "$SSL/server.crt.ec" -noout -text | grep -m1 'NIST CURVE')"
echo "lancuch EC: $(openssl verify -CAfile "$SSL/ca.crt" "$SSL/server.crt.ec" 2>&1)"

hr "1. KOSZT PODPISU NA TYM WEZLE (systemowy openssl — punkt odniesienia)"
ssh -n dcc-1 "openssl speed -seconds 2 rsa4096 2>&1 | tail -2; echo; openssl speed -seconds 2 ecdsap256 2>&1 | tail -2"

install_certs() {   # $1 = rsa|ec
  cp "$SSL/server.crt.$1" "$SSL/server.crt"
  cp "$SSL/server.pem.$1" "$SSL/server.pem"
  echo "   certy serwera ustawione na: $1"
}

# ssh -f NIE moze trafic do podstawienia polecen — patrz RUNBOOK §9.
start_servers() {   # $1 = quic|netty
  local kind="$1" flag="" dir i=0
  [ "$kind" = quic ] && flag="--quic"
  dir="/data/$USER/raft/$kind"
  for n in $SERVERS; do ssh -n "$n" "pkill -u $USER -f CounterServer" >/dev/null 2>&1; done
  sleep 2
  for n in $SERVERS; do
    ssh -n "$n" "mkdir -p $dir && ln -sfn \$HOME/ratis-test $dir/ratis-test && rm -rf $dir/n*" >/dev/null 2>&1
  done
  for n in $SERVERS; do
    ssh -f "$n" "cd $dir && RATIS_EXAMPLE_CONF=$CONF \
      nohup sh -c 'sleep 86400 | $JAVA -cp $CP \
        org.apache.ratis.examples.counter.server.CounterServer $i $flag' \
      </dev/null > server$i.log 2>&1 &" >/dev/null 2>&1
    i=$((i+1))
  done
  local ready=0 waited=0
  while [ "$waited" -lt 40 ]; do
    ready=0
    for n in $SERVERS; do
      ssh -n "$n" "ss -lnp 2>/dev/null | grep -q 10024" >/dev/null 2>&1 && ready=$((ready+1))
    done
    [ "$ready" -ge "$NSRV" ] && break
    sleep 2; waited=$((waited+2))
  done
  echo "   porty gotowe po ${waited}s ($ready/$NSRV)"
  [ "$ready" -lt "$NSRV" ] && { echo "   >>> serwery nie wstaly, log:"; \
    ssh -n "$(echo $SERVERS | cut -d' ' -f1)" "head -20 $dir/server0.log"; return 1; }
  sleep 3
  return 0
}

measure() {   # $1 = quic|netty   $2 = A|B   $3 = rsa|ec
  local kind="$1" C="$2" cert="$3" tr="TCP_TLS" p50s="" p99s="" r log line p50 p99
  [ "$kind" = quic ] && tr="QUIC"
  for r in $(seq 1 "$REPS"); do
    log="$OUT/${cert}_${kind}_conn${C}_r${r}.log"
    ssh -n "$CLIENT" "cd \$HOME && RATIS_EXAMPLE_CONF=$CONF $JAVA \
      -cp $CP org.apache.ratis.examples.counter.client.RaftBench \
      --transport $tr --mode rywrites --payload 1kB --requests $REQ \
      --conn $C --clients 1:1:1" > "$log" 2>&1
    line=$(grep -E "^(QUIC|TCP_TLS)," "$log" | head -1)
    p50=$(echo "$line" | awk -F, '{print $12}')
    p99=$(echo "$line" | awk -F, '{print $13}')
    p50s="$p50s $p50"; p99s="$p99s $p99"
    printf '   [%s] %s conn%s przebieg %d: write p50=%-9s p99=%s\n' "$cert" "$kind" "$C" "$r" "${p50:-?}" "${p99:-?}"
  done
  local m; m=$(echo $p50s | med)
  printf '   >>> [%s] %s conn%s MEDIANA p50 = %s ms\n' "$cert" "$kind" "$C" "$m"
  echo "$cert,$kind,$C,$m,$(echo $p99s | med)" >> "$OUT/summary.csv"
}

echo "cert,transport,conn,write_p50_med,write_p99_med" > "$OUT/summary.csv"
echo "powtorzen: $REPS,  zadan na przebieg: $REQ"

for cert in rsa ec; do
  hr "CERTY: $(echo $cert | tr a-z A-Z)"
  install_certs "$cert"
  for kind in quic netty; do
    echo; echo "--- $kind ---"
    start_servers "$kind" || { echo "   pomijam $kind/$cert"; continue; }
    measure "$kind" A "$cert"
    measure "$kind" B "$cert"
  done
done

for n in $SERVERS; do ssh -n "$n" "pkill -u $USER -f CounterServer" >/dev/null 2>&1; done

hr "PODSUMOWANIE"
column -t -s, "$OUT/summary.csv" 2>/dev/null || cat "$OUT/summary.csv"

hr "KOSZT ZESTAWIENIA POLACZENIA  (conn A - conn B)"
awk -F, 'NR>1 {v[$1"_"$2"_"$3]=$4}
END {
  printf "%-6s %-7s %12s %12s %14s\n", "cert", "transp", "connA_p50", "connB_p50", "koszt_pol.";
  split("rsa ec", CS, " "); split("quic netty", TS, " ");
  for (i=1;i<=2;i++) for (j=1;j<=2;j++) {
    a=v[CS[i]"_"TS[j]"_A"]; b=v[CS[i]"_"TS[j]"_B"];
    if (a!="" && b!="") printf "%-6s %-7s %12s %12s %14.2f\n", CS[i], TS[j], a, b, a-b;
  }
}' "$OUT/summary.csv"

cat <<'EOF'

Jak to czytac (kolumna koszt_pol.):
  * roznica rsa -> ec pokazuje, ile REALNIE kosztowal podpis w DANEJ bibliotece.
    Systemowy openssl mowil 16.8 ms, ale QUIC uzywa BoringSSL z quiche,
    a Netty BoringSSL z tcnative — to dwa rozne buildy.
  * jesli spadek u QUIC >> spadek u Netty  -> quiche podpisuje wolniej,
    i to ONO tlumaczy przewage TCP w conn A, a nie sam protokol.
  * jesli spadki podobne, a QUIC dalej drozszy -> narzut QUIC jest prawdziwy,
    szukamy go poza kryptografia.
  * na certach EC teoria mowi: QUIC (1 RTT) powinien byc TANSZY od TCP (2 RTT).
    Jesli nie jest — mamy twardy dowod na blad implementacji.
EOF

echo; echo "gotowe: $OUT"
