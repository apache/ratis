#!/usr/bin/env bash
# ============================================================================
#  diag.sh — jeden przebieg diagnostyczny dla luki conn A (QUIC 183ms vs TCP 26ms)
#
#  Uruchom na WEZLE DOSTEPOWYM (dcc), przy zaalokowanych wezlach dcc-1..dcc-4.
#  Sam restartuje serwery dla obu transportow. Nic nie zmienia w kodzie.
#
#  Uzycie:  bash ~/diag.sh
#  Wyniki:  ~/raft-results/diag/  (home przezyje wygaszenie wezlow)
# ============================================================================
set -u

JAVA="${JAVA:-$HOME/jdk21/bin/java}"
JAR="${JAR:-$HOME/ratis.jar}"
QUICHE="${QUICHE:-$HOME/netty-quiche-linux.jar}"
CONF="${CONF:-$HOME/conf.properties}"
SSL_DIR="${SSL_DIR:-$HOME/ratis-test/src/test/resources/ssl}"
SERVERS="${SERVERS:-dcc-1 dcc-2 dcc-3}"
NSRV=$(echo $SERVERS | wc -w | tr -d ' ')
CLIENT="${CLIENT:-dcc-4}"
OUT="$HOME/raft-results/diag"
CP="$JAR:$QUICHE"

mkdir -p "$OUT"
exec > >(tee "$OUT/diag.log") 2>&1

hr() { printf '\n============================================================\n%s\n============================================================\n' "$1"; }

hr "0. SANITY"
for f in "$JAVA" "$JAR" "$QUICHE" "$CONF"; do
  [ -e "$f" ] && echo "OK   $f" || { echo "BRAK $f — popraw sciezke i uruchom ponownie"; exit 1; }
done
echo "certy: $SSL_DIR"; ls "$SSL_DIR"/server.crt "$SSL_DIR"/ca.crt 2>/dev/null || echo "  (uwaga: nie znaleziono certow pod ta sciezka)"
echo "jar sha256:"; sha256sum "$JAR" 2>/dev/null || md5sum "$JAR"

hr "1. SRODOWISKO, NAZWY, TLS  (NetDiag)"
if [ -f "$HOME/NetDiag.java" ]; then
  ssh -n "$CLIENT" "cd \$HOME && $JAVA -Dssl.dir=$SSL_DIR -cp $CP \$HOME/NetDiag.java $SERVERS" 2>&1 \
    | grep -vE 'WARNING|log4j'
else
  echo "POMINIETO — brak ~/NetDiag.java (wyslij go razem z tym skryptem)"
fi

hr "2. KOSZT KRYPTOGRAFII NA WEZLE"
ssh -n dcc-1 "openssl speed -seconds 2 rsa4096 2>&1 | tail -2; echo; openssl speed -seconds 2 ecdsap256 2>&1 | tail -2"

# ---------------------------------------------------------------------------
run_transport() {   # $1 = quic|netty
  local kind="$1" flag="" tr="TCP_TLS" dir
  [ "$kind" = quic ] && { flag="--quic"; tr="QUIC"; }
  dir="/data/$USER/raft/$kind"

  hr "3.$kind  START SERWEROW ($tr)"
  for n in $SERVERS; do ssh -n "$n" "pkill -f CounterServer" 2>/dev/null; done
  sleep 2
  for n in $SERVERS; do
    ssh -n "$n" "mkdir -p $dir && ln -sfn \$HOME/ratis-test $dir/ratis-test && rm -rf $dir/n*"
  done
  local i=0
  for n in $SERVERS; do
    ssh -f "$n" "cd $dir && RATIS_EXAMPLE_CONF=$CONF \
      nohup sh -c 'sleep 86400 | $JAVA -cp $CP \
        org.apache.ratis.examples.counter.server.CounterServer $i $flag' \
      </dev/null > server$i.log 2>&1 &"
    i=$((i+1))
  done
  sleep 18
  local up=0
  for n in $SERVERS; do
    if ssh -n "$n" "ss -lnp 2>/dev/null | grep -q 10024"; then echo "$n: OK"; up=$((up+1)); else echo "$n: BRAK"; fi
  done
  if [ "$up" -lt "$NSRV" ]; then
    echo ">>> serwery $kind nie wstaly — log pierwszego wezla:"
    ssh -n "$(echo $SERVERS | cut -d' ' -f1)" "head -25 $dir/server0.log"
    return 1
  fi

  hr "4.$kind  POMIARY (1 klient, 10 zadan, bez rozgrzewki)"
  for C in A B; do
    local log="$OUT/${kind}_conn${C}.log"
    ssh -n "$CLIENT" "cd \$HOME && RATIS_EXAMPLE_CONF=$CONF $JAVA -Dratis.quic.connect.timing=true \
      -cp $CP org.apache.ratis.examples.counter.client.RaftBench \
      --transport $tr --mode rywrites --payload 1kB --requests 10 --warmup 0 \
      --conn $C --clients 1:1:1" > "$log" 2>&1
    local nconn
    nconn=$(grep -c 'QUIC-CONNECT' "$log")
    echo "--- $kind conn $C ---"
    grep -E 'Leader =' "$log" | head -1
    grep -E "^(QUIC|TCP_TLS)," "$log"
    echo "polaczen QUIC-CONNECT: $nconn na 10 zadan  => $(awk -v a="$nconn" 'BEGIN{printf "%.1f", a/10}') na zadanie"
    if [ "$nconn" -gt 0 ]; then
      echo "rozklad faz (mediany, ms):"
      for ph in codec bind handshake stream; do
        grep -o "$ph=[0-9.,]*" "$log" | cut -d= -f2 | tr ',' '.' | sort -n \
          | awk -v p="$ph" '{a[NR]=$1} END{if(NR)printf "   %-10s %8.2f   (n=%d, min=%.2f, max=%.2f)\n", p, a[int((NR+1)/2)], NR, a[1], a[NR]}'
      done
    fi
    echo
  done

  hr "5.$kind  PELNY PRZEBIEG (1 klient, 100 zadan — liczba porownywalna z wczorajszymi)"
  ssh -n "$CLIENT" "cd \$HOME && RATIS_EXAMPLE_CONF=$CONF $JAVA \
    -cp $CP org.apache.ratis.examples.counter.client.RaftBench \
    --transport $tr --mode rywrites --payload 1kB --requests 100 \
    --conn A --clients 1:1:1" 2>&1 | grep -E 'Leader =|^(QUIC|TCP_TLS),'
  ssh -n "$CLIENT" "cd \$HOME && RATIS_EXAMPLE_CONF=$CONF $JAVA \
    -cp $CP org.apache.ratis.examples.counter.client.RaftBench \
    --transport $tr --mode rywrites --payload 1kB --requests 100 \
    --conn B --clients 1:1:1" 2>&1 | grep -E 'Leader =|^(QUIC|TCP_TLS),'
}

run_transport quic
run_transport netty

hr "6. SPRZATANIE"
for n in $SERVERS; do ssh -n "$n" "pkill -f CounterServer" 2>/dev/null; done
echo "gotowe. komplet w $OUT"
ls -la "$OUT"
