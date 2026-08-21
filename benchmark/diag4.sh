#!/usr/bin/env bash
# ============================================================================
#  diag4.sh — czy na certach EC problem wielu klientow nadal istnieje?
#
#  TLO: przy 5 klientach QUIC przegrywal ~2-3x. Mechanizm: wszystkie handshake'y
#  ida przez JEDNO gniazdo UDP = jeden watek event loopa, a kazdy handshake
#  blokowal ten watek na ~31 ms podpisu RSA (handshake 39 ms -> ~88 ms mediany
#  przy 5 klientach). Na certach EC podpis kosztuje ~0.07 ms.
#
#  PYTANIE: czy kolejka zniknela razem z podpisem?
#    handshake@5kl ~= handshake@1kl  -> TAK, stara "trojka" nie istnieje,
#                                       SO_REUSEPORT niepotrzebny
#    handshake@5kl >> handshake@1kl  -> kolejka zostala (siedzi w samym
#                                       przetwarzaniu pakietow) -> SO_REUSEPORT
#
#  Mierzy: 2 transporty x conn A/B x 1 i 5 klientow, po REPS przebiegow.
#  Wymaga certow EC (sprawdza; instaluje z *.ec jesli trzeba).
#
#  Uzycie:  bash ~/diag4.sh        |  REPS=5 bash ~/diag4.sh
#  Wyniki:  ~/raft-results/diag4/
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
OUT="$HOME/raft-results/diag4"
CP="$JAR:$QUICHE"

mkdir -p "$OUT"
exec > >(tee "$OUT/diag4.log") 2>&1

hr()  { printf '\n============================================================\n%s\n============================================================\n' "$1"; }
med() { tr ' ' '\n' | grep -v '^$' | sort -n | awk '{a[NR]=$1} END{if(NR)printf "%.2f", a[int((NR+1)/2)]}'; }

hr "0. CERTY MUSZA BYC EC"
BITS=$(openssl x509 -in "$SSL/server.crt" -noout -text | grep -m1 'Public-Key' | grep -o '[0-9]*')
if [ "$BITS" != "256" ]; then
  if [ -f "$SSL/server.crt.ec" ]; then
    cp "$SSL/server.crt.ec" "$SSL/server.crt"; cp "$SSL/server.pem.ec" "$SSL/server.pem"
    echo "zainstalowano certy EC (byly: ${BITS}-bit)"
  else
    echo "BLAD: cert ma ${BITS} bitow i nie ma $SSL/server.crt.ec — odpal najpierw diag3.sh"; exit 1
  fi
else
  echo "OK: server.crt = EC P-256"
fi

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

measure() {   # $1 = quic|netty   $2 = A|B   $3 = liczba klientow
  local kind="$1" C="$2" NC="$3" tr="TCP_TLS" p50s="" p99s="" tputs="" hss="" r log line
  [ "$kind" = quic ] && tr="QUIC"
  for r in $(seq 1 "$REPS"); do
    log="$OUT/${kind}_conn${C}_${NC}kl_r${r}.log"
    ssh -n "$CLIENT" "cd \$HOME && RATIS_EXAMPLE_CONF=$CONF $JAVA -Dratis.quic.connect.timing=true \
      -cp $CP org.apache.ratis.examples.counter.client.RaftBench \
      --transport $tr --mode rywrites --payload 1kB --requests $REQ \
      --conn $C --clients $NC:$NC:1" > "$log" 2>&1
    line=$(grep -E "^(QUIC|TCP_TLS)," "$log" | head -1)
    local p50 p99 tput hs=""
    p50=$(echo "$line" | awk -F, '{print $12}')
    p99=$(echo "$line" | awk -F, '{print $13}')
    tput=$(echo "$line" | awk -F, '{print $10}')
    if [ "$kind" = quic ]; then
      hs=$(grep -o 'handshake=[0-9.,]*' "$log" | cut -d= -f2 | tr ',' '.' | sort -n \
        | awk '{a[NR]=$1} END{if(NR)printf "%.1f", a[int((NR+1)/2)]}')
    fi
    p50s="$p50s $p50"; p99s="$p99s $p99"; tputs="$tputs $tput"; hss="$hss $hs"
    printf '   %s conn%s %skl przebieg %d: p50=%-8s p99=%-8s tput=%-7s%s\n' \
      "$kind" "$C" "$NC" "$r" "${p50:-?}" "${p99:-?}" "${tput:-?}" \
      "$([ -n "$hs" ] && echo " handshake_med=${hs}ms")"
  done
  local m_p50 m_p99 m_tput m_hs
  m_p50=$(echo $p50s | med); m_p99=$(echo $p99s | med); m_tput=$(echo $tputs | med)
  m_hs=$(echo $hss | med)
  printf '   >>> %s conn%s %skl MEDIANY: p50=%s p99=%s tput=%s%s\n' \
    "$kind" "$C" "$NC" "$m_p50" "$m_p99" "$m_tput" \
    "$([ -n "$m_hs" ] && echo " handshake=${m_hs}ms")"
  echo "$kind,$C,$NC,$m_p50,$m_p99,$m_tput,${m_hs:-}" >> "$OUT/summary.csv"
}

echo "transport,conn,klientow,write_p50_med,write_p99_med,tput_med,handshake_med" > "$OUT/summary.csv"
echo "powtorzen: $REPS,  zadan na klienta: $REQ,  certy: EC P-256"

for kind in quic netty; do
  hr "TRANSPORT: $kind"
  start_servers "$kind" || { echo "pomijam $kind"; continue; }
  for NC in 1 5; do
    for C in A B; do
      echo "--- $kind conn$C, $NC klient(ow) ---"
      measure "$kind" "$C" "$NC"
    done
  done
done

for n in $SERVERS; do ssh -n "$n" "pkill -u $USER -f CounterServer" >/dev/null 2>&1; done

hr "PODSUMOWANIE"
column -t -s, "$OUT/summary.csv" 2>/dev/null || cat "$OUT/summary.csv"

hr "ODPOWIEDZI"
awk -F, 'NR>1 {p50[$1"_"$2"_"$3]=$4; tput[$1"_"$2"_"$3]=$6; hs[$1"_"$2"_"$3]=$7}
END {
  h1=hs["quic_A_1"]; h5=hs["quic_A_5"];
  if (h1!="" && h5!="") {
    printf "1. Kolejka handshake: 1kl=%.1fms  5kl=%.1fms  (na RSA bylo 39 -> ~88)\n", h1, h5;
    if (h5 < h1*1.5) print "   => kolejka ZNIKNELA — stara trojka nie istnieje, SO_REUSEPORT zbedny";
    else             print "   => kolejka ZOSTALA — przetwarzanie pakietow tez sie serializuje -> SO_REUSEPORT";
  }
  qa=tput["quic_A_5"]; na=tput["netty_A_5"];
  qb=tput["quic_B_5"]; nb=tput["netty_B_5"];
  if (qa!="" && na!="") printf "2. Przepustowosc 5kl conn A: QUIC=%s Netty=%s  (Netty/QUIC = %.2fx; na RSA bylo ~2.9x)\n", qa, na, na/qa;
  if (qb!="" && nb!="") printf "3. Przepustowosc 5kl conn B: QUIC=%s Netty=%s  (Netty/QUIC = %.2fx; na RSA bylo ~2.1x)\n", qb, nb, nb/qb;
  pa1=p50["quic_A_1"]; pa5=p50["quic_A_5"];
  if (pa1!="" && pa5!="") printf "4. QUIC conn A p50: 1kl=%s 5kl=%s (rosnie kolejka? na RSA: 51.6 -> 132.7)\n", pa1, pa5;
}' "$OUT/summary.csv"

echo; echo "gotowe: $OUT"
