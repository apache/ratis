#!/usr/bin/env bash
# ============================================================================
#  diag5.sh — czy strumienie QUIC daja przewage przy duzym ruchu serwer-serwer?
#
#  TEZA PRACY: miedzy serwerami ida 4 osobne strumienie (0x00 AppendEntries,
#  0x01 heartbeat, 0x02 InstallSnapshot, 0x03 RequestVote). W TCP wszystko
#  jedzie jednym polaczeniem, wiec duzy AppendEntries BLOKUJE heartbeat
#  i wszystko za nim (head-of-line blocking). W QUIC strumienie sa niezalezne.
#
#  Przy payload 1kB efekt nie istnieje (1-2 pakiety, nie ma czego blokowac) —
#  dlatego dotychczasowe pomiary pokazywaly parytet. Ten test robi SWEEP PO
#  ROZMIARZE payloadu i szuka punktu, w ktorym krzywe sie przecinaja.
#
#  PRZEWIDYWANIE (kierunkowe, falsyfikowalne):
#    - male payloady: parytet (to juz zmierzone)
#    - im wiekszy payload, tym lepszy QUIC — najpierw w p99, potem w tput
#    - efekt rosnie z liczba klientow (pelniejsze lacze = dluzsze kolejki TCP)
#  Jesli QUIC nie wygra nawet przy 1MB — teza o strumieniach do rewizji.
#
#  conn B (polaczenia trwale): zestawianie polaczen nie zaburza wyniku,
#  mierzymy czysta sciezke danych. Certy: EC (wymagane, sprawdzane).
#
#  Uzycie:  bash ~/diag5.sh          |  REPS=2 bash ~/diag5.sh (szybciej)
#  Czas:    ~40-60 min przy REPS=3
#  Wyniki:  ~/raft-results/diag5/
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
OUT="$HOME/raft-results/diag5"
CP="$JAR:$QUICHE"

# payload : zadan_na_klienta : warmup  (mniej zadan przy duzych rozmiarach,
# zeby jeden przebieg nie trwal kwadransa; 1MB x 20 req x 5 kl = 100MB przez konsensus)
SWEEP="${SWEEP:-1kB:100:20 16kB:100:20 64kB:60:10 256kB:40:10 1MB:20:5}"

mkdir -p "$OUT"
exec > >(tee "$OUT/diag5.log") 2>&1

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

# Duze payloady rozpychaja log Raft — czyscimy storage miedzy konfiguracjami,
# zeby kazda zaczynala od pustego logu (rowne warunki, /data sie nie zapelnia).
wipe_storage() {   # $1 = quic|netty
  local dir="/data/$USER/raft/$1"
  for n in $SERVERS; do ssh -n "$n" "pkill -u $USER -f CounterServer" >/dev/null 2>&1; done
  sleep 1
  for n in $SERVERS; do ssh -n "$n" "rm -rf $dir/n*" >/dev/null 2>&1; done
}

measure() {   # $1 = quic|netty  $2 = payload  $3 = req  $4 = warmup  $5 = klientow
  local kind="$1" P="$2" REQ="$3" WU="$4" NC="$5" tr="TCP_TLS"
  local wp50s="" wp99s="" wtputs="" wmbs="" rp99s="" r log line
  [ "$kind" = quic ] && tr="QUIC"
  for r in $(seq 1 "$REPS"); do
    log="$OUT/${kind}_${P}_${NC}kl_r${r}.log"
    ssh -n "$CLIENT" "cd \$HOME && RATIS_EXAMPLE_CONF=$CONF $JAVA \
      -cp $CP org.apache.ratis.examples.counter.client.RaftBench \
      --transport $tr --mode rywrites --payload $P --requests $REQ --warmup $WU \
      --conn B --clients $NC:$NC:1" > "$log" 2>&1
    line=$(grep -E "^(QUIC|TCP_TLS)," "$log" | head -1)
    if [ -z "$line" ]; then
      echo "   !!! $kind $P ${NC}kl przebieg $r: BRAK WYNIKU — poczatek logu:"
      head -5 "$log" | sed 's/^/       /'
      continue
    fi
    local wp50 wp99 wtput wmb rp99
    wtput=$(echo "$line" | awk -F, '{print $10}')
    wmb=$(echo "$line"  | awk -F, '{print $11}')
    wp50=$(echo "$line" | awk -F, '{print $12}')
    wp99=$(echo "$line" | awk -F, '{print $13}')
    rp99=$(echo "$line" | awk -F, '{print $17}')
    wp50s="$wp50s $wp50"; wp99s="$wp99s $wp99"; wtputs="$wtputs $wtput"
    wmbs="$wmbs $wmb"; rp99s="$rp99s $rp99"
    printf '   %s %-5s %skl przebieg %d: wp50=%-8s wp99=%-9s tput=%-7s MB/s=%-6s rp99=%s\n' \
      "$kind" "$P" "$NC" "$r" "$wp50" "$wp99" "$wtput" "$wmb" "$rp99"
  done
  [ -z "$wp50s" ] && { echo "$kind,$P,$NC,,,,," >> "$OUT/summary.csv"; return; }
  printf '   >>> %s %-5s %skl MEDIANY: wp50=%s wp99=%s tput=%s MB/s=%s rp99=%s\n' \
    "$kind" "$P" "$NC" "$(echo $wp50s | med)" "$(echo $wp99s | med)" \
    "$(echo $wtputs | med)" "$(echo $wmbs | med)" "$(echo $rp99s | med)"
  echo "$kind,$P,$NC,$(echo $wp50s | med),$(echo $wp99s | med),$(echo $wtputs | med),$(echo $wmbs | med),$(echo $rp99s | med)" >> "$OUT/summary.csv"
}

echo "transport,payload,klientow,write_p50,write_p99,tput,MB_s,read_p99" > "$OUT/summary.csv"
echo "powtorzen: $REPS,  sweep: $SWEEP"

for kind in quic netty; do
  hr "TRANSPORT: $kind"
  for spec in $SWEEP; do
    P="${spec%%:*}"; rest="${spec#*:}"; REQ="${rest%%:*}"; WU="${rest#*:}"
    echo "--- $kind payload=$P (req=$REQ, warmup=$WU) ---"
    wipe_storage "$kind"
    start_servers "$kind" || { echo "pomijam $kind/$P"; continue; }
    for NC in 1 5; do
      measure "$kind" "$P" "$REQ" "$WU" "$NC"
    done
  done
done

for n in $SERVERS; do ssh -n "$n" "pkill -u $USER -f CounterServer" >/dev/null 2>&1; done

hr "PODSUMOWANIE"
column -t -s, "$OUT/summary.csv" 2>/dev/null || cat "$OUT/summary.csv"

hr "QUIC vs NETTY — obok siebie (dodatnie % = QUIC lepszy)"
awk -F, 'NR>1 && $4!="" {
  key=$2"_"$3
  if ($1=="quic") { qp99[key]=$5; qtput[key]=$6; qrp99[key]=$8 }
  else            { np99[key]=$5; ntput[key]=$6; nrp99[key]=$8 }
  if (!(key in seen)) { order[++n]=key; seen[key]=1 }
}
END {
  printf "%-12s %10s %10s %8s | %9s %9s %8s | %9s %9s %8s\n",
    "payload_kl", "q_wp99", "n_wp99", "wp99%", "q_tput", "n_tput", "tput%", "q_rp99", "n_rp99", "rp99%"
  for (i=1;i<=n;i++) {
    k=order[i]
    if (k in qp99 && k in np99) {
      dw = (np99[k]-qp99[k])/np99[k]*100
      dt = (qtput[k]-ntput[k])/ntput[k]*100
      dr = (nrp99[k]-qrp99[k])/nrp99[k]*100
      printf "%-12s %10s %10s %+7.1f%% | %9s %9s %+7.1f%% | %9s %9s %+7.1f%%\n",
        k, qp99[k], np99[k], dw, qtput[k], ntput[k], dt, qrp99[k], nrp99[k], dr
    }
  }
  print ""
  print "Czytanie: szukamy payloadu, od ktorego kolumny % robia sie DODATNIE."
  print "  wp99%  — ogon zapisow: tu HOL blocking TCP powinien bolec najpierw"
  print "  tput%  — przepustowosc zapisow"
  print "  rp99%  — ogon odczytow z followera (heartbeat/commit za duzym AppendEntries)"
  print "Jesli przy 1MB wszystko nadal ujemne — przewaga strumieni sie NIE potwierdza."
}' "$OUT/summary.csv"

echo; echo "gotowe: $OUT"
