#!/usr/bin/env bash
# ============================================================================
#  diag2.sh — czy pozycja lidera tlumaczy skok 53ms -> 198ms w conn A?
#
#  HIPOTEZA: swiezy klient w conn A nie wie, kto jest liderem, wiec celuje
#  w PIERWSZEGO peera (n0). Gdy liderem jest n0 - trafia. Gdy n1/n2 - pudluje
#  i placi dodatkowy handshake (~43 ms na klastrze, ~7 ms lokalnie, stad brak
#  efektu w tescie lokalnym).
#
#  Priorytetow uzyc NIE MOZNA: Constants.java:120 wpisuje je w RaftPeer, a klient
#  czyta ten sam conf, wiec celowalby w tego samego peera, ktory zostaje liderem.
#  Dlatego lidera "losujemy" restartami az do trafienia.
#
#  Kazda konfiguracja mierzona REPS razy -> mediana. Pojedynczy przebieg stracil
#  wiarygodnosc (197.8 vs 53.1 przy tym samym jarze).
#
#  Uzycie:  bash ~/diag2.sh            # pelny przebieg
#           REPS=5 bash ~/diag2.sh     # wiecej powtorzen
#  Wyniki:  ~/raft-results/diag2/
# ============================================================================
set -u

JAVA="${JAVA:-$HOME/jdk21/bin/java}"
JAR="${JAR:-$HOME/ratis.jar}"
QUICHE="${QUICHE:-$HOME/netty-quiche-linux.jar}"
CONF="${CONF:-$HOME/conf.properties}"
SERVERS="${SERVERS:-dcc-1 dcc-2 dcc-3}"
NSRV=$(echo $SERVERS | wc -w | tr -d ' ')
CLIENT="${CLIENT:-dcc-4}"
REPS="${REPS:-3}"
REQ="${REQ:-100}"
MAX_ELECTIONS="${MAX_ELECTIONS:-8}"
OUT="$HOME/raft-results/diag2"
CP="$JAR:$QUICHE"

mkdir -p "$OUT"
exec > >(tee "$OUT/diag2.log") 2>&1

hr()  { printf '\n============================================================\n%s\n============================================================\n' "$1"; }
med() { tr ' ' '\n' | grep -v '^$' | sort -n | awk '{a[NR]=$1} END{if(NR)printf "%.2f", a[int((NR+1)/2)]}'; }

# UWAGA: start serwerow NIE moze byc wolany przez $(...). `ssh -f` idzie w tlo
# z otwartym stdout, wiec podstawienie polecen czekaloby na wygasniecie serwera
# (86400 s). Dlatego start i wykrycie lidera sa rozdzielone, a wynik wraca
# przez globalna zmienna LEADER zamiast przez stdout.
LEADER=""

# --- start serwerow danego transportu (nic nie zwraca) -----------------------
start_servers() {   # $1 = quic|netty
  local kind="$1" flag="" dir i=0
  [ "$kind" = quic ] && flag="--quic"
  dir="/data/$USER/raft/$kind"

  for n in $SERVERS; do ssh -n "$n" "pkill -f CounterServer" 2>/dev/null; done
  sleep 2
  for n in $SERVERS; do
    ssh -n "$n" "mkdir -p $dir && ln -sfn \$HOME/ratis-test $dir/ratis-test && rm -rf $dir/n*"
  done
  for n in $SERVERS; do
    # >/dev/null 2>&1 jest tu KONIECZNE — bez tego backgroundowany ssh trzyma
    # otwarty deskryptor i zawiesza wszystko, co probuje przechwycic wyjscie.
    ssh -f "$n" "cd $dir && RATIS_EXAMPLE_CONF=$CONF \
      nohup sh -c 'sleep 86400 | $JAVA -cp $CP \
        org.apache.ratis.examples.counter.server.CounterServer $i $flag' \
      </dev/null > server$i.log 2>&1 &" >/dev/null 2>&1
    i=$((i+1))
  done

  # Zamiast stalego sleepa: odpytuj, az wszystkie 3 porty nasluchuja.
  # Lokalnie zajmuje 2.9 s; na wezle klastra 6-10 s. Limit 40 s.
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
  sleep 3   # margines na elekcje po tym, jak wszystkie wstaly
}

# --- wykrycie lidera (jedyna funkcja wolana przez $(...)) --------------------
detect_leader() {   # $1 = quic|netty
  local kind="$1" tr="TCP_TLS"
  [ "$kind" = quic ] && tr="QUIC"
  ssh -n "$CLIENT" "cd \$HOME && RATIS_EXAMPLE_CONF=$CONF $JAVA -cp $CP \
    org.apache.ratis.examples.counter.client.RaftBench --transport $tr --mode rywrites \
    --payload 1kB --requests 2 --warmup 1 --conn B --clients 1:1:1" 2>&1 \
    | grep -o 'Leader = n[0-9]' | head -1 | sed 's/Leader = //'
}

# --- restartuj az elekcja da zadanego lidera ---------------------------------
# Wynik zapisuje do globalnej LEADER — bez podstawienia polecen wokol startu.
hunt_leader() {   # $1 = quic|netty   $2 = n0|n1
  local kind="$1" want="$2" attempt=1
  while [ "$attempt" -le "$MAX_ELECTIONS" ]; do
    start_servers "$kind"
    LEADER=$(detect_leader "$kind")
    echo "   elekcja $attempt: lider = ${LEADER:-BRAK}"
    [ "$LEADER" = "$want" ] && return 0
    attempt=$((attempt+1))
  done
  echo "   nie udalo sie wylosowac $want w $MAX_ELECTIONS probach"
  return 1
}

# --- REPS przebiegow jednej konfiguracji -------------------------------------
measure() {   # $1 = quic|netty   $2 = A|B   $3 = etykieta lidera
  local kind="$1" C="$2" leader="$3" tr="TCP_TLS" p50s="" p99s="" conns="" r log
  [ "$kind" = quic ] && tr="QUIC"

  for r in $(seq 1 "$REPS"); do
    log="$OUT/${kind}_${leader}_conn${C}_r${r}.log"
    ssh -n "$CLIENT" "cd \$HOME && RATIS_EXAMPLE_CONF=$CONF $JAVA -Dratis.quic.connect.timing=true \
      -cp $CP org.apache.ratis.examples.counter.client.RaftBench \
      --transport $tr --mode rywrites --payload 1kB --requests $REQ \
      --conn $C --clients 1:1:1" > "$log" 2>&1
    local line p50 p99 nc
    line=$(grep -E "^(QUIC|TCP_TLS)," "$log" | head -1)
    p50=$(echo "$line" | awk -F, '{print $12}')
    p99=$(echo "$line" | awk -F, '{print $13}')
    nc=$(grep -c 'QUIC-CONNECT' "$log")
    p50s="$p50s $p50"; p99s="$p99s $p99"; conns="$conns $nc"
    printf '   %s conn%s [%s] przebieg %d: write p50=%-8s p99=%-8s polaczen=%s\n' \
      "$kind" "$C" "$leader" "$r" "${p50:-?}" "${p99:-?}" "$nc"
  done
  printf '   >>> %s conn%s [%s] MEDIANA p50 = %s ms   (p99 = %s ms, polaczen = %s)\n' \
    "$kind" "$C" "$leader" "$(echo $p50s | med)" "$(echo $p99s | med)" "$(echo $conns | med)"
  echo "$kind,$C,$leader,$(echo $p50s | med),$(echo $p99s | med),$(echo $conns | med)" >> "$OUT/summary.csv"
}

echo "transport,conn,lider,write_p50_med,write_p99_med,polaczen_med" > "$OUT/summary.csv"
echo "powtorzen na konfiguracje: $REPS,  zadan na przebieg: $REQ"

for kind in quic netty; do
  for want in n0 n1; do
    hr "$kind — polujemy na lidera $want"
    if ! hunt_leader "$kind" "$want"; then
      echo ">>> POMINIETO $kind/$want — elekcja uparcie dawala ${LEADER:-BRAK}"
      continue
    fi
    echo ">>> lider = $LEADER  ($([ "$want" = n0 ] && echo 'PIERWSZY peer — klient trafia' || echo 'NIE pierwszy — klient pudluje'))"
    measure "$kind" A "$want"
    measure "$kind" B "$want"
  done
done

hr "PODSUMOWANIE"
column -t -s, "$OUT/summary.csv" 2>/dev/null || cat "$OUT/summary.csv"
echo
echo "Czytanie wyniku:"
echo "  conn A quic n1 >> conn A quic n0   -> hipoteza POTWIERDZONA, koszt pudla = roznica"
echo "  conn A quic n1 ~= conn A quic n0   -> hipoteza OBALONA, skok 198ms mial inna przyczyne"
echo "  conn B nie powinien zalezec od lidera w ogole (klient uczy sie raz)"

for n in $SERVERS; do ssh -n "$n" "pkill -f CounterServer" 2>/dev/null; done
echo; echo "gotowe: $OUT"
