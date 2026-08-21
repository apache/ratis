#!/usr/bin/env bash
#
# Lokalny smoke-test klastra o DOWOLNYM N na localhoscie - do weryfikacji zmian
# ZANIM cokolwiek pojedzie na klaster DCC. macOS-friendly (lsof zamiast ss).
#
#   N=5 TR=quic  bash benchmark/local/run_local_n.sh
#   N=7 TR=netty bash benchmark/local/run_local_n.sh
#
# Kryteria sukcesu (wypisywane na koncu):
#   - N procesow CounterServer zyje i nasluchuje na swoich portach
#   - CounterClient: zapis przechodzi przez lidera ('Current counter value' w wyjsciu)
#   - RaftBench (rywrites) konczy sie - to on faktycznie testuje odczyty z followerow;
#     CSV ma kolumne cluster_size=N
set -euo pipefail

N="${N:-5}"
TR="${TR:-quic}"          # quic | netty
PORT0="${PORT0:-10024}"
PORT_STEP=100
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
J="$REPO/ratis-examples/target/ratis-examples-3.3.0-SNAPSHOT.jar"
WD="${WD:-/tmp/raft-local-$TR-n$N}"

[ -f "$J" ] || { echo "!! brak $J - zbuduj ratis-examples (patrz RUNBOOK §3.1)"; exit 1; }

flag=""; t=TCP_TLS
[ "$TR" = quic ] && { flag="--quic"; t=QUIC; }

echo ">> Sprzatam poprzedni przebieg"
pkill -f CounterServer 2>/dev/null || true
sleep 1
rm -rf "$WD"; mkdir -p "$WD"; cd "$WD"
ln -sfn "$REPO/ratis-test" ratis-test    # certy czytane sciezka wzgledna

list=""
for ((i=0; i<N; i++)); do list+="${list:+,}127.0.0.1:$((PORT0 + PORT_STEP*i))"; done
echo "raft.server.address.list=$list" > conf.properties
export RATIS_EXAMPLE_CONF="$WD/conf.properties"
echo ">> conf: $list"

echo ">> Startuje $N serwerow ($TR)"
for ((i=0; i<N; i++)); do
  # sleep | java: CounterServer blokuje sie na Scanner.nextLine(); puste stdin = natychmiastowy exit
  nohup sh -c "sleep 3600 | java -cp '$J' \
    org.apache.ratis.examples.counter.server.CounterServer $i $flag" \
    </dev/null > "server$i.log" 2>&1 &
done

# UWAGA na kod wyjscia lsof: przy kilku selekcjach (-iTCP:p -iUDP:p) lsof zwraca 1,
# gdy KTORAKOLWIEK z nich nic nie znalazla - a tak jest zawsze (QUIC nie sluchа na TCP,
# Netty nie sluchа na UDP). W potoku pod `set -o pipefail` przewracalo to cale
# `lsof | grep -q java` i licznik zostawal na zerze mimo dzialajacych serwerow.
# Dlatego: przechwytujemy wyjscie z `|| true` i dopasowujemy tekstem.
# Jedno wywolanie lsof na probe (nie N) - lsof na macOS jest wolny (~1s).
LSOF_ARGS=()
for ((i=0; i<N; i++)); do
  p=$((PORT0 + PORT_STEP*i))
  LSOF_ARGS+=(-iTCP:$p -iUDP:$p)
done

count_listening() {
  local out p c=0 k
  out=$(lsof -nP "${LSOF_ARGS[@]}" 2>/dev/null || true)
  for ((k=0; k<N; k++)); do
    p=$((PORT0 + PORT_STEP*k))
    grep -qE "(UDP .*:$p\$|TCP .*:$p \(LISTEN\))" <<<"$out" && c=$((c+1)) || true
  done
  echo "$c"
}

READY_TIMEOUT="${READY_TIMEOUT:-900}"
echo ">> Czekam na gniazda (max ${READY_TIMEOUT}s; zimny start N JVM-ow bywa bardzo wolny)"
up=0; waited=0
while [ "$waited" -lt "$READY_TIMEOUT" ]; do
  up=$(count_listening)
  [ "$up" -eq "$N" ] && break
  sleep 3; waited=$((waited+3))
  [ $((waited % 30)) -eq 0 ] && echo "   ... $up/$N po ${waited}s" || true
done
echo "   nasluchuje $up/$N po ${waited}s"
[ "$up" -eq "$N" ] || { echo "!! nie wszystkie serwery wstaly; server0.log:"; tail -30 server0.log; exit 1; }

# CounterClient po wypisaniu "Current counter value" WISI z zalozenia na linearizowalnych
# odczytach z followerow (sendReadOnly -> ReadIndex) - ubijamy go po czasie i patrzymy
# na wyjscie, nie na kod powrotu. Sama linia dowodzi: lider wybrany + zapis przechodzi.
echo ">> Probny zapis przez lidera"
ok=""
for attempt in $(seq 1 15); do
  perl -e 'alarm 45; exec @ARGV' java -cp "$J" \
    org.apache.ratis.examples.counter.client.CounterClient 1 IO 1 $flag > probe.log 2>&1 || true
  grep -q 'Current counter value:' probe.log && { ok=1; break; }
  sleep 3
done
[ -n "$ok" ] || { echo "!! zapis nie przeszedl (brak lidera?); probe.log:"; tail -15 probe.log; exit 1; }
echo "   $(grep '^read from ' probe.log | head -1)"

echo ">> RaftBench rywrites (BEZ --read-ratio/--read-from - w tym trybie nie dzialaja)"
# workerow tyle co followerow => rozklad rowny 1:1
W=$((N-1)); [ "$W" -gt 10 ] && W=10
java -cp "$J" org.apache.ratis.examples.counter.client.RaftBench \
  --transport $t --mode rywrites --clients $W:$W:1 \
  --payload 1kB --requests 20 --warmup 5 --conn B \
  --run-id "local-${TR}-n${N}" --rep 1 --csv "$WD/ryw.csv"

echo ">> CSV:"
head -2 "$WD/ryw.csv"
# kolumny: run_id,rep,transport,cluster_size,... -> cluster_size to pole 4
size_col=$(tail -1 "$WD/ryw.csv" | cut -d, -f4)
[ "$size_col" = "$N" ] || { echo "!! cluster_size=$size_col, oczekiwano $N"; exit 1; }

pkill -f CounterServer 2>/dev/null || true
echo ">> OK: N=$N $TR - serwery wstaly, zapis+odczyty dzialaja, cluster_size=$N w CSV"
