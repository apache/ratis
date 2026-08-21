#!/usr/bin/env bash
#
# Benchmark Raft-over-QUIC vs Netty/TCP+TLS na klastrze DCC (SLURM), DOWOLNE N serwerow.
# Uruchamiaj NA WEZLE DOSTEPOWYM klastra: bash benchmark/lan/run_lan.sh
#
# Wymagania (RUNBOOK §2/§3):
#   ~/jdk21/                     wlasne JDK (na wezlach nie ma javy)
#   ~/ratis.jar                  fat jar ratis-examples (zbudowany na Macu, scp na klaster)
#   ~/netty-quiche-linux.jar     natywna quiche pod linux-x86_64 (jar z Maca jej NIE ma)
#   ~/ratis-test/src/test/resources/ssl/   certy (server.crt ECDSA P-256, ca.crt, ...)
#   klucz ssh w ~/.ssh/authorized_keys (ssh miedzy wezlami bez hasla)
#
# TEN SKRYPT TYLKO MIERZY - nigdy nie rezerwuje i nigdy nie zwalnia wezlow.
# Wezly musza byc juz zarezerwowane przez alloc.sh:
#   bash alloc.sh                     # rezerwuje, zapisuje JOBID do ~/.raft-alloc
#   bash run_lan.sh                   # mierzy (mozna wielokrotnie)
#   bash alloc.sh free                # zwalnia wezly
# Bez rezerwacji skrypt konczy sie bledem i nie robi nic.
#
# Przyklady:
#   SIZES="3 5" TRANSPORTS=quic CONNS=B REPEATS=1 REQUESTS=50 bash benchmark/lan/run_lan.sh   # smoke
#   bash benchmark/lan/run_lan.sh                                                             # pelny sweep
#   JOBID=12345 bash benchmark/lan/run_lan.sh                # jawne wskazanie rezerwacji
#   SIZES=5 REPEATS=1 bash benchmark/lan/run_lan.sh --runall  # pelne porownanie:
#                                                             # TCP A, TCP B, QUIC A, QUIC B
set -euo pipefail

# ---------------- KONFIGURACJA ----------------
SIZES="${SIZES:-3 5 7}"              # rozmiary klastra do przemiatania (nieparzyste!)
TRANSPORTS="${TRANSPORTS:-quic netty}"
CONNS="${CONNS:-A B}"

# --runall: pelne porownanie w ustalonej kolejnosci - najpierw TCP (conn A i B),
# potem QUIC (conn A i B). Nadpisuje TRANSPORTS i CONNS; reszta parametrow
# (SIZES, PAYLOADS, REPEATS, ...) dziala normalnie.
for arg in "$@"; do
  if [ "$arg" = "--runall" ]; then
    TRANSPORTS="netty quic"
    CONNS="A B"
  fi
done
# rywrites: kazdy worker pisze do lidera i czyta SWOJ klucz z przypisanego followera.
# --read-ratio / --read-from NIE dzialaja w tym trybie - nie podawac.
# Rowny rozklad workerow po followerach wymaga BENCH_CLIENTS podzielnego przez N-1
# dla kazdego N ze SIZES (dla 3/5: 8 workerow; dla 3/5/7 trzeba 12 => 2 procesy klienckie,
# czego ten skrypt nie automatyzuje - patrz RUNBOOK §7.2).
BENCH_CLIENTS="${BENCH_CLIENTS:-8:8:1}"   # <=10 workerow na proces JVM
PAYLOADS="${PAYLOADS:-1kB}"          # lista, np. "64 1kB 1MB" - przemiatana jak SIZES
REQUESTS="${REQUESTS:-500}"
WARMUP="${WARMUP:-20}"
REPEATS="${REPEATS:-3}"              # 3 przebiegi; pierwszy odrzucic (zimna JVM), mediana z reszty

PORT="${PORT:-10024}"
NUM_CLIENT_NODES="${NUM_CLIENT_NODES:-1}"   # ile OSTATNICH wezlow alokacji to klienci
# Skad wziac rezerwacje: jawne JOBID= wygrywa, inaczej czytamy plik od alloc.sh.
# Wezlow NIE zwalniamy w zadnym przypadku - robi to `bash alloc.sh free`.
JOBID="${JOBID:-}"
ALLOC_FILE="${ALLOC_FILE:-$HOME/.raft-alloc}"

JAVA="$HOME/jdk21/bin/java"
JAR="$HOME/ratis.jar"
CP="$JAR:$HOME/netty-quiche-linux.jar"
SERVER_CLASS=org.apache.ratis.examples.counter.server.CounterServer
CLIENT_CLASS=org.apache.ratis.examples.counter.client.CounterClient
BENCH_CLASS=org.apache.ratis.examples.counter.client.RaftBench

RUN_ID="$(date +%Y%m%d_%H%M%S)"
RESULTS="$HOME/raft-results/$RUN_ID"     # NFS home - przezywa uspienie wezlow
BOOT_TIMEOUT="${BOOT_TIMEOUT:-300}"      # wybudzenie wezla trwa ~3 min

MAX_N=0; for n in $SIZES; do [ "$n" -gt "$MAX_N" ] && MAX_N=$n; done
TOTAL_NODES=$((MAX_N + NUM_CLIENT_NODES))
NODES=(); SERVERS=(); CLIENTS=()

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# ---------------- 1. UZYCIE ISTNIEJACEJ REZERWACJI ----------------
# Ten skrypt NIE rezerwuje. Bierze wezly z rezerwacji zrobionej przez alloc.sh
# (albo ze wskazanego JOBID=) i nigdy jej nie zwalnia.
use_allocation() {
  mkdir -p "$RESULTS"
  # 1) jawne JOBID= wygrywa
  # 2) plik od alloc.sh
  # 3) wlasna dzialajaca rezerwacja (np. zrobiona recznie przez salloc) - jesli jest DOKLADNIE jedna
  if [ -z "$JOBID" ] && [ -f "$ALLOC_FILE" ]; then
    JOBID=$(sed -n 's/^JOBID=//p' "$ALLOC_FILE" | head -1)
  fi
  if [ -z "$JOBID" ]; then
    local running count
    running=$(squeue -u "$USER" -h -t RUNNING -o %i 2>/dev/null || true)
    count=$(printf '%s\n' "$running" | grep -c . || true)
    if [ "$count" = "1" ]; then
      JOBID="$running"
      log "Znalazlem Twoja rezerwacje JOBID=$JOBID - uzywam jej (nie zwolnie na koniec)"
    elif [ "$count" -gt 1 ]; then
      echo "!! Masz kilka dzialajacych rezerwacji - wskaz, ktorej uzyc:"
      squeue -u "$USER" -o "%.8i %.12j %.10M %.10L %R"
      echo "       JOBID=<numer> bash run_lan.sh"
      exit 1
    fi
  fi
  if [ -z "$JOBID" ]; then
    echo "!! Brak rezerwacji wezlow."
    echo "   Ten skrypt tylko mierzy - wezly rezerwuje alloc.sh. Zrob najpierw:"
    echo "       bash alloc.sh"
    echo "   albo zrob salloc recznie, albo wskaz:  JOBID=<numer> bash run_lan.sh"
    exit 1
  fi

  local st
  st=$(squeue -h -j "$JOBID" -o %T 2>/dev/null || true)
  if [ -z "$st" ]; then
    echo "!! Rezerwacja JOBID=$JOBID nie istnieje (wygasla albo zostala zwolniona)."
    echo "   Zarezerwuj na nowo:  bash alloc.sh"
    exit 1
  fi
  # alloc.sh oddaje sterowanie dopiero w stanie RUNNING, ale przy JOBID= podanym
  # recznie wezly moga sie jeszcze budzic (stan CF/CONFIGURING) - poczekajmy.
  local t=0
  while [ "$st" != "RUNNING" ]; do
    sleep 2; t=$((t+2))
    [ "$t" -ge "$BOOT_TIMEOUT" ] && { echo "!! rezerwacja $JOBID w stanie '$st', nie RUNNING"; exit 1; }
    st=$(squeue -h -j "$JOBID" -o %T 2>/dev/null || true)
    [ -z "$st" ] && { echo "!! rezerwacja $JOBID zniknela"; exit 1; }
  done

  mapfile -t NODES < <(scontrol show hostnames "$(squeue -h -j "$JOBID" -o %N)")
  if [ "${#NODES[@]}" -lt "$TOTAL_NODES" ]; then
    echo "!! Rezerwacja ma ${#NODES[@]} wezlow, a SIZES=\"$SIZES\" (+$NUM_CLIENT_NODES klient)"
    echo "   potrzebuje $TOTAL_NODES. Zmniejsz SIZES albo zarezerwuj wiecej:"
    echo "       bash alloc.sh free && NODES=$TOTAL_NODES bash alloc.sh"
    exit 1
  fi
  # KLIENCI = OSTATNIE wezly alokacji -> ten sam sprzet klienta przy kazdym N (porownywalnosc)
  CLIENTS=("${NODES[@]: -NUM_CLIENT_NODES}")
  log "Wezly: ${NODES[*]}   klient(y): ${CLIENTS[*]}"
  { echo "run_id=$RUN_ID"; echo "jobid=$JOBID"; echo "nodes=${NODES[*]}";
    echo "clients=${CLIENTS[*]}"; echo "sizes=$SIZES"; echo "transports=$TRANSPORTS";
    echo "conns=$CONNS repeats=$REPEATS"; echo "mode=rywrites clients_spec=$BENCH_CLIENTS";
    echo "payloads=$PAYLOADS requests=$REQUESTS warmup=$WARMUP";
  } > "$RESULTS/meta.txt"
}

# Sprzatamy tylko PROCESY na wezlach - rezerwacja zostaje (zwalnia ja `alloc.sh free`).
cleanup() {
  local n
  for n in "${NODES[@]:-}"; do
    ssh -n -o ConnectTimeout=5 "$n" "pkill -f CounterServer" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

# ---------------- 2. BOOT ----------------
wait_boot() {
  local node t
  for node in "${NODES[@]}"; do
    t=0
    until ssh -n -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5 \
        "$node" true 2>/dev/null; do
      sleep 2; t=$((t+2))
      [ "$t" -ge "$BOOT_TIMEOUT" ] && { echo "!! $node nie wstal w ${BOOT_TIMEOUT}s"; exit 1; }
    done
    log "  $node OK"
  done
}

# ---------------- 3. conf.properties dla danego N (NFS home => widoczne wszedzie) ----------------
gen_conf() {   # $1 = N ; echo -> sciezka
  local n="$1" list="" i
  for ((i=0; i<n; i++)); do list+="${list:+,}${NODES[$i]}:${PORT}"; done
  local f="$HOME/conf-n${n}.properties"
  { echo "# WYGENEROWANE przez run_lan.sh (run $RUN_ID) - nie edytuj recznie";
    echo "raft.server.address.list=${list}"; } > "$f"
  cp "$f" "$RESULTS/conf-n${n}.properties"
  echo "$f"
}

# ---------------- 4. START SERWEROW ----------------
start_servers() {   # $1=N $2=transport $3=conf
  local n="$1" tr="$2" conf="$3" i node
  local flag=""; [ "$tr" = quic ] && flag="--quic"
  local wd="/data/$USER/raft/$tr/n$n"
  SERVERS=("${NODES[@]:0:$n}")

  # pkill w OSOBNYM ssh - w jednej komendzie pkill -f zabija wlasna powloke (RUNBOOK §9)
  for node in "${SERVERS[@]}"; do
    ssh -n "$node" "pkill -f CounterServer" >/dev/null 2>&1 || true
  done
  # Kasowanie STAREGO storage n* jest krytyczne: log Raft z przebiegu o innym N ma inny
  # sklad grupy i serwer wystartowalby RECOVER na zlej konfiguracji.
  for node in "${SERVERS[@]}"; do
    ssh -n "$node" "mkdir -p '$wd' && ln -sfn '$HOME/ratis-test' '$wd/ratis-test' && rm -rf '$wd'/n*"
  done

  i=0
  for node in "${SERVERS[@]}"; do
    # indeks peera = POZYCJA W TABLICY (nie numer z nazwy hosta) - SLURM moze dac dowolne wezly.
    # sleep 86400 | java: CounterServer czeka na Scanner.nextLine(); puste stdin ubija go od razu.
    # ssh -f + przekierowania: bez nich skrypt zawisa na otwartym stdin/stdout (RUNBOOK §9).
    ssh -f "$node" "cd '$wd' && RATIS_EXAMPLE_CONF='$conf' \
      nohup sh -c 'sleep 86400 | $JAVA -cp $CP $SERVER_CLASS $i $flag' \
      </dev/null > 'server$i.log' 2>&1 &" >/dev/null 2>&1
    i=$((i+1))
  done
  log "  wystartowano $n serwerow ($tr) w $wd"
}

# ---------------- 5. GOTOWOSC ----------------
# log4j nie ma appendera => grep 'becomeLeader' NIE dziala. Dwustopniowo:
#   (a) gniazdo na kazdym wezle: ss -lnp bez -t/-u lapie TCP i UDP naraz,
#   (b) probny zapis przez lidera: linia 'Current counter value' = lider wybrany + zapis
#       przechodzi. UWAGA: CounterClient potem WISI z zalozenia na linearizowalnych
#       odczytach z followerow (sendReadOnly -> ReadIndex), stad timeout i ignorowany
#       kod wyjscia. Odczyty z followerow realnie testuje dopiero RaftBench (stale read).
wait_ready() {   # $1=N $2=transport $3=conf
  local n="$1" tr="$2" conf="$3" node up=0 waited=0
  # Petla sprawdza NAJPIERW, spi tylko gdy jeszcze nie gotowe - przy normalnym starcie
  # wychodzi po paru sekundach. sock_timeout to tylko granica cierpliwosci.
  local sock_timeout="${SOCK_TIMEOUT:-120}"
  while [ "$waited" -lt "$sock_timeout" ]; do
    up=0
    for node in "${SERVERS[@]}"; do
      ssh -n "$node" "ss -lnp 2>/dev/null | grep -q ':$PORT'" && up=$((up+1)) || true
    done
    [ "$up" -eq "$n" ] && break
    sleep 2; waited=$((waited+2))
    [ $((waited % 30)) -eq 0 ] && log "   ... nasluchuje $up/$n po ${waited}s" || true
  done
  [ "$up" -eq "$n" ] || { dump_logs "$n" "$tr"
    echo "!! nasluchuje tylko $up/$n serwerow na :$PORT (po ${waited}s)"; exit 1; }

  local cflag=""; [ "$tr" = quic ] && cflag="--quic"
  local probe="$RESULTS/probe_${tr}_n${n}.log"
  for attempt in $(seq 1 20); do
    ssh -n "${CLIENTS[0]}" "cd '$HOME' && RATIS_EXAMPLE_CONF='$conf' timeout 60 \
          $JAVA -cp $CP $CLIENT_CLASS 1 IO 1 $cflag" > "$probe" 2>&1 || true
    if grep -q 'Current counter value:' "$probe"; then
      log "  klaster gotowy: lider wybrany, zapis przechodzi"
      return 0
    fi
    sleep 2
  done
  dump_logs "$n" "$tr"
  echo "!! brak lidera lub niekompletny klaster (szczegoly: $probe)"; exit 1
}

dump_logs() {   # $1=N $2=transport
  local n="$1" tr="$2" i=0 node
  for node in "${SERVERS[@]}"; do
    scp -q "$node:/data/$USER/raft/$tr/n$n/server$i.log" \
        "$RESULTS/${tr}_n${n}_server${i}.log" 2>/dev/null || true
    i=$((i+1))
  done
}

# ---------------- 6. BENCHMARK ----------------
run_bench() {   # $1=N $2=transport $3=conf $4=conn $5=payload $6=rep
  local n="$1" tr="$2" conf="$3" conn="$4" payload="$5" rep="$6"
  local t=TCP_TLS; [ "$tr" = quic ] && t=QUIC
  # Jeden CSV per (transport, conn). W wierszu: cluster_size rozroznia N, payload_bytes
  # rozroznia ladunek, rep powtorzenia, run_id przynaleznosc do tego przebiegu.
  local csv="$RESULTS/${tr}_rywrites_conn${conn}.csv"
  ssh -n "${CLIENTS[0]}" "cd '$HOME' && RATIS_EXAMPLE_CONF='$conf' $JAVA -cp $CP $BENCH_CLASS \
    --transport $t --mode rywrites --conn $conn --clients $BENCH_CLIENTS \
    --payload $payload --requests $REQUESTS --warmup $WARMUP \
    --run-id '$RUN_ID' --rep $rep --csv '$csv'" \
    | tee -a "$RESULTS/bench_${tr}_n${n}_conn${conn}.log"
}

# ---------------- MAIN ----------------
# Petla: N { transport { payload { conn { rep } } } }
# - oba transporty ida tuz po sobie w obrebie jednego N, wiec ewentualny dryf
#   obciazenia klastra dotyka ich jednakowo;
# - payload i conn sa WEWNATRZ, bo to parametry klienta - nie wymagaja restartu serwerow.
use_allocation
wait_boot
for N in $SIZES; do
  CONF="$(gen_conf "$N")"
  for TR in $TRANSPORTS; do
    log "=== N=$N transport=$TR ==="
    start_servers "$N" "$TR" "$CONF"
    wait_ready    "$N" "$TR" "$CONF"
    for PL in $PAYLOADS; do
      for CONN in $CONNS; do
        for REP in $(seq 1 "$REPEATS"); do
          log "  bench N=$N $TR payload=$PL conn=$CONN rep=$REP/$REPEATS"
          run_bench "$N" "$TR" "$CONF" "$CONN" "$PL" "$REP"
        done
      done
    done
    dump_logs "$N" "$TR"
    for node in "${SERVERS[@]}"; do
      ssh -n "$node" "pkill -f CounterServer" >/dev/null 2>&1 || true
    done
  done
done
log "Gotowe. Wyniki: $RESULTS"
