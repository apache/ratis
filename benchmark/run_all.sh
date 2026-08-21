#!/usr/bin/env bash
# Uruchamia wszystkie testy dla QUIC i Netty
# Użycie: ./benchmark/run_all.sh [--docker|--linux] [--transport quic|netty|both]
#
# Przykład Docker:  ./benchmark/run_all.sh --docker --transport both
# Przykład Linux:   ./benchmark/run_all.sh --linux  --transport quic

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ---- Argumenty ----
MODE_ARG="docker"
TRANSPORT_ARG="both"

while [[ $# -gt 0 ]]; do
    case $1 in
        --docker) MODE_ARG="docker" ;;
        --linux)  MODE_ARG="linux"  ;;
        --transport) TRANSPORT_ARG="$2"; shift ;;
        *) echo "Nieznany argument: $1"; exit 1 ;;
    esac
    shift
done

# ---- Załaduj konfigurację ----
source "$SCRIPT_DIR/config/${MODE_ARG}.conf"
source "$SCRIPT_DIR/lib.sh"

# ---- Katalog wyników ----
RESULTS_DIR="$SCRIPT_DIR/results/$(date +%Y%m%d_%H%M%S)_${MODE_ARG}"
mkdir -p "$RESULTS_DIR"
RESULTS_FILE="$RESULTS_DIR/summary.txt"

log() { echo -e "$*" | tee -a "$RESULTS_FILE"; }

# Zmienne wyników (zamiast declare -A, kompatybilne z bash 3)
r_quic_baseline="n/a"
r_quic_follower_new="n/a"
r_quic_leader_new="n/a"
r_quic_leader_connected="n/a"
r_netty_baseline="n/a"
r_netty_follower_new="n/a"
r_netty_leader_new="n/a"
r_netty_leader_connected="n/a"

set_result() {
    local key=$1
    local val=$2
    eval "r_${key}=\"${val}\""
}

get_result() {
    local key=$1
    eval "echo \"\${r_${key}}\""
}

# ================================================================
run_transport_tests() {
    local transport=$1
    local quic_flag=""
    [ "$transport" = "quic" ] && quic_flag="--quic"

    log ""
    print_separator
    log "Transport: $(echo "$transport" | tr '[:lower:]' '[:upper:]')"
    print_separator

    # ----------------------------------------------------------------
    # TEST 1: Baseline — 3 pomiary, mediana
    # ----------------------------------------------------------------
    log "\n[1/4] Baseline (5 inkrementów, bez awarii)"

    local t1 t2 t3
    t1=$(run_client "$transport" 5 "$MAX_BASELINE")
    log "  Pomiar 1: ${t1}s"
    t2=$(run_client "$transport" 5 "$MAX_BASELINE")
    log "  Pomiar 2: ${t2}s"
    t3=$(run_client "$transport" 5 "$MAX_BASELINE")
    log "  Pomiar 3: ${t3}s"

    local baseline
    baseline=$(printf '%s\n' "$t1" "$t2" "$t3" \
        | grep -v "FAIL\|TIMEOUT" | sort -n | awk 'NR==2{print}')
    [ -z "$baseline" ] && baseline="FAIL"

    check_result "Baseline (mediana 3 pomiarów)" "$baseline" "$MAX_BASELINE"
    set_result "${transport}_baseline" "$baseline"

    # ----------------------------------------------------------------
    # TEST 2: Awaria followera — nowy klient
    # ----------------------------------------------------------------
    log "\n[2/4] Awaria followera — nowy klient"

    local leader follower
    leader=$(find_leader)
    follower=""
    for n in "${NODES[@]}"; do
        if [ "$n" != "$leader" ]; then
            follower="$n"
            break
        fi
    done

    if [ -z "$follower" ]; then
        log "  BRAK — nie znaleziono followera"
        set_result "${transport}_follower_new" "FAIL"
    else
        log "  Lider: $leader  |  DROP followera: $follower"
        partition_node "$follower"
        sleep 1

        local t_fol
        t_fol=$(run_client "$transport" 5 "$MAX_FOLLOWER_FAILURE")
        check_result "Awaria followera / nowy klient" "$t_fol" "$MAX_FOLLOWER_FAILURE"
        set_result "${transport}_follower_new" "$t_fol"

        restore_node "$follower"
        sleep 2
    fi

    # ----------------------------------------------------------------
    # TEST 3: Awaria lidera — nowy klient
    # ----------------------------------------------------------------
    log "\n[3/4] Awaria lidera — nowy klient"

    leader=$(find_leader)
    if [ -z "$leader" ]; then
        log "  BRAK lidera — pomijam test"
        set_result "${transport}_leader_new" "FAIL"
    else
        log "  DROP lidera: $leader"
        partition_node "$leader"

        local t_ldr_new
        t_ldr_new=$(run_client "$transport" 5 "$MAX_LEADER_FAILURE_NEW")
        check_result "Awaria lidera / nowy klient" "$t_ldr_new" "$MAX_LEADER_FAILURE_NEW"
        set_result "${transport}_leader_new" "$t_ldr_new"

        restore_node "$leader"
        sleep 3
    fi

    # ----------------------------------------------------------------
    # TEST 4: Awaria lidera — klient już połączony
    # Klient wysyła 20 inkrementów; po 1s DROP lidera
    # ----------------------------------------------------------------
    log "\n[4/4] Awaria lidera — klient już połączony"

    leader=$(find_leader)
    if [ -z "$leader" ]; then
        log "  BRAK lidera — pomijam test"
        set_result "${transport}_leader_connected" "FAIL"
    else
        log "  Startuje klient (20 inkrementów), po 1s DROP lidera: $leader"

        local tmpfile
        tmpfile=$(mktemp)

        # Klient w tle
        client_exec java -cp "$JAR" "$CLIENT_CLASS" \
            20 IO $quic_flag > "$tmpfile" 2>&1 &
        local client_pid=$!

        # Po 1s partycja lidera
        sleep 1
        partition_node "$leader"

        # Czekaj na zakończenie (max MAX_LEADER_FAILURE_CONNECTED s)
        local elapsed=0
        local finished=false
        while [ $elapsed -lt "$MAX_LEADER_FAILURE_CONNECTED" ]; do
            sleep 1
            elapsed=$((elapsed + 1))
            if ! kill -0 "$client_pid" 2>/dev/null; then
                finished=true
                break
            fi
        done

        if [ "$finished" = false ]; then
            kill "$client_pid" 2>/dev/null
            wait "$client_pid" 2>/dev/null
            check_result "Awaria lidera / połączony klient" "TIMEOUT" "$MAX_LEADER_FAILURE_CONNECTED"
            set_result "${transport}_leader_connected" "TIMEOUT"
        else
            wait "$client_pid" || true
            local t_con
            t_con=$(grep "Completed sending" "$tmpfile" \
                | grep -o '[0-9]*\.[0-9]*s' | tr -d 's' || echo "FAIL")
            [ -z "$t_con" ] && t_con="FAIL"
            cat "$tmpfile" >> "${RESULTS_DIR}/output.log" 2>/dev/null
            check_result "Awaria lidera / połączony klient" "$t_con" "$MAX_LEADER_FAILURE_CONNECTED"
            set_result "${transport}_leader_connected" "$t_con"
        fi
        rm -f "$tmpfile"

        restore_node "$leader"
        sleep 3
    fi
}

# ---- Uruchom testy ----
log "Benchmark: QUIC vs Netty — $(date)"
log "Środowisko: $MODE_ARG"

if [ "$TRANSPORT_ARG" = "both" ] || [ "$TRANSPORT_ARG" = "quic" ]; then
    run_transport_tests "quic"
fi

if [ "$TRANSPORT_ARG" = "both" ] || [ "$TRANSPORT_ARG" = "netty" ]; then
    run_transport_tests "netty"
fi

# ================================================================
# Tabela podsumowująca
# ================================================================
log ""
print_separator
log "PODSUMOWANIE"
print_separator
printf "%-38s | %-12s | %-12s\n" "Test" "QUIC" "Netty" \
    | tee -a "$RESULTS_FILE"
printf -- "-%.0s" {1..68} | tee -a "$RESULTS_FILE"; echo | tee -a "$RESULTS_FILE"

print_row() {
    local label=$1 key=$2
    local q n
    q=$(get_result "quic_${key}")
    n=$(get_result "netty_${key}")
    printf "%-38s | %-12s | %-12s\n" "$label" "$q" "$n" \
        | tee -a "$RESULTS_FILE"
}

print_row "Baseline (mediana 3 x 5 inkrementów)"  "baseline"
print_row "Awaria followera / nowy klient"         "follower_new"
print_row "Awaria lidera / nowy klient"            "leader_new"
print_row "Awaria lidera / klient połączony"       "leader_connected"

log ""
log "Wyniki zapisane w: $RESULTS_DIR"
