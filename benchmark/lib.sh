#!/usr/bin/env bash
# Wspólne funkcje dla wszystkich testów

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

PASS="${GREEN}PASS${NC}"
FAIL="${RED}FAIL${NC}"

# Progi czasowe (sekundy) — przekroczenie → FAIL
MAX_BASELINE=5
MAX_FOLLOWER_FAILURE=15
MAX_LEADER_FAILURE_NEW=20
MAX_LEADER_FAILURE_CONNECTED=20

partition_node() {
    local node=$1
    node_exec "$node" sh -c "iptables -A INPUT -j DROP && iptables -A OUTPUT -j DROP" 2>/dev/null
    echo "  [partition] $node zablokowany"
}

restore_node() {
    local node=$1
    node_exec "$node" iptables -F 2>/dev/null
    echo "  [restore]   $node przywrócony"
}

find_leader() {
    # Zwraca nazwę węzła który jest liderem (ostatni becomeLeader w logach)
    get_logs 2>/dev/null \
        | grep "becomeLeader" \
        | tail -1 \
        | grep -oE 'n[0-9]+@' \
        | tr -d '@'
}

# Uruchamia CounterClient z timeoutem
# Wypisuje czas (np. 0.452) lub TIMEOUT / FAIL
run_client() {
    local transport=$1
    local increments=$2
    local timeout_sec=$3

    local quic_flag=""
    [ "$transport" = "quic" ] && quic_flag="--quic"

    local tmpfile
    tmpfile=$(mktemp)

    # Uruchom w tle
    client_exec java -cp "$JAR" "$CLIENT_CLASS" \
        "$increments" IO $quic_flag > "$tmpfile" 2>&1 &
    local pid=$!

    # Czekaj max timeout_sec sekund
    local elapsed=0
    while [ $elapsed -lt "$timeout_sec" ]; do
        sleep 1
        elapsed=$((elapsed + 1))
        if ! kill -0 "$pid" 2>/dev/null; then
            break
        fi
    done

    # Jeśli jeszcze działa — kill → TIMEOUT
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null
        rm -f "$tmpfile"
        echo "TIMEOUT"
        return 1
    fi

    wait "$pid" || true
    local exit_code=$?

    # Zapisz output do logu
    cat "$tmpfile" >> "${RESULTS_DIR}/output.log" 2>/dev/null

    if [ $exit_code -ne 0 ]; then
        rm -f "$tmpfile"
        echo "FAIL"
        return 1
    fi

    # Wyciągnij czas z linii "Completed sending X command(s) in Y.YYYs"
    local time
    time=$(grep "Completed sending" "$tmpfile" \
        | grep -o '[0-9]*\.[0-9]*s' | tr -d 's')
    rm -f "$tmpfile"

    if [ -z "$time" ]; then
        echo "FAIL"
        return 1
    fi

    echo "$time"
}

# Sprawdza wynik i drukuje PASS/FAIL
check_result() {
    local label=$1
    local time=$2
    local max=$3

    if [ "$time" = "TIMEOUT" ] || [ "$time" = "FAIL" ]; then
        printf "  %-42s [" "$label"
        echo -en "$FAIL"
        printf "] (%s)\n" "$time"
        return 1
    fi

    # Porównanie float przez awk (kompatybilne z macOS)
    local ok
    ok=$(awk "BEGIN{print ($time <= $max) ? 1 : 0}")
    if [ "$ok" = "1" ]; then
        printf "  %-42s [" "$label"
        echo -en "$PASS"
        printf "] (%.3fs)\n" "$time"
    else
        printf "  %-42s [" "$label"
        echo -en "$FAIL"
        printf "] (%.3fs > max %ss)\n" "$time" "$max"
        return 1
    fi
}

wait_for_leader() {
    local max_wait=30
    local elapsed=0
    echo -n "  Czekam na lidera"
    while [ $elapsed -lt $max_wait ]; do
        local leader
        leader=$(find_leader)
        if [ -n "$leader" ]; then
            echo " → $leader"
            echo "$leader"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
        echo -n "."
    done
    echo " → BRAK LIDERA po ${max_wait}s"
    return 1
}

print_separator() {
    printf -- '─%.0s' {1..65}
    echo
}
