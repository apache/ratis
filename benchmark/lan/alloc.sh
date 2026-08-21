#!/usr/bin/env bash
#
# Rezerwacja wezlow DCC (SLURM) - OSOBNO od pomiaru.
# Sens rozdzielenia: rezerwujesz RAZ, a potem odpalasz run_lan.sh ile razy chcesz
# bez placenia za kazdym razem ~3 min na budzenie wezlow i bez oddawania ich na koniec.
#
# Uruchamiaj NA WEZLE DOSTEPOWYM klastra (tam, gdzie sa salloc/squeue/scontrol).
#
#   bash alloc.sh                      # zarezerwuj (domyslnie pod SIZES="3 5 7" + 1 klient)
#   SIZES="3 5" bash alloc.sh          # tyle wezlow, ile trzeba na najwieksze N
#   NODES=6 TIME_LIMIT=02:00:00 bash alloc.sh   # albo wprost: 6 wezlow na 2h
#   bash alloc.sh status               # co aktualnie trzymam
#   bash alloc.sh free                 # zwolnij
#
# JOBID ladue do ~/.raft-alloc, skad run_lan.sh podnosi go SAM - nie musisz
# przeklejac numerow. Dopoki plik wskazuje na zywa rezerwacje, run_lan.sh
# jej NIE zwalnia po zakonczeniu pomiarow.
set -euo pipefail

ALLOC_FILE="${ALLOC_FILE:-$HOME/.raft-alloc}"

SIZES="${SIZES:-3 5 7}"              # do wyliczenia, ile wezlow trzeba
NUM_CLIENT_NODES="${NUM_CLIENT_NODES:-1}"
PARTITION="${PARTITION:-dcc}"
# dcc-1..8 = 8GB RAM, dcc-9..16 = 4GB. Trzymamy sie 1-8, zeby sprzet byl jednorodny
# miedzy punktami sweepu. NODELIST="" -> SLURM wybiera sam.
NODELIST="${NODELIST:-dcc-[1-8]}"
TIME_LIMIT="${TIME_LIMIT:-00:30:00}"   # na dluzszy sweep podaj wiecej, np. TIME_LIMIT=04:00:00
JOB_NAME="${JOB_NAME:-raftbench}"
BOOT_TIMEOUT="${BOOT_TIMEOUT:-300}"    # wybudzenie wezla trwa do ~3 min

MAX_N=0; for n in $SIZES; do [ "$n" -gt "$MAX_N" ] && MAX_N=$n; done
NODES_WANTED="${NODES:-$((MAX_N + NUM_CLIENT_NODES))}"

saved_jobid() { [ -f "$ALLOC_FILE" ] && sed -n 's/^JOBID=//p' "$ALLOC_FILE" | head -1 || true; }
job_state()   { squeue -h -j "$1" -o %T 2>/dev/null || true; }

do_status() {
  local id st
  id=$(saved_jobid)
  [ -n "$id" ] || { echo "Brak zapisanej rezerwacji ($ALLOC_FILE nie istnieje)."; return 0; }
  st=$(job_state "$id")
  if [ -z "$st" ]; then
    echo "JOBID=$id z $ALLOC_FILE juz nie istnieje (wygasl albo zostal zwolniony)."
    echo "Odpal 'bash alloc.sh' zeby zarezerwowac na nowo."
    return 0
  fi
  echo "JOBID=$id  stan=$st"
  # -u $USER, a nie -j $id: pokazuje WSZYSTKIE Twoje rezerwacje, wiec wylapie tez
  # osierocona, o ktorej zapomniales. Kolumna TIME_LEFT = ile czasu zostalo.
  squeue -u "$USER" -o "%.8i %.12j %.10M %.10L %R"
  [ "$st" = "RUNNING" ] && echo "Wezly: $(scontrol show hostnames "$(squeue -h -j "$id" -o %N)" | tr '\n' ' ')"
  return 0
}

do_free() {
  local id
  id=$(saved_jobid)
  [ -n "$id" ] || { echo "Nic do zwolnienia."; return 0; }
  echo ">> scancel $id"
  scancel "$id" 2>/dev/null || true
  rm -f "$ALLOC_FILE"
  echo "Zwolnione, $ALLOC_FILE usuniety."
}

do_alloc() {
  local id st out t=0
  id=$(saved_jobid)
  if [ -n "$id" ]; then
    st=$(job_state "$id")
    if [ -n "$st" ]; then
      echo "Rezerwacja $id juz istnieje (stan=$st) - nie robie drugiej."
      echo "Zwolnij ja przez 'bash alloc.sh free', jesli chcesz inna."
      do_status
      return 0
    fi
    echo ">> $ALLOC_FILE wskazywal na nieistniejacy JOBID=$id, rezerwuje na nowo"
  fi

  echo ">> salloc: $NODES_WANTED wezlow, partycja $PARTITION, limit $TIME_LIMIT"
  if [ -n "$NODELIST" ]; then
    out=$(salloc --no-shell -p "$PARTITION" -w "$NODELIST" -N "$NODES_WANTED" \
            -t "$TIME_LIMIT" -J "$JOB_NAME" 2>&1) || { echo "$out"; exit 1; }
  else
    out=$(salloc --no-shell -p "$PARTITION" -N "$NODES_WANTED" \
            -t "$TIME_LIMIT" -J "$JOB_NAME" 2>&1) || { echo "$out"; exit 1; }
  fi
  echo "$out"
  id=$(sed -n 's/.*Granted job allocation \([0-9][0-9]*\).*/\1/p' <<<"$out")
  [ -n "$id" ] || { echo "!! nie udalo sie odczytac JOBID z wyjscia salloc"; exit 1; }

  echo ">> czekam az wezly wstana (max ${BOOT_TIMEOUT}s)"
  while :; do
    st=$(job_state "$id")
    [ "$st" = "RUNNING" ] && break
    [ -z "$st" ] && { echo "!! rezerwacja $id zniknela"; exit 1; }
    sleep 2; t=$((t+2))
    [ "$t" -ge "$BOOT_TIMEOUT" ] && { echo "!! stan '$st', nie RUNNING po ${t}s"; scancel "$id"; exit 1; }
  done

  local nodes
  nodes=$(scontrol show hostnames "$(squeue -h -j "$id" -o %N)" | tr '\n' ' ')
  { echo "JOBID=$id"; echo "NODES=$nodes"; echo "TIME_LIMIT=$TIME_LIMIT"; } > "$ALLOC_FILE"

  echo
  echo "Gotowe. JOBID=$id, wezly: $nodes"
  echo "Zapisane w $ALLOC_FILE - run_lan.sh podniesie to sam."
  echo
  echo "Teraz mozesz odpalac pomiary ile razy chcesz, np.:"
  echo "  SIZES=3 TRANSPORTS=quic CONNS=B REPEATS=1 REQUESTS=50 bash ~/run_lan.sh"
  echo "  nohup env SIZES=\"3 5\" bash ~/run_lan.sh > ~/sweep.log 2>&1 &"
  echo
  echo "Na koniec ZWOLNIJ wezly:  bash alloc.sh free"
}

case "${1:-alloc}" in
  alloc|"")  do_alloc  ;;
  status)    do_status ;;
  free)      do_free   ;;
  *) echo "Uzycie: bash alloc.sh [alloc|status|free]"; exit 1 ;;
esac
