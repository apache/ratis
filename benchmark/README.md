# Benchmark: Raft na QUIC kontra TCP z TLS w Apache Ratis

To repozytorium jest kopią rozwojową biblioteki [Apache Ratis](https://ratis.apache.org/)
(3.3.0-SNAPSHOT) rozszerzoną o nowy moduł transportowy `ratis-quic` oraz o narzędzie
pomiarowe `RaftBench`. Celem jest ilościowe porównanie dwóch wymiennych transportów
algorytmu konsensusu Raft:

| etykieta w wynikach | co pod nią siedzi | moduł |
|---|---|---|
| `tcp`  | transport referencyjny: TCP z TLS 1.3 (Netty + BoringSSL); warstwę TLS na ścieżce RPC dodano w tej pracy | `ratis-netty` |
| `quic` | protokół QUIC (netty-incubator-codec-quic 0.0.75.Final na bibliotece quiche + BoringSSL) | `ratis-quic` |

Oba transporty realizują te same interfejsy Ratisa (`ServerFactory`, `ClientFactory`,
`RaftServerRpc`, `RaftClientRpc`), używają tej samej serializacji (Protocol Buffers w kopertach
`RaftNettyServerRequestProto` / `RaftNettyServerReplyProto`), tych samych certyfikatów i limitów
czasowych. Wybór transportu to jedna właściwość (`raft.rpc.type`), a w aplikacji przykładowej
flaga `--quic`. Rdzeń algorytmu, maszyna stanów, klient i narzędzie pomiarowe są wspólne.

Szczegóły implementacji modułu QUIC (tagi strumieni, pule wątków, kodeki, konfiguracja) opisuje
[QUIC_IMPLEMENTATION.md](../QUIC_IMPLEMENTATION.md).

---

## 1. Mierzony system

Klaster N serwerów (w macierzy N = 5: lider + 4 followerów) uruchamia `CounterServer`
z maszyną stanów `CounterStateMachine` (`ratis-examples`). Maszyna obsługuje:

- `INCREMENT` z ładunkiem: transakcja przez pełny konsensus, ładunek zapamiętywany pod kluczem
  równym identyfikatorowi workera;
- `GET`: zapytanie obsługiwane lokalnie, zwraca zapamiętany ładunek (odpowiedź ma rozmiar zapisu);
- `PING` -> `PONG <t1> <t2>`: sonda benchmarku do pomiaru czasów jednokierunkowych (patrz 2.3).

### Układ strumieni QUIC

Połączenie serwer-serwer to jedno połączenie QUIC (jedno gniazdo UDP) z trwałymi strumieniami
dwukierunkowymi, po jednym na typ komunikatu. Pierwszy bajt strumienia to tag jego roli:

| tag | wartość | komunikaty | strumień |
|---|---|---|---|
| `TAG_APPEND_ENTRIES`   | 0x00 | `AppendEntries` z wpisami (replikacja) | trwały |
| `TAG_HEARTBEAT`        | 0x01 | `AppendEntries` bez wpisów (sygnał podtrzymania) | trwały |
| `TAG_INSTALL_SNAPSHOT` | 0x02 | `InstallSnapshot` | trwały |
| `TAG_REQUEST_VOTE`     | 0x03 | `RequestVote`, `StartLeaderElection` | trwały |
| `TAG_CLIENT_REQUEST`   | 0x04 | żądania klientów i administracyjne; w trybie klienckim jedyny strumień | trwały |
| `TAG_READ_INDEX`       | 0x05 | `ReadIndex` | krótkotrwały, na jedno żądanie |
| `TAG_PEER_SINGLE`      | 0x06 | wszystkie typy serwer-serwer na jednym strumieniu (wariant kontrolny, `raft.quic.server.single-stream=true`) | trwały |

Dzięki temu heartbeat ani odczyt klienta nie czekają w kolejce za dużą paczką replikacji.
Klient zewnętrzny zawsze otwiera jeden strumień na połączenie.

---

## 2. Narzędzie pomiarowe RaftBench

Klasa `org.apache.ratis.examples.counter.client.RaftBench` używa tego samego `RaftClient`,
tej samej maszyny stanów i tej samej konfiguracji transportu co aplikacja licznika.

### 2.1 Model obciążenia (tryb `rywrites`)

Pętla zamknięta: każdy worker wysyła kolejne żądanie dopiero po odpowiedzi na poprzednie,
więc liczba żądań w locie równa się liczbie workerów. Jedna iteracja to:

1. **zapis** ładunku do lidera (`io().send`): pełna ścieżka konsensusu, tj. dopisanie do logu
   lidera, replikacja, utrwalenie na dysku followerów, potwierdzenie przez większość;
2. **odczyt** własnego klucza z przypisanego followera (`io().sendStaleRead(..., minIndex=0, follower)`):
   obsługa natychmiast z bieżącego stanu, bez konsensusu.

Zapis mierzy ścieżkę konsensusu, odczyt niemal czysty transport z obsługą po stronie followera.
Iteracje rozgrzewkowe (`--warmup`) są odrzucane, a zegar punktu rusza od końca rozgrzewki.
Follower przypisywany jest z globalnego id workera (`id % liczba_followerów`).

Dwa modele połączenia:

- **conn A**: nowe połączenie z pełnym uzgadnianiem TLS na każde żądanie (nowy `RaftClient`);
- **conn B**: jedno połączenie na workera, reużywane przez cały przebieg.

Każde żądanie ma własny `try/catch`; worker po błędzie kontynuuje, a próbki i liczniki
publikuje także po awarii całego workera (bez tego padnięty worker znikałby z wyników razem
ze swoimi udanymi pomiarami). W conn B po zerwanym połączeniu klient jest odtwarzany.

### 2.2 Parametry

```
RaftBench --transport {TCP_TLS|QUIC} --mode {scaling|rywrites}
          --clients FROM:TO:STEP --payload SIZE --requests N --conn {A|B}
          [--warmup W] [--csv FILE] [--run-id S] [--rep N]
          [--worker-offset K] [--lat-file F]
          [--read-ratio R --read-from {leader|followers}]   # tylko tryb scaling
```

| flaga | znaczenie |
|---|---|
| `--clients 6:6:1` | liczba workerów w procesie (sweep OD:DO:KROK) |
| `--payload 1B / 1kB / 1MB` | rozmiar ładunku (1kB = 1024 B, 1MB = 1024*1024 B) |
| `--requests` | mierzone iteracje na workera |
| `--worker-offset K` | pierwszy id workera w tym procesie; kilka procesów RaftBench na jednym klastrze musi mieć rozłączne id, bo id jest kluczem w maszynie stanów |
| `--lat-file F` | zrzut surowych próbek (`w <ns>`, `r <ns>`, `hop <rtt> <c2l> <l2c>`), scalany później przez `scal.sh` |
| `--run-id`, `--rep` | znaczniki przebiegu i powtórzenia w wierszu CSV |

Tryb `scaling` (czytelnicy/pisarze wg `--read-ratio`) jest używany tylko do dodatkowego pomiaru
przepustowości odczytów (`READTPUT=1`, sekcja 5).

### 2.3 Sonda hop (czasy jednokierunkowe bez synchronizacji zegarów)

Równolegle z obciążeniem osobny wątek co ~100 ms wysyła do lidera `PING` ścieżką stale read.
Serwer odpowiada `PONG <t1> <t2>` (swoje `nanoTime` przy wejściu do zapytania i przy budowie
odpowiedzi). Z czterech znaczników `{t0,t1,t2,t3}` liczone są RTT oraz czasy klient->lider
i lider->klient rachunkiem jak w NTP: przesunięcie zegarów wyznaczają dwie kotwice (próbki
o najmniejszym RTT z pierwszej i drugiej połowy przebiegu) interpolowane liniowo w czasie.

Na liderze `LogAppenderDefault` wypisuje na stdout linie `HOPSTAT <follower> <liczba> <suma ns>`
(RTT `AppendEntries` z wpisami, heartbeaty pomijane); skrypty liczą z nich deltę na punkt.

### 2.4 Wiersz CSV procesu RaftBench

Nowe kolumny są zawsze dopisywane na końcu, żeby indeksy w `awk` się nie przesuwały:

```
run_id,rep,transport,cluster_size,mode,total,writers,readers,payload_bytes,conn,read_from,
duration_s,write_tput_req_s,write_MB_s,write_p50_ms,write_p99_ms,
read_tput_req_s,read_MB_s,read_p50_ms,read_p99_ms,
write_mean_ms,write_stddev_ms,read_mean_ms,read_stddev_ms,
requests_sent,requests_committed,requests_failed,conn_failed,reads_ok,reads_failed,
hop_c2l_ms,hop_l2c_ms,hop_rtt_ms
```

`requests_committed` = liczba próbek zapisu (próbka powstaje tylko z udanego zapisu), więc
`requests_failed = sent - committed` z konstrukcji. `conn_failed` zlicza wyjątki sklasyfikowane
jako zerwanie połączenia (timeout żądania nim nie jest).

---

## 3. Macierz eksperymentu

Pełna macierz czynnikowa o czterech osiach, klaster stale N = 5:

| oś | wartości |
|---|---|
| transport | `quic`, `tcp` (TCP z TLS) |
| model połączenia | conn A (nowe połączenie na żądanie), conn B (reużywane) |
| ładunek | 1 B, 1 kB, 1 MB |
| obciążenie | 1..6 workerów na każdym z 5 węzłów klienckich = 5, 10, 15, 20, 25, 30 klientów |

Razem 72 punkty pomiarowe (216 przy trzech powtórzeniach). Na workera przypada 1000 iteracji,
przy 1 MB 200. Oba transporty mierzone są w jednej rezerwacji, klaster jest restartowany przy
zmianie transportu (osobne "bloki"), a układy klientów idą jeden po drugim w tych samych warunkach.

### Metryki na punkt (jeden wiersz `wyniki.csv`)

- opóźnienie zatwierdzenia zapisu u klienta: średnia, mediana, odchylenie, p99;
- przepustowość zatwierdzonych zapisów [zapisy/s];
- opóźnienie odczytu z followera: średnia, mediana, odchylenie, p99;
- liczniki kontrolne: żądania wysłane / zatwierdzone / nieudane, zerwane połączenia, odczyty udane / nieudane;
- elekcje lidera i próby elekcji (z logów serwerów, delta na punkt);
- rozbicie czasu: klient->lider, lider->klient (sonda hop) oraz RTT `AppendEntries` lider->follower (HOPSTAT);
- czas trwania pomiaru i czas ścienny punktu.

Percentyle i odchylenia liczone są przez `scal.sh` z **połączonej puli surowych próbek** ze
wszystkich węzłów klienckich (plików `lat_*.txt`), nigdy jako średnia percentyli między
procesami. Przepustowość to suma przepustowości procesów, liczniki to sumy.

Kolumny scalonego `wyniki.csv`:

```
run_id,seq,block,layout,rep,transport,cluster_size,payload_bytes,conn,clients_total,client_nodes,
requests_per_client,warmup,mode,
commit_mean_ms,commit_p50_ms,commit_stddev_ms,commit_p99_ms,commit_tput_req_s,
duration_s,point_wall_s,requests_sent,requests_committed,requests_failed,conn_failed,elections,
read_mean_ms,read_p50_ms,read_stddev_ms,read_p99_ms,read_tput_req_s,reads_ok,reads_failed,
hop_client_leader_ms,hop_server_server_ms,hop_leader_client_ms,candidate_attempts
```

`seq` = kolejność wykonania, `block` = `<nr>-<transport>` (jeden cykl życia serwerów),
`layout` = rozkład workerów po węzłach (`6-6-6-6-6`).

---

## 4. Środowisko: klaster DCC Politechniki Poznańskiej (SLURM)

- węzeł dostępowy `dcc.cs.put.poznan.pl`, rezerwacje przez SLURM (`salloc`, `squeue`, `scancel`);
- 16 węzłów `dcc-1..16`, Intel Core i7-12700, 1 GbE: `dcc-1..8` mają 8 GB RAM, `dcc-9..16` 4 GB;
- w macierzy: 5 serwerów z puli 8 GB, 5 węzłów klienckich z puli 4 GB (jeden proces RaftBench na węzeł);
- węzły są wyłączone do czasu alokacji (start do ~3 min), po 30 min bezczynności gasną;
- brak Javy na węzłach: własne JDK 21 w katalogu domowym (NFS), tam też `ratis.jar`
  i natywna biblioteka quiche dla Linuksa (`netty-quiche-linux.jar`, jar z macOS jej nie zawiera);
- log Rafta na lokalnym dysku węzła (`/data/...`), nie na NFS;
- certyfikaty serwera ECDSA P-256 (podpis RSA-4096 kosztował różnie w obu buildach BoringSSL
  i zaniżał wynik QUIC o różnicę bibliotek, nie protokołów).

Rezerwacje od 30 min do 9 h w zależności od rozmiaru przebiegu; `matrix6.sh` tnie macierz
na kawałki mieszczące się w budżecie rezerwacji.

---

## 5. Jak uruchomić

Pełna procedura krok po kroku (logowanie, VPN, budowanie jara, pułapki) jest w
[RUNBOOK.md](RUNBOOK.md); wersja "gotowe komendy" w [RUNBOOK-SKRYPTY.md](RUNBOOK-SKRYPTY.md);
tryb wielu węzłów klienckich w [RUNBOOK-KLIENCI.md](RUNBOOK-KLIENCI.md).

### 5.1 Budowanie (laptop)

```bash
# przed buildem zamknij IDE z autobuildem Javy (RUNBOOK §3.1 / §9)
find . -path '*/target/classes' -prune -exec rm -rf {} +
find . -path '*/target/test-classes' -prune -exec rm -rf {} +
find . -path '*/target/*.jar' -delete
GITHUB_ACTIONS=true ./mvnw -pl ratis-examples -am install \
  -DskipTests -Dcheckstyle.skip=true -Dspotbugs.skip=true -Dlicense.skip=true
rm -f ratis-examples/target/*.jar
GITHUB_ACTIONS=true ./mvnw -pl ratis-examples package \
  -DskipTests -Dcheckstyle.skip=true -Dspotbugs.skip=true -Dlicense.skip=true
# wynik: ratis-examples/target/ratis-examples-3.3.0-SNAPSHOT.jar (fat jar)
```

### 5.2 Smoke test lokalny (bez klastra)

```bash
N=3 TR=quic  bash benchmark/local/run_local_n.sh
N=3 TR=netty bash benchmark/local/run_local_n.sh
N=3 TR=quic SINGLE_STREAM=1 bash benchmark/local/run_local_n.sh   # wariant jednostrumieniowy
N=3 TR=netty HB_THREAD=1    bash benchmark/local/run_local_n.sh   # heartbeaty z osobnego wątku
```

### 5.3 Klaster DCC

```bash
# 1. wgraj jar i skrypty (RUNBOOK §3.3)
scp ratis-examples/target/ratis-examples-3.3.0-SNAPSHOT.jar <login>@dcc.cs.put.poznan.pl:ratis.jar
scp benchmark/lan/{run_matrix.sh,scal.sh,matrix6.sh,alloc.sh,log4j.properties} <login>@dcc.cs.put.poznan.pl:

# 2. na węźle dostępowym: plan macierzy (nie dotyka klastra)
REQ_1MB=200 bash ~/matrix6.sh plan

# 3. kawałek po kawałku, każdy we własnej rezerwacji (-t wg planu)
salloc --no-shell -p dcc -N 10 -t 00:30:00
bash ~/matrix6.sh 1
scancel <jobid>
```

`run_matrix.sh` można też wywołać bezpośrednio, np. jeden układ i jeden ładunek:

```bash
SERVER_NODES="dcc-1 dcc-2 dcc-3 dcc-4 dcc-5" CLIENT_NODES="dcc-9 dcc-10 dcc-11 dcc-12 dcc-13" \
CLIENT_LAYOUTS='"1 1 1 1 1" "6 6 6 6 6"' SIZES=5 PAYLOADS=1kB CONNS="A B" \
TRANSPORTS="quic tcp" REQUESTS=1000 REPEATS=1 bash ~/run_matrix.sh
```

Najważniejsze zmienne `run_matrix.sh`:

| zmienna | znaczenie |
|---|---|
| `SIZES`, `PAYLOADS`, `CONNS`, `TRANSPORTS`, `REQUESTS`, `WARMUP`, `REPEATS` | osie sweepu |
| `CLIENT_LAYOUTS='"1 1 1 1 1" "2 2 2 2 2"'` | rozkłady workerów po węzłach klienckich, jeden proces na węzeł, `--worker-offset` narastająco |
| `SERVER_NODES`, `CLIENT_NODES` | jawny podział węzłów rezerwacji; bez nich reguła pozycyjna |
| `DRYRUN=1` | tylko plan punktów i szacunek czasu |
| `JAVA`, `CLIENT_JAVA` | JVM serwerów / klientów (klientom wolno ograniczyć pamięć, serwerów nie ruszamy) |
| `RUN_ID` | wspólny katalog `~/raft-results/<RUN_ID>` dla wielu rezerwacji |

Skrypt tylko mierzy: nigdy nie rezerwuje ani nie zwalnia węzłów (od tego jest `alloc.sh`).

### 5.4 Docker (funkcjonalnie, nie do pomiarów)

Klaster w kontenerach i testy awarii followera/lidera z partycją `iptables DROP` opisuje
[README-awarie-docker.md](README-awarie-docker.md) oraz [../docker/README.md](../docker/README.md).

---

## 6. Warianty i eksperymenty uzupełniające

Wszystkie są domyślnie wyłączone; bez flag serwer zachowuje się jak w macierzy głównej.
Etykiety transportów w CSV nie zmieniają się, wariant rozpoznaje się po `RUN_ID`
(sufiksy `_1s`, `_hb`, `_ram`, `_t<MIN>`, `_np`), wpisie w `meta.txt` i nagłówku `wyniki.csv`.

| zmienna skryptu | flaga / właściwość serwera | co zmienia |
|---|---|---|
| `QUIC_SINGLE_STREAM=1` | `--quic --single-stream` = `raft.quic.server.single-stream=true` | QUIC z jednym strumieniem na połączenie serwer-serwer (jak jedno połączenie TCP); ten sam stos, inny tylko układ strumieni. Kontrola wkładu podziału na strumienie |
| `HB_THREAD=1` | `--hb-thread` = `raft.server.log.appender.heartbeat.thread=true` | osobny wątek wysyła heartbeat, gdy przez połowę minimalnego limitu elekcji nic nie poszło do followera; heartbeat leci **obok** paczki `AppendEntries` / fragmentu migawki. Oba transporty. Klasa `LogAppenderWithHeartbeatThread`, rejestr zmian w [HB-THREAD-CHANGES.md](../HB-THREAD-CHANGES.md) |
| `RPC_TIMEOUT=MIN,MAX` | `--rpc-timeout=MIN,MAX` (ms) | limit elekcji obu transportów (domyślnie 150,300; heartbeat co MIN/2) |
| `NO_PREVOTE=1` | `--no-prevote` | klasyczny Raft bez fazy pre-vote |
| `FGAP_MS=<ms>` | `-Dratis.fgap.threshold.ms` | follower loguje `FGAP <id> <ms> <typ>`: odstępy między komunikatami od lidera >= progu |
| `SERVER_JAVA_OPTS="-Dratis.appender.buffer=8MB -Dratis.log.write.buffer=32MB"` | właściwości JVM serwerów | rozmiar paczki replikacji (write buffer musi być > appender buffer + 8 B) |
| `RAM_LOG=1` | storage serwerów na tmpfs (`/dev/shm`) | wariant kontrolny bez zapisu trwałego: z rundy `AppendEntries` znika `fsync`, zostaje sieć i obsługa. Tylko 1 B i 1 kB |
| `READTPUT=1` | RaftBench `--mode scaling --read-ratio 1.0` | po punktach bloku: sami czytelnicy, stale read 1 MB z followerów, bez konsensusu i dysku (`readtput.csv`) |

Gotowe scenariusze:

- `benchmark/lan/pilot_hb.sh`: pilot stabilności przywództwa (~15 min): 30 workerów x 1 MB,
  conn B, `HB_THREAD=1`, trzy warianty `tcp` / `quic` / `quic1s` w jednym punkcie, na końcu
  tabela z elekcjami, RTT heartbeatu i rozkładem FGAP;
- `benchmark/lan/szukaj_dysk8.sh`: szuka limitu elekcji, przy którym TCP jeszcze zatwierdza
  zapisy, ale traci lidera (połowienie przedziału), i mierzy w nim trzy warianty z powtórzeniami;
- `benchmark/lan/fgap_stats.py`: podsumowanie FGAP/HBSTAT z pobranych logów.

---

## 7. Gdzie lądują wyniki

Na klastrze `~/raft-results/<RUN_ID>/`:

| plik | zawartość |
|---|---|
| `wyniki.csv` | jeden scalony wiersz na punkt (kolumny w sekcji 3) |
| `meta.txt` | parametry przebiegu (węzły, warianty, opcje JVM serwerów) |
| `point_*.env` | metadane punktu + delty liczników serwerowych (elekcje, HOPSTAT) |
| `lat_*.txt` | surowe próbki z każdego węzła klienckiego (materiał źródłowy, zostaje na stałe) |
| `<tr>_rywrites_conn<A lub B>_<układ>_<węzeł>.csv`, `bench_*.log` | wiersze i logi procesów RaftBench (po jednym na węzeł kliencki) |
| `<tr>_n<N>_server<i>.log` | logi serwerów bloku (HOPSTAT, FGAP, HBSTAT, wpisy elekcji) |
| `readtput.csv` | tylko z `READTPUT=1` |

Ponowne scalenie offline: `for f in ~/raft-results/<RUN_ID>/point_*.env; do bash ~/scal.sh "$f"; done`
(ten sam klucz punktu nadpisuje wiersz, nie dubluje).

W repozytorium `benchmark/results/` przechowuje arkusz zbiorczy macierzy
(`QUIC_vs_TCP_macierz_<data>.xlsx`) oraz CSV wariantu kontrolnego z logiem w pamięci.

---

## 8. Główne wyniki macierzy (N = 5, 72 punkty, 1 przebieg na punkt)

Skrót ustaleń z pracy magisterskiej, dla której powstał ten benchmark:

- **Zapis małych wpisów nie zależy od transportu.** Wspólny sufit ok. 460 zapisów/s (conn A)
  i RTT `AppendEntries` 8-11 ms u obu transportów; czas rundy to głównie utrwalenie na dysku
  followera, którego algorytm wymaga, sieć to ułamki milisekundy.
- **Ogon odczytu z followera jest 2-4 razy niższy dla QUIC** przy każdym ładunku i od 10 klientów
  wzwyż (conn B, 1 kB: p99 2-3 ms wobec 8-10 ms). p99 TCP równa się czasowi jednej rundy replikacji
  (stosunek 0,95-1,3), bo odczyt klienta czeka na obsługę paczki od lidera w tej samej grupie
  pętli zdarzeń; przy osobnych strumieniach QUIC stosunek wynosi 0,27-0,38. Mediany są zbliżone,
  TCP nieznacznie szybszy.
- **1 MB, połączenie reużywane: TCP z TLS o ok. 25 % wydajniejszy** (16,0 wobec 12,8 zapisów/s),
  bo segmentację i potwierdzenia wykonuje jądro, a QUIC szyfruje i wysyła każdy pakiet z przestrzeni
  użytkownika.
- **1 MB, nowe połączenie na żądanie: zapaść przeciążeniowa TCP** od 20 klientów (spadek do
  8,6 zapisów/s, nieudane żądania, p99 zatwierdzenia do 9,8 s), QUIC stałe 12,5-13,3 zapisów/s bez
  błędów.
- **Koszt uzgodnienia w sieci lokalnej** 4,2-4,9 ms (TCP z TLS 1.3, 2 RTT) wobec 5,3-6,1 ms
  (QUIC, 1 RTT): zaoszczędzony obieg jest wart mniej niż koszt kryptografii i stanu połączenia.
- **Stabilność przywództwa** (eksperyment uzupełniający z `HB_THREAD=1`, paczki 8 MB):
  heartbeat na osobnym strumieniu QUIC wraca po ~3 ms niezależnie od paczki, w TCP po 150-180 ms;
  przy limicie 120/240 ms TCP traci lidera, QUIC z pięcioma strumieniami nie odnotował żadnej
  próby elekcji, a QUIC z jednym strumieniem zachowuje się jak TCP.

---

## 9. Mapa plików

```
ratis-quic/                       moduł transportu QUIC (QuicFactory, QuicRpcService, QuicRpcProxy,
                                  QuicClientRpc, QuicConfigKeys, kodeki protobuf)
ratis-netty/                      transport referencyjny + TLS na ścieżce RPC + opcja wątku heartbeatów
ratis-server/.../leader/          LogAppenderDefault (HOPSTAT), LogAppenderWithHeartbeatThread
ratis-server/.../impl/FollowerState.java   diagnostyka FGAP
ratis-examples/.../counter/       CounterServer (flagi --quic, --single-stream, --hb-thread,
                                  --rpc-timeout, --no-prevote), CounterStateMachine (PING/PONG),
                                  RaftBench
benchmark/lan/                    alloc.sh, run_matrix.sh, matrix6.sh, scal.sh, run_lan.sh,
                                  pilot_hb.sh, szukaj_dysk8.sh, fgap_stats.py, log4j.properties
benchmark/local/run_local_n.sh    smoke test na jednej maszynie
benchmark/results/                arkusz macierzy, CSV wariantu kontrolnego
benchmark/RUNBOOK*.md             procedury na klastrze
docker/                           klaster w kontenerach do testów funkcjonalnych
QUIC_IMPLEMENTATION.md            opis implementacji modułu QUIC
HB-THREAD-CHANGES.md              rejestr zmian wariantu z wątkiem heartbeatów i pilotów elekcji
LINUX_INSTALLATION.md             uruchomienie na dowolnych maszynach Linux bez SLURM
```
