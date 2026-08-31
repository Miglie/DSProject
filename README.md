# Progetto Distributed Systems

Infrastruttura per gestire job inviati a un cluster di worker, con join dinamico dei nodi e load balancing tramite gossip protocol. Tutto esposto via Java RMI.

## Architettura

- `com.progetto.job` — modello dati: `Task` (lavoro grezzo: `type` + `payload`), `Job` (Task + jobId + stato), `JobStatus` (`PENDING`/`RUNNING`/`COMPLETED`/`FAILED`), `JobResult`.
- `com.progetto.rmi` — interfacce remote: `WorkerRemote` (API client-facing: `submitJob`/`getStatus`/`getResult`, più le API di membership `registerPeer`/`getKnownPeers`) e `GossipRemote` (API worker-to-worker: `exchangeState`/`forwardJob`). `forwardJob` è un ack non bloccante: il risultato di un job inoltrato viene poi recuperato dal nodo d'origine via `getResult`. `exchangeState` porta anche id e stub del chiamante, così ogni round di gossip vale come annuncio di membership.
- `com.progetto.gossip` — `WorkerView` (snapshot versionato del carico di un worker), `ClusterState` (mappa mergeable workerId → WorkerView, ultimo scrivente vince per versione), `GossipService` (round di gossip periodico e decisione di forwarding).
- `com.progetto.worker` — `Worker`: implementa sia `WorkerRemote` che `GossipRemote` sullo stesso oggetto esportato/bindato.
- `com.progetto.client` — `Client`, in due modalità: *demo* (un job per tipo) e *stress* (un burst di job per generare squilibrio di carico e osservare il forwarding).
- `src/test/java` — suite JUnit 5 che rispecchia i package sopra; vedi [Test automatici](#test-automatici).

### Come funziona

1. **Join dinamico**: un worker parte da solo, oppure indicando un nodo seed già attivo. Al join, scarica la lista di peer conosciuti dal seed e fa handshake (`registerPeer`) con tutti — niente configurazione statica del cluster.
2. **Gossip del carico**: ogni worker, ogni 2 secondi, sceglie un peer a caso tra quelli conosciuti e scambia il proprio `ClusterState` (push-pull in una chiamata RMI). Ogni voce del cluster porta un contatore di versione: chi ha la versione più alta vince il merge.
3. **Load balancing**: quando arriva un nuovo job, il worker controlla se un peer noto ha un carico significativamente più basso del proprio (soglia: differenza > 2). Se sì, il job viene inoltrato in background e il client continua a interrogare **solo** il worker a cui si è connesso all'inizio — mai il worker che lo esegue realmente. Ogni forwarding *prenota* subito uno slot sul peer scelto: dato che il gossip aggiorna il carico solo ogni 2s, senza la prenotazione tutti i job di una raffica vedrebbero la stessa foto "quel peer è scarico" e finirebbero tutti sullo stesso nodo.Inoltre il bilanciamento del carico è gestito anche tramite un meccanismo di Work Stealing dinamico e reattivo, basato sull'interazione continua tra l'esecutore locale e il protocollo di Gossip. Quando un worker scende sotto la media di carico del cluster, consulta lo stato del cluster memorizzato nel ClusterState per individuare il peer con la coda di lavoro più carica. A quel punto, il nodo scarico prende l'iniziativa ed effettua un'invocazione RMI diretta verso il worker selezionato per prelevare ed estrarre uno dei job nello stato PENDING. Il task trasferito viene così preso in carico ed eseguito dal worker che ha effettuato lo stealing (o ulteriormente rubato), il quale, una volta completata l'elaborazione, restituisce il risultato finale tramite callback RMI, garantendo un'efficiente redistribuzione delle risorse senza appesantire il nodo d'origine. Il callback è asincrono e il vero esecutore del Job è totalmente trasparente all'origine.
4. **Failure detection del nodo esecutore**: quando un Job viene inoltrato (forwardJob) o rubato (stealJob) da un altro nodo, lo status del Job passa a DELEGATED. Ogni worker ha un thread che si occupa di rilevare se un Job è in status DELEGATED da troppo tempo. Ciò significa che qualcosa è andato storto, e il worker remoto non è in grado di portare a termine il task, che viene dunque preso in carico localmente. La scelta del parametro di timeout è di grande importanza per il buon funzionamento del sistema e va valutata in base al tipo di task che il sistema esegue: troppo grande potrebbe aumentare di molto la latenza, troppo piccolo invece rischierebbe di rendere inutile il lavoro svolto dai nodi remoti in caso di task piuttosto lunghi.
5. **Resilienza minima**: un peer viene rimosso dalla lista dei peer e dal cluster state quando risulta irraggiungibile (connection refused o heartbeat scaduto). Solo in quel caso il job torna in esecuzione locale. L'espulsione non è definitiva (punto 6) e la semantica di esecuzione che ne risulta è discussa al punto 7.
6. **Failure detection disaccoppiata**: il controllo dei guasti opera in modo passivo e indipendente dall'I/O di rete per evitare che ritardi o blocchi di rete congelino l'intero ciclo di controllo:
   - **Architettura non bloccante**: lo `scheduler` principale del gossip gira su un unico thread dedicato. Le chiamate RMI uscenti (`exchangeState`) vengono inoltrate in modo asincrono a un pool di thread I/O (`gossipExecutor`).
   - **Failure Detection passiva**: ad ogni round di 2 secondi, `detectPeerFailure()` verifica l'anzianità dei timestamp di heartbeat nel `ClusterState`. Se un peer non aggiorna il proprio stato da oltre `CRASHED_WORKER_THRESHOLD_MS` (6000 ms), viene espulso da `peers` senza bloccare i cicli di gossip tra i nodi superstiti.
   - **Ri-ammissione (anti-entropy sulla membership)**: la failure detection può solo *togliere* peer, quindi serve un percorso simmetrico per rimetterli. Ogni `exchangeState` porta id e stub del chiamante, e il ricevente lo ri-registra: un nodo che era solo rallentato rientra da solo al primo round utile, senza protocolli aggiuntivi. Senza questo, chi veniva espulso per un blocco temporaneo spariva per sempre e il cluster restava spaccato.
   - **Rilevamento dello stallo locale**: se un round arriva con più di `SCHEDULER_STALL_THRESHOLD_MS` di ritardo, il blocco eravamo noi (JVM congelata, GC lungo, SIGSTOP): tutti gli heartbeat sembrano vecchi per colpa nostra. In quel round la failure detection viene saltata e i timestamp vengono rinfrescati in blocco, altrimenti il nodo appena risvegliato espellerebbe l'intero cluster in un colpo solo.
   - **Restart con lo stesso id**: `registerPeer` sostituisce sempre lo stub (un processo riavviato è esportato su una porta anonima diversa, quello vecchio è morto), e il version counter viene rialzato sopra la versione che i peer conservano di noi — altrimenti dopo un riavvio ogni nostro aggiornamento perderebbe il confronto last-writer-wins e il nostro carico resterebbe congelato al valore pre-crash.
7. **Semantica di esecuzione — at-least-once, e perché non può essere exactly-once**: il fallback locale scatta quando il peer è *irraggiungibile*, cioè esattamente nella situazione in cui non si può distinguere "è morto" da "è lento o partizionato ma sta eseguendo il job". Senza un servizio di coordinamento esterno (lease, fencing token) l'exactly-once attraverso una partizione non è ottenibile: è un limite teorico, non una mancanza di questa implementazione. Il sistema garantisce quindi **at-least-once**, e tre proprietà lo rendono innocuo:
   - **Assunzione sui task**: i task sono deterministici e privi di effetti collaterali — `SUM`, `SLEEP` e `MATRIX_MULT` lo sono. Sotto questa assunzione una doppia esecuzione spreca CPU ma è *osservazionalmente equivalente* all'exactly-once, perché produce lo stesso risultato. Task con effetti collaterali violerebbero l'assunzione e dovrebbero farsi carico da soli dell'idempotenza.
   - **Risposta univoca al client**: il nodo d'origine pubblica una sola `JobResult` per job — quella del peer se il risultato viene comunicato in push-mode dal nodo esecutore, altrimenti quella della propria riesecuzione. Il risultato calcolato da un peer che torna in vita resta nella sua mappa locale e non viene letto da nessuno. Il client non vede mai due risposte né una risposta incoerente.
   - **Deduplica locale**: ogni worker tiene traccia dei jobId che ha accettato per l'esecuzione locale (`locallyAccepted`) e ignora una seconda consegna dello stesso job. Chiude il caso "stesso job accodato due volte sullo stesso nodo"; non chiude — e non può chiudere — quello di due nodi diversi che lo eseguono entrambi. Il marcatore è volutamente distinto dalla mappa `jobs`: su chi ha inoltrato un job quella mappa lo contiene già pur non avendolo mai accodato, quindi usarla come guardia sopprimerebbe proprio il fallback locale.
8. **Stable storage su disco**: per aumentare la resilienza in caso di crash ogni nodo salva su disco un Write-Ahead-Log con informazioni relative ai Job e ai JobResult. Ogni nodo è considerato responsabile per i job che vengono a lui sottomessi, dunque nel WAL vengono salvate informazioni solamente sui Job relativi al worker in questione e sui loro cambi di status. Quando un nodo recupera da un crash quindi rimette in coda tutti i job che risultavano ancora pending (sia quelli che erano nella coda locale sia quelli delegati ai worker remoti), per assicurarne il completamento. Inoltre dal log vengono estratti tutti i risultati dei job già completati, che vengono nuovamente resi disponibili per un eventuale poll del client.

Non ancora implementato: stable storage su disco, recovery dei job in coda dopo un crash, e cancellazione best-effort sul peer prima del fallback locale — quest'ultima restringerebbe (senza chiuderla) la finestra di doppia esecuzione descritta al punto 7.

## Come compilare

```bash
mvn compile
```

## Come lanciare un cluster

Ogni worker deve girare su una porta diversa (anche sulla stessa macchina).

**Primo worker (seed, nessun nodo a cui collegarsi):**
```bash
java -cp target/classes com.progetto.worker.Worker 1099 worker-1
```

**Worker successivi (si collegano a un nodo già attivo, non necessariamente il primo):**
```bash
java -cp target/classes com.progetto.worker.Worker 1100 worker-2 localhost 1099 worker-1
java -cp target/classes com.progetto.worker.Worker 1101 worker-3 localhost 1099 worker-1
```

Ogni worker stampa su console gli handshake di join e, ogni 2s, i round di gossip (`[gossip=worker-X] round -> ... / round <- ...`) — utile per verificare a occhio che il cluster converga.

## Test automatici

```bash
mvn test
```

42 test JUnit 5, nessuna rete e nessun registry RMI coinvolti: girano in pochi secondi.

### Setup

I test usano **JUnit 5** (Jupiter). Il `pom.xml` dichiara due sole cose:

```xml
<dependency>
    <groupId>org.junit.jupiter</groupId>
    <artifactId>junit-jupiter</artifactId>
    <version>5.10.2</version>
    <scope>test</scope>
</dependency>
```

l'artefatto aggregatore (tira dentro `junit-jupiter-api`, `-params` e `-engine`, quindi non serve altro), e il **Surefire 3.2.5**, necessario perché le versioni precedenti non sanno lanciare la piattaforma JUnit 5. È configurato con `redirectTestOutputToFile`: worker e gossip loggano parecchio su stdout, e senza quello un `mvn test` sommerge il risultato sotto migliaia di righe. L'output resta comunque disponibile per classe sotto `target/surefire-reports/*-output.txt` quando serve indagare un fallimento.

La prima esecuzione scarica le dipendenze, quindi richiede rete; dopo funziona anche offline (`mvn -o test`).

### Layout

I test stanno in `src/test/java` e rispecchiano i package del codice, così hanno accesso ai membri package-private che fanno da giunto di test:

```
src/test/java/com/progetto/gossip/ClusterStateTest.java
src/test/java/com/progetto/gossip/GossipServiceTest.java
src/test/java/com/progetto/gossip/StubPeer.java          (peer fittizio condiviso)
src/test/java/com/progetto/worker/WorkerExecutionTest.java
```

### Comandi utili

```bash
mvn test                                  # tutta la suite
mvn test -Dtest=GossipServiceTest         # una sola classe
mvn test -Dtest=GossipServiceTest#aStalledRoundSkipsDetectionInsteadOfEvictingTheWholeCluster
mvn -o test                               # senza rete, a dipendenze già scaricate
```

### Cosa coprono

- `ClusterStateTest` (11 test) — regole di merge: last-writer-wins stretto sulla versione, heartbeat ristampato con l'orologio *locale* (così il cluster non ha bisogno di clock sincronizzati), view relayata già nota che non deve rinfrescare l'heartbeat, filtro sui peer conosciuti, esclusione della propria view.
- `GossipServiceTest` (17 test) — scelta del target di forwarding e soglia di sbilanciamento, prenotazione degli slot che impedisce a una raffica di finire tutta sullo stesso peer, version floor dopo un restart, e la failure detection completa: espulsione di un peer silenzioso, nessuna espulsione di un peer che continua a gossippare, round in ritardo che salta la detection, e grazia che resta temporanea.
- `WorkerExecutionTest` (14 test) — i tre tipi di task, i percorsi di errore (tipo sconosciuto, payload mancante, dimensioni incompatibili) con la verifica che l'esecutore sopravviva a un job fallito, la deduplica delle consegne doppie (e la conferma che due task identici restino comunque due job distinti), e la gestione dei peer (`registerPeer` che sostituisce uno stub morto, rifiuto dell'auto-registrazione, copia difensiva).

Il tempo è iniettabile — `GossipService` ha un costruttore package-private che accetta un `LongSupplier`, e `gossipRound()` è richiamabile direttamente invece di aspettare lo scheduler. Per questo `WorkerView` e `ClusterState` ricevono l'istante come parametro anziché leggere `System.currentTimeMillis()` al proprio interno: una soglia da 6 secondi si attraversa senza attese reali, e la semantica dell'heartbeat resta visibile nelle firme.

**Cosa non è coperto**, di proposito:

- il percorso RMI end-to-end (join fra processi, forwarding, ri-ammissione via `exchangeState`) — richiederebbe avviare registry e JVM vere, con il rischio di flakiness da porte occupate; resta coperto dalle procedure manuali qui sotto;
- il collegamento fra `Worker.enqueueLocally` e il contatore di carico: `gossipService` è privato e non osservabile dall'esterno, quindi la logica è testata a livello di `GossipService`;
- `multiply` con matrici vuote o righe di lunghezza disomogenea, che oggi lancia `IndexOutOfBoundsException` invece di un errore leggibile: prima va sistemato il comportamento, poi testato.

## Come testare a mano

C'è un unico client, `com.progetto.client.Client`. Tutti gli argomenti sono opzionali:

```bash
java -cp target/classes com.progetto.client.Client [host] [port] [workerId] [stress [numJobs] [sleepMillis]]
# default: localhost 1099 worker-1
```

Senza `stress` sottomette un job per ogni tipo di task (SUM, SLEEP, MATRIX_MULT) e ne stampa i risultati — serve a verificare che il worker li esegua tutti correttamente:

```bash
java -cp target/classes com.progetto.client.Client localhost 1099 worker-1
```

Aggiungendo `stress` sottomette invece un burst di job SLEEP identici (default: 8 job da 3000ms) allo stesso worker, per generare uno squilibrio di carico e vedere il forwarding in azione:

```bash
# 10 job da 3s a worker-1
java -cp target/classes com.progetto.client.Client localhost 1099 worker-1 stress 10 3000
```

In entrambi i casi il client sottomette *tutti* i job prima di iniziare il polling: fare polling nel mezzo svuoterebbe la coda alla stessa velocità con cui la si riempie, il carico non salirebbe mai e il forwarding non scatterebbe.

Aspetta almeno 3-4 secondi dopo l'avvio del cluster prima di sottomettere job: finché non è avvenuto almeno un round di gossip, nessun worker conosce ancora il carico dei peer e nessun forwarding può scattare.

Nei log dei worker cerca:
- `FORWARDING to peer worker-X (load balancing)` — il worker d'origine ha deciso di inoltrare
- `RECEIVED forwarded job from peer` — il worker che lo eseguirà per davvero
- `FORWARD to worker-X completed` — il risultato è tornato al worker d'origine
- `FORWARD to worker-X FAILED (...), evicting peer` — il peer è risultato irraggiungibile: viene espulso e il job rieseguito localmente

Con un cluster di 3 nodi e `Client ... stress 10 3000` i 10 job si distribuiscono su tutti e tre i worker (es. 5/3/2) e ognuno viene eseguito **una volta sola**: se un `job=<id>` compare con `:: RUNNING` su due worker diversi senza che nessuno sia stato ucciso, è un bug.

### Test della Failure Detection (Freeze con SIGSTOP)

È possibile testare la reattività della Failure Detection simulando il congelamento/blocco di un worker senza chiudere la sua socket TCP:

1. Avvia un cluster di 3 worker (`worker-1` su porta 1099, `worker-2` su 1100, `worker-3` su 1101) e aspetta che le tre view convergano.
2. Congela `worker-3` eseguendo da un altro terminale:
   ```bash
   kill -STOP $(pgrep -f "Worker 1101 worker-3")
   ```
3. Dopo ~6-10s, nei log di `worker-1` e `worker-2` compare:
   ```
   [worker-1] FAILURE DETECTION: Node worker-3 has timed out, removed from peers.
   ```
   e i round di gossip successivi mostrano `cluster now: worker-2(...) worker-1(...)`, senza `worker-3`.
4. Scongela il nodo:
   ```bash
   kill -CONT $(pgrep -f "Worker 1101 worker-3")
   ```
5. Verifica che il cluster **si ricomponga** (è il punto del test — l'espulsione non deve essere definitiva):
   - `worker-3` stampa `scheduler stalled for NNNNNms -> skipping failure detection` e **non** espelle `worker-1`/`worker-2`;
   - `worker-1` e `worker-2` stampano `added worker-3` entro un round o due;
   - tutte e tre le view tornano a elencare i tre nodi.

   Un `FAILURE DETECTION` stampato da `worker-3` al risveglio, o un `worker-3` che non ricompare più nelle view degli altri, sono regressioni.

### Test del restart

Ucci­di un worker e riavvialo con lo **stesso** id (`kill -9` + rilancio dello stesso comando). Devi vedere `stub refreshed (peer restarted?) for worker-3` (se il riavvio batte i 6s di timeout) oppure `added worker-3`, e la versione di `worker-3` deve ripartire *sopra* quella che i peer avevano memorizzato, non da 1.

### Test work stealing

Avvia un solo worker (per bypassare il forwarding alla submission), invia un burst di carico a quel worker. Avvia il secondo worker per assistere al riequilibro del carico tramite stealing.

### Test Job timeout con fallback locale

Avvia due worker, invia un burst di carico al primo e aspetta il carico si distribuisca sul secondo. Uccidi (CTRL + C) il secondo worker e attendi la scadenza del timeout per il Job DELEGATED, che sarà preso in carico localmente dal worker origine.

### Test stable storage

Avvia un solo worker, manda un burst di carico e uccidilo. Poi riavvialo con l'id che avevi utilizzato precedentemente. Ora verifica che il worker riprenda l'esecuzione dei job che aveva in coda precedentemente.

## Parametri CLI

| Comando | Parametri |
|---|---|
| Worker indipendente | `<port> <workerId>` |
| Worker che si unisce al cluster | `<port> <workerId> <seedHost> <seedPort> <seedWorkerId>` |
| Client | `[host] [port] [workerId] [stress [numJobs] [sleepMillis]]` |
