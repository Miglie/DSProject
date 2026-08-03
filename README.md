# Progetto Distributed Systems

Infrastruttura per gestire job inviati a un cluster di worker, con join dinamico dei nodi e load balancing tramite gossip protocol. Tutto esposto via Java RMI.

## Architettura

- `com.progetto.job` — modello dati: `Task` (lavoro grezzo: `type` + `payload`), `Job` (Task + jobId + stato), `JobStatus` (`PENDING`/`RUNNING`/`COMPLETED`/`FAILED`), `JobResult`.
- `com.progetto.rmi` — interfacce remote: `WorkerRemote` (API client-facing: `submitJob`/`getStatus`/`getResult`, più le API di membership `registerPeer`/`unregisterPeer`/`getKnownPeers`) e `GossipRemote` (API worker-to-worker: `exchangeState`/`forwardJob`). `forwardJob` è un ack non bloccante: il risultato di un job inoltrato viene poi recuperato dal nodo d'origine via `getResult`.
- `com.progetto.gossip` — `WorkerView` (snapshot versionato del carico di un worker), `ClusterState` (mappa mergeable workerId → WorkerView, ultimo scrivente vince per versione), `GossipService` (round di gossip periodico e decisione di forwarding).
- `com.progetto.worker` — `Worker`: implementa sia `WorkerRemote` che `GossipRemote` sullo stesso oggetto esportato/bindato.
- `com.progetto.client` — `Client`, in due modalità: *demo* (un job per tipo) e *stress* (un burst di job per generare squilibrio di carico e osservare il forwarding).

### Come funziona

1. **Join dinamico**: un worker parte da solo, oppure indicando un nodo seed già attivo. Al join, scarica la lista di peer conosciuti dal seed e fa handshake (`registerPeer`) con tutti — niente configurazione statica del cluster.
2. **Gossip del carico**: ogni worker, ogni 2 secondi, sceglie un peer a caso tra quelli conosciuti e scambia il proprio `ClusterState` (push-pull in una chiamata RMI). Ogni voce del cluster porta un contatore di versione: chi ha la versione più alta vince il merge.
3. **Load balancing**: quando arriva un nuovo job, il worker controlla se un peer noto ha un carico significativamente più basso del proprio (soglia: differenza > 2). Se sì, il job viene inoltrato in background e il client continua a interrogare **solo** il worker a cui si è connesso all'inizio — mai il worker che lo esegue realmente. Ogni forwarding *prenota* subito uno slot sul peer scelto: dato che il gossip aggiorna il carico solo ogni 2s, senza la prenotazione tutti i job di una raffica vedrebbero la stessa foto "quel peer è scarico" e finirebbero tutti sullo stesso nodo.
4. **Forwarding ack-then-poll**: `forwardJob` ritorna appena il job è in coda sul peer — ritornare senza errore significa "il peer ha preso in carico il job". Da quel momento il nodo d'origine fa polling di `getResult` sul peer finché il risultato non è pronto, e **non** lo riesegue: un peer lento viene semplicemente atteso. Se `forwardJob` bloccasse per tutta la durata del job, un peer occupato sarebbe indistinguibile da uno morto e il job verrebbe eseguito due volte.
5. **Resilienza minima**: un peer viene rimosso dalla lista dei peer e dal cluster state solo quando è davvero irraggiungibile (connection refused, o più poll falliti di fila). Solo in quel caso il job torna in esecuzione locale — quindi il forwarding è *exactly-once* nel caso normale e *at-least-once* se un peer muore mentre teneva il job.

Non ancora implementato: vera crash detection (heartbeat con timeout), stable storage su disco, recovery dopo crash.

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

## Come testare

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

## Parametri CLI

| Comando | Parametri |
|---|---|
| Worker indipendente | `<port> <workerId>` |
| Worker che si unisce al cluster | `<port> <workerId> <seedHost> <seedPort> <seedWorkerId>` |
| Client | `[host] [port] [workerId] [stress [numJobs] [sleepMillis]]` |
