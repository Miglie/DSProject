# Progetto Distributed Systems

Infrastruttura per gestire job inviati a un cluster di worker, con join dinamico dei nodi e load balancing tramite gossip protocol. Tutto esposto via Java RMI.

## Architettura

- `com.progetto.job` — modello dati: `Task` (lavoro grezzo: `type` + `payload`), `Job` (Task + jobId + stato), `JobStatus` (`PENDING`/`RUNNING`/`COMPLETED`/`FAILED`), `JobResult`.
- `com.progetto.rmi` — interfacce remote: `WorkerRemote` (API client-facing: `submitJob`/`getStatus`/`getResult`, più le API di membership `registerPeer`/`unregisterPeer`/`getKnownPeers`) e `GossipRemote` (API worker-to-worker: `exchangeState`/`forwardJob`).
- `com.progetto.gossip` — `WorkerView` (snapshot versionato del carico di un worker), `ClusterState` (mappa mergeable workerId → WorkerView, ultimo scrivente vince per versione), `GossipService` (round di gossip periodico e decisione di forwarding).
- `com.progetto.worker` — `Worker`: implementa sia `WorkerRemote` che `GossipRemote` sullo stesso oggetto esportato/bindato.
- `com.progetto.client` — `Client` (client di test base) e `StressClient` (submette un burst di job per generare squilibrio di carico e osservare il forwarding).

### Come funziona

1. **Join dinamico**: un worker parte da solo, oppure indicando un nodo seed già attivo. Al join, scarica la lista di peer conosciuti dal seed e fa handshake (`registerPeer`) con tutti — niente configurazione statica del cluster.
2. **Gossip del carico**: ogni worker, ogni 2 secondi, sceglie un peer a caso tra quelli conosciuti e scambia il proprio `ClusterState` (push-pull in una chiamata RMI). Ogni voce del cluster porta un contatore di versione: chi ha la versione più alta vince il merge.
3. **Load balancing**: quando arriva un nuovo job, il worker controlla se un peer noto ha un carico significativamente più basso del proprio (soglia: differenza > 2). Se sì, il job viene inoltrato in background e il client continua a interrogare **solo** il worker a cui si è connesso all'inizio — mai il worker che lo esegue realmente.
4. **Resilienza minima**: se un peer non risponde per un errore reale (non un timeout dovuto a carico), viene rimosso dalla lista dei peer e dal cluster state. Un timeout dovuto a un peer semplicemente occupato *non* lo esclude — riprova più avanti.

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

**Client semplice** — sottomette 3 job di test (SUM, SLEEP, MATRIX_MULT) a un worker e ne stampa i risultati:
```bash
java -cp target/classes com.progetto.client.Client <host> <port> <workerId>
# esempio:
java -cp target/classes com.progetto.client.Client localhost 1099 worker-1
```

**Stress client** — sottomette N job SLEEP in rapida sequenza allo stesso worker, per generare uno squilibrio di carico e osservare il forwarding in azione:
```bash
java -cp target/classes com.progetto.client.StressClient <host> <port> <workerId> [numJob=8] [sleepMillis=3000]
# esempio: 10 job da 3s a worker-1
java -cp target/classes com.progetto.client.StressClient localhost 1099 worker-1 10 3000
```

Aspetta almeno 3-4 secondi dopo l'avvio del cluster prima di sottomettere job: finché non è avvenuto almeno un round di gossip, nessun worker conosce ancora il carico dei peer e nessun forwarding può scattare.

Nei log dei worker cerca:
- `FORWARDING to peer worker-X (load balancing)` — il worker d'origine ha deciso di inoltrare
- `RECEIVED forwarded job from peer` — il worker che lo eseguirà per davvero
- `FORWARD to worker-X completed` — il risultato è tornato al worker d'origine

## Parametri CLI

| Comando | Parametri |
|---|---|
| Worker indipendente | `<port> <workerId>` |
| Worker che si unisce al cluster | `<port> <workerId> <seedHost> <seedPort> <seedWorkerId>` |
| Client | `<workerHost> <workerPort> <workerId>` |
| StressClient | `<workerHost> <workerPort> <workerId> [numJob] [sleepMillis]` |
