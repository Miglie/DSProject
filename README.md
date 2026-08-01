Progetto di Distributed Systems

Versione 2.0:

Ora è possibile creare un cluster di più worker connessi tra di loro. Un client si può connettere con RMI per eseguire operazioni a uno qualsiasi dei worker. Per semplicità vado a eseguire questo testo con tre operazioni: una somma, una moltiplicazione matriciale e una sleep che simula un'operazione complessa. Dovrò usare i seguenti comandi:

Inizialmente, per compilare: mvn compile

- java -cp target/classes com.progetto.worker.Worker 1099 worker-1                           per creare il primo nodo o un nodo indipendente dalla rete
- java -cp target/classes com.progetto.worker.Worker 1100 worker-2 localhost 1099 worker-1   per connettere un secondo nodo usando il nodo 1 come seed/anchor
- java -cp target/classes com.progetto.client.Client localhost 1099 worker-1                 per collegare il client a un nodo

Parametri:

- worker indipendente <Port> <ID>
- worker connesso <Port> <ID> <SeedAddress> <SeedPort> <SeedId>
- client <WorkerAddress> <WorkerPort> <WorkerID>

Nell'implementazione corrente ogni worker se sullo stesso dispositivo deve essere associato a una porta differente.