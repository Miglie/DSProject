Progetto di Distributed Systems
Prima versione:
Un solo worker, un solo client che si connettono in RMI per eseguire operazioni. Per semplicità vado a eseguire questo testo con tre operazioni: una somma, una moltiplicazione matriciale e una sleep che simula un'operazione complessa. Dovrò usare i seguenti comandi:

Inizialmenete, per compilare: mvn compile
java -cp target/classes com.progetto.client.Client localhost 1099 worker-1                 nel terminale 1, quello del client
java -cp target/classes com.progetto.worker.Worker 1099 worker-1                           nel terminale 2, quello del worker


