# Pasos para realizar una transaccion

- Crear objeto de clase MultiDocumentTransactionManager ("tx"), pasandole como parametros objetos de conexion con Couchbase, asi como indicar el enfoque (Optimistic, Pessimistic, etc).

### Pasos
- tx.begin
- tx.partialCommit (para cada modificacion)
    - tx.rollback (en caso de fallo)
    - tx.close
- tx.commit (una vez que ha procesado todos los documentos sin fallo)
- tx.close