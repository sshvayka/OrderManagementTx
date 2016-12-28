# OrderManagementTx

Evaluation of Transaction strategies in Couchbase for a concurrent microservice-based enviornment.

*SourcingOL: An optimistic approach is used. A new MultiDocumentTransactionManager is used to manage the process.
*SourcingPL: A pessimistic approach is used. Documents are locked using Couchbase API.

![alt tag](https://cloud.githubusercontent.com/assets/6870793/21527092/179c9b36-cd29-11e6-84e3-bf164b9669f7.png)
