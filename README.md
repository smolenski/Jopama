# Jopama

**Jopama** is a scalable transactional key-value store.

**Jopama** provides following highly-desired properties:

- **Consistency** - Always, in all scenarios
- **Scalability for storing components** - Use more servers to store more components - Linear scalability
- **Transaction Processing** - Allows to process transactions on components distributed in the whole system
- **Scalability for processing transactions** - Use more servers to process more transactions - Linear scalability
- **Fault Tolerance** - There is no single point of failures - Number of tolerated failures can be tuned (it is determined by subcluster size)

Jopama has two main components:

- Fault-tolerant non-scalable transactional key-value store (e.g. ZooKeeper) - let's call it **FTNonScalableKVStore**
- Novel fault-tolerant algorithm for distributed transaction processing - let's call it **JopamaAlgorithm**

Jopama stores components (key-value entries) and transactions on instances of *FTNonScalableKVStore*.
*JopamaAlgorithm* is executed by *TransactionProcessors*.
*TransactionProcessors* watch *FTNonScalableKVStore* for transactions.
There is redundancy in *TransactionProcessors* - so that each transaction is detected and processed by *TransactionProcessor* from mutliple nodes.
*JopamaAlgorithm* ensures that single transaction is processed consistently - no metter how many *TransactionProcessors* are processing it.
