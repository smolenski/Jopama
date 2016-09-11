# Jopama
Scalable distributed database.

# Internals
There are two types of objects stored:
* data
* transaction

## Data object
#### Data
* version: INT (initially 0)
* value: object data (initially NULL)
* owner: INT (initially NULL)
* ensambleTidFence[numEnsambles]

#### Operations
* lock (tid):
	* self.tid := tid
* unlock:
	* self.tid := NULL
* update (value):
	* self.value := value 
* query:
	* return value

## Transaction object
#### Data
* ++ensambleTid (in ensamble of 5)
* list(ids)
* fun: values -> values

#### Operations
* perform
	* assign ensambleTid
	* locally locking (these object are locked by this transactions and all transactions with lowest ensambleTid will not need to lock any of these objects)
	* locking (ensambleTid)
	* querying and saving in transaction
	* updating
	* waiting until lowest not commited is eqaul ensambleTid
	* fencingWithUnlocking (ensambleTid)





