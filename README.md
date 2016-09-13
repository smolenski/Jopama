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
* owner: INT (initially NULL) - LOCKED/UNLOCKED

#### Operations
* lock (tid):
	* self.owner := tid
* unlock:
	* self.tid := NULL
* update (value):
	* self.value := value 
	* self.version := self.version + 1
* query:
	* return version,value,owner

## Transaction object
#### Data ()
* tid
* fun: values -> values
* [ids]:
	* versionToLock
	* versionLocked
	* valueLocked
* done

#### Data (initial)
* 1022
* fun: lambda values : e + 1 for e in values
* [ids]
	* NULL
	* NULL
	* NULL
* done: false

#### Data (locking - independently for each id)
both retrieved atomically: rver, rown
both retrieved atomically: vtolock, vlocked
vlocked=rver,own=TID <- vlocked=NIL,own=TID
vlocked=NIL,own=TID <- vlocked=NIL,vtolock=rval,own=NIL
vlocked=NIL,vtolock=rval,own=NIL <- vlocked=NIL,vtolock<rval,own=NIL
own=NIL <- vlocked=NIL,own!=NIL,own!=TID
vlocked=NIL,vtolock=rval,own=NIL <- vlocked=NIL,vtolock=NIL


