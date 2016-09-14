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
* both retrieved and updated atomically: rver, rown
* both retrieved and updated atomically: vtolock, vlocked
* retrieve both periodically, and do actions with respect to these rules:
	* vlocked=rver,own=TID <- vlocked=NIL,own=TID
	* vlocked=NIL,own=TID <- vlocked=NIL,vtolock=rver,own=NIL
	* vlocked=NIL,vtolock=rver,own=NIL <- vlocked=NIL,vtolock<rver,own=NIL
	* own=NIL <- vlocked=NIL,own!=NIL,own!=TID
	* vlocked=NIL,vtolock=rver,own=NIL <- vlocked=NIL,vtolock=NIL

* locking (for all values) and retrieving:
	* updating vtolock:
		* query rver,rown, if rown=NIL then cmpSet(rver)(vlocked=NIL && (vtolock=NIL || vtolock<rver) -> vtolock=rver)
	* updating rown:
		* read vtolock, if vtolock!=NIL then cmpSet(vtolock,TID)(rver=vtolock && rown=NIL -> rown=TID)
	* updating vlocked:
		* read rver,rown if rown=TID then cmpSet(rver)(vlocked=NIL -> vlocked=rver)
	* updating/retrieving value:
		* read vlocked,value if vlocked!=NIL and value=NIL then read rver,rown,rval cmpSet(vlocked=rver,value=NIL -> value=rval)
* updating value and releasing:
	* read whole transaction, if all values retrieved then outValues=fun(value), for each value do cmpSet(rver=vlocked -> rval=outValue,++rver,rown=NIL)
