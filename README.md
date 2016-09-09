# jopama
Scalable distributed database.

# internals
There are two types of objects stored:
* data
* transaction

## Data object
### data
* version: INT (initially 0)
* value: object data (initially NULL)
* owner: INT (initially NULL)

### operations
* lock (id):
	* owner := id
* unlock:
	* owner := NULL
* query:
	* returns (owner,version,value)
* update (new_value):
	* version := version + 1
	* value := new_value

## Transaction object
