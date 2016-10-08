# Jopama
Scalable transactional distributed database.

# Internals
There are two types of objects stored:
* data
* transaction

Object:
- rver
- rown
- val
- newval

Transaction:
- done
- object data:
--- vtolock
--- vlocked
- fun

* ::add
    * create TID with fun with all objects (vtolock=NIL,vlocked=NIL)
* ::advanceVTolock
    * with rver,rown=NIL cmpSet(rver)(vlocked=NIL,vtolock=NIL || vtolock < rver -> vtolock=rver)
* ::lock
    * with vtolock cmpSet(vtolock)(rown=NIL,rver=vtolock -> rown=TID)
* ::update
    * with all objects rown=TID all objects cmpSet(rown=TID -> newval=fun(values))[i]
* ::release
    * with all objects rown=TID and newval[i]!=NIL -> with vlocked cmpSet(rown=TID -> rver=vlocked+1,rval=rnewval,rown=NIL
* ::delete
    * if all objects vlocked!=NIL then if all objects rown!=NIL then remove TID
