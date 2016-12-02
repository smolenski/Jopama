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
    * with vlocked=NIL,rown=TID -> vlocked=rver
* ::update
    * with all objects rown=TID,vlocked!=NIL all objects cmpSet(rown=TID -> newval=fun(values))[i]
    * with all objects rown[i]=TID and newval[i] != NIL -> done := true
* ::release
    * with done if rown[i]=TID -> with vlocked[i] cmpSet(rown=TID -> rver=vlocked+1,rval=rnewval,rnewval=NIL,rown=NIL)
* ::delete
    * if all objects vlocked!=NIL then if all objects rown!=NIL then remove TID
