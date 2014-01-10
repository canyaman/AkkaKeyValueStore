package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout
import scala.util._
import scala.language.postfixOps
import kvstore.Replicator.Snapshot


object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case object SendPersist
  case object CheckReplica

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  implicit val timeout =Timeout(100 millisecond)
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var index = 0L;

  val persistence=context.actorOf(persistenceProps)

  var persistancePending=Vector.empty[Persist]
  var replicatePending=Vector.empty[Long]

  //leader variable   id->(sender,replicators)
  var replicating=Map.empty[Long,(ActorRef,Set[ActorRef])]

  var lastPersistedId = 0L
  var lastReplicadId = 0L
  //snaphotsSeq-replicator
  var replicatorOfSnaphot=Map.empty[Long,ActorRef]

  var resendSchedule:Cancellable=null
  var operationScheduler:Cancellable=null


  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
  override def preStart() ={
      arbiter ! Join
  }

  def receive = {
    case JoinedPrimary   => {
      context.become(leader orElse replica)
      initReplicas(Set(self.actorRef))
    }
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) => kv += (key -> value); replicateAll(Replicate(key, Some(value), id))
    case Remove(key, id)        => kv -= key ; replicateAll(Replicate(key, None, id))
    case CheckReplica => isReplicated
    case Replicated(key,id)=> replicated(id)
    case Replicas(replicas)=> initReplicas(replicas)
    /*
    case snp @ Snapshot(key, None, seq)        if seq<index  => sender ! SnapshotAck(key,seq)
    case snp @ Snapshot(key, None, seq)        if seq==index => kv-= key ; persist(snp)
    case snp @ Snapshot(key, Some(value), seq) if seq<index  => sender ! SnapshotAck(key,seq)
    case snp @ Snapshot(key, Some(value), seq) if seq==index => kv+=(key->value) ; persist(snp)
    case Get(key, id)           => sender ! GetResult(key,kv.get(key),id)
    case Persisted(key, id) if id == index => index+=1 ;lastPersistedId=id; snaphotSender(id) ! SnapshotAck(key, id); persistancePending=persistancePending.tail
    case SendPersist => sendPersist  */
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case snp @ Snapshot(key, None, seq)        if seq<index  => sender ! SnapshotAck(key,seq)
    case snp @ Snapshot(key, None, seq)        if seq==index => kv-= key ; persist(snp)
    case snp @ Snapshot(key, Some(value), seq) if seq<index  => sender ! SnapshotAck(key,seq)
    case snp @ Snapshot(key, Some(value), seq) if seq==index => kv+=(key->value) ; persist(snp)
    case Get(key, id) => sender ! GetResult(key,kv.get(key),id)
    case Persisted(key, id) if id == index => index+=1 ; persisted(key,id)
    case SendPersist => sendPersist
  }

  def replicateAll(r:Replicate)={
    replicating += (r.id->(sender,replicators))
    replicatePending=replicatePending :+ r.id
    replicators.map( _ ! r)
    startOperationSchedule
  }

  def startOperationSchedule={
    if((operationScheduler==null)||(operationScheduler.isCancelled)){
      operationScheduler=context.system.scheduler.schedule(1 second,1 second,self,CheckReplica)
    }
  }
  def stopOperationSchedule={
    operationScheduler.cancel()
  }
  def isReplicated={
    if(replicatePending.nonEmpty){
     if (replicatePending.head==lastReplicadId){
       stopOperationSchedule
        val (target,waitingReplicators)=replicating(lastReplicadId)
        target ! OperationFailed(replicatePending.head)
      }
    }else{
      stopOperationSchedule
    }
  }
  def persist(snapshot:Snapshot)={
    replicatorOfSnaphot+=(snapshot.seq->sender)
    val persist=Persist(snapshot.key,snapshot.valueOption,snapshot.seq)
    persistancePending=persistancePending :+ persist
    persistence ! persist
    startPersistanceSchedule
  }

  def sendPersist={
    if(persistancePending.isEmpty){
      stopPersistanceSchedule
    } else{
      persistence ! persistancePending.head
    }
  }
  def snaphotSender(snapshotSeq:Long)={
    val replicator=replicatorOfSnaphot(snapshotSeq);
    replicatorOfSnaphot-=snapshotSeq
    replicator
  }

  def startPersistanceSchedule = {
    if(resendSchedule==null||resendSchedule.isCancelled){
      resendSchedule=context.system.scheduler.schedule(100 milliseconds,100 milliseconds,self,SendPersist)
    }
  }
  def stopPersistanceSchedule ={
    resendSchedule.cancel()
  }
  
  def initReplicas(replicas: Set[ActorRef])={
    System.out.println(s"Before Replicators:$replicators")
    System.out.println(s"Replicas: $replicas")

    val newReplicas= replicas.diff(secondaries.keySet)
    val removedReplicas= secondaries.keySet.diff(replicas)

    /*var newReplicating = Map.empty[Long,(ActorRef,Set[ActorRef])]
    for((key,(target,replicators))<-replicating){
      val remaningReplicators=replicators.filterNot(replicator => removedReplicas.contains(secondaries(replicator)) )
      newReplicating += (key -> (target,remaningReplicators ) )
    }
    replicating=newReplicating */
    replicatePending.foreach(
      id=>removedReplicas.foreach( replica => self.tell(Replicated(null,id),secondaries(replica)) )
    )
    removedReplicas.foreach(removedReplica=>{
      val removedReplicator=secondaries(removedReplica)
      removedReplicator!PoisonPill
      secondaries-=removedReplica
      replicators-=removedReplicator
    })
 //   removedReplicas.foreach(_!PoisonPill)

    //System.out.println(s"Already replicas:$replicas")
    //System.out.println(s"New replicas:$newReplicas")

    secondaries=Map.empty[ActorRef, ActorRef]
    replicators++=newReplicas.map(replica => {
      val replicator=context.actorOf(Replicator.props(replica))
      secondaries+=(replica->replicator)
      replicator
    })

    var _idCounter = 0L
    def nextId = {
      val ret = _idCounter
      _idCounter += 1
      ret
    }

    for((key,value)<-kv ; newReplica<-newReplicas){
      secondaries(newReplica) ! Replicate(key, Some(value), nextId)
    }


    System.out.println(s"After Replicators:$replicators")
  }

  def persisted(key: String, id: Long) = {
    lastPersistedId=id
    snaphotSender(id) ! SnapshotAck(key, id)
    persistancePending=persistancePending.tail
    sendPersist
  }

  def replicated(id:Long)={
    if(replicatePending.nonEmpty) {
      if(replicatePending.head==id){
        val (target,waitingReplicators)=replicating(id)
        val remainingReplicators=waitingReplicators-sender
        if(remainingReplicators.isEmpty){
          replicating-=id
          replicatePending=replicatePending.tail
          lastReplicadId=id
          target ! OperationAck(id)
        }else{
          replicating+=(id->(target,remainingReplicators))
        }
      }
    }
  }
}
