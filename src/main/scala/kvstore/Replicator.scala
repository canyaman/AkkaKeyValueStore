package kvstore

import akka.actor._
import scala.concurrent.duration._
import akka.util.Timeout
import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
  case object SendSnapshot

}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  implicit val timeout = Timeout(1 seconds)

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
  var resendSchedule:Cancellable=null

  def startResend = {
    if(resendSchedule==null||resendSchedule.isCancelled){
      resendSchedule=context.system.scheduler.schedule(100 milliseconds,100 milliseconds,self,SendSnapshot)
    }
  }
  def stopResend ={
    resendSchedule.cancel()
  }
  override def preStart()={
       context.watch(replica)
  }

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case r @ Replicate(key, valueOption, id) => {
      val snp=Snapshot(key,valueOption,nextSeq)
      //System.out.println(s"Receive:$snp to $replica")
      pending = pending :+ snp
      acks+=(snp.seq->(sender,r))
      self ! SendSnapshot
      startResend
    }
     case SnapshotAck(key, seq) => {
       if(pending.nonEmpty){
         pending.head.seq match {
           case s if s==seq =>{
             val (replicateSender:ActorRef,Replicate(k,_,id))= acks(seq)
             replicateSender ! Replicated(key,id)
             pending=pending.tail
             sendSnapshot
           }
         }
       }
    }
    case SendSnapshot => {
      sendSnapshot
    }
    case OperationFailed(id)=> {
      System.out.println(s"OperationFailed $id $replica")
    }
    case Terminated(_)=>{

    }
  }

  def sendSnapshot={
    if(pending.isEmpty){
      stopResend
    }  else{
      //System.out.println(s"Send:$pending to $replica")
      replica ! pending.head
    }
  }
}
