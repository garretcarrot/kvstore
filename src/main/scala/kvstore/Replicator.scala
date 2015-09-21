package kvstore

import akka.actor.{Actor, ActorRef, Props, ReceiveTimeout}
import kvstore.Replicator._

import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {

  context.setReceiveTimeout(100.millis)

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
  
  def receive: Receive = {
    case replicationRequest @ Replicate(key, valueOption, id) =>
      val seq = nextSeq
      val primaryNode = sender()
      acks = acks.updated(seq, (primaryNode, replicationRequest))
      replica ! Snapshot(key, valueOption, seq)

    case SnapshotAck(key, seq) =>
      acks.get(seq) match {
        case None =>
        case Some((primaryReplica, Replicate(key2, valueOption, id))) =>
          acks -= seq
          primaryReplica ! Replicated(key2, id)
      }

    case ReceiveTimeout =>
      acks foreach {
        case (seq, (primaryReplica, Replicate(key, valueOption, id))) =>
          replica ! Snapshot(key, valueOption, seq)
      }
  }

}
