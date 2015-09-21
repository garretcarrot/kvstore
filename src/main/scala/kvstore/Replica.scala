package kvstore

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import kvstore.Arbiter._
import kvstore.Persistence._
import kvstore.Replica._
import kvstore.Replicator._

import scala.concurrent.duration._

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

  case class OneSecondTimeMachine(client: ActorRef, id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import context.dispatcher

  context.setReceiveTimeout(100.millis)
  arbiter ! Join

  var kv = Map.empty[String, String]

  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var expectedSeq: Long = 0
  
  val persistence = context.system.actorOf(persistenceProps)

  // restart persistence actor when it fails
  override val supervisorStrategy = OneForOneStrategy() {
    case _: PersistenceException => Restart
  }

  // operation id -> (requestSource, requestMessage, ackMessage)
  var persists = Map.empty[Long, (ActorRef, Persist, Object)]

  // operation id -> unacknowledged replicas
  var replications = Map.empty[Long, Set[ActorRef]]

  // operation id -> source of original request
  var replicationClients = Map.empty[Long, ActorRef]

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val getting: Receive = {
    case Get(key: String, id: Long) =>
      sender() ! GetResult(key, kv.get(key), id)
  }

  val persisting: Receive = {
    case Persisted(key, id) =>
      for ((source, _, ackMessage) <- persists.get(id)) {
        persists -= id
        // should only happen if all replications succeeded too
        // and if it hasn't been ack'd already
        replications.get(id) match {
          case Some(unacked) if unacked.nonEmpty =>
          case _ =>
            replicationClients -= id
            source ! ackMessage
        }
      }

    case ReceiveTimeout =>
      for ((_, persistRequest, _) <- persists.values) {
        persistence ! persistRequest
      }
  }

  val leader: Receive = getting orElse persisting orElse {
    case Insert(key: String, value: String, id: Long) =>
      updatePrimary(sender(), key, Option(value), id)

    case Remove(key: String, id: Long) =>
      updatePrimary(sender(), key, None, id)

    case OneSecondTimeMachine(client, id) =>
      for (unacked <- replications.get(id) if unacked.nonEmpty) {
        client ! OperationFailed(id)
      }

      for (_ <- persists.get(id)) {
        client ! OperationFailed(id)
      }

    case Replicated(key, id) =>
      replications.get(id) match {
        case None =>
          if (!persists.contains(id)) {
            replicationClients.get(id) match {
              case None =>
              case Some(replicationClient) =>
                replicationClients -= id
                replicationClient ! OperationAck(id)
            }
          }

        case Some(unackedReplicas) =>
          val ackedReplicator = sender()
          val maybeAcked = secondaries.find { case (_, replicator) => replicator == ackedReplicator }
          val stillUnackedReplicas = maybeAcked.fold(unackedReplicas)({ case (r, _) => unackedReplicas - r })
          if (stillUnackedReplicas.nonEmpty) {
            replications = replications.updated(id, stillUnackedReplicas)
          } else {
            replications -= id
            if (!persists.contains(id)) {
              replicationClients.get(id) match {
                case None =>
                case Some(replicationClient) =>
                  replicationClients -= id
                  replicationClient ! OperationAck(id)
              }
            }
          }
      }

    case Replicas(replicas) =>
      val newSecondaries = replicas - self
      val added = newSecondaries diff secondaries.keySet
      val removed = secondaries.keySet diff newSecondaries

      // remove old replicas
      removed foreach { r =>
        secondaries.get(r) match {
          case None =>
          case Some(replicator) =>
            replicators -= replicator
            context.system.stop(replicator)
        }
        secondaries -= r
      }

      // add new replicas
      added foreach { r =>
        val replicator = context.system.actorOf(Replicator.props(r))
        secondaries = secondaries.updated(r, replicator)
        replicators += replicator

        kv foreach {
          case (k, v) =>
            val newId = scala.util.Random.nextInt()
            replicator ! Replicate(k, Option(v), newId)
            val currentSet = replications.getOrElse(newId, Set.empty)
            replications = replications.updated(newId, currentSet + r)
        }
      }

      // remove old replicas that remain unacknowledged
      replications = replications flatMap {
        case (id, unackedReplicas) =>
          val stillUnacked = unackedReplicas.diff(removed)
          if (stillUnacked.nonEmpty) Option(id -> stillUnacked)
          else {
            for (client <- replicationClients.get(id)) {
              replicationClients -= id
              client ! OperationAck(id)
            }
            None
          }
      }
  }

  val replica: Receive = getting orElse persisting orElse {
    case snapshot @ Snapshot(key: String, valueOption: Option[String], seq: Long) =>
      val replicator = sender()
      if (seq < expectedSeq) {
        replicator ! SnapshotAck(key, seq)
      } else if (seq == expectedSeq) {
        update(key, valueOption)
        expectedSeq += 1
        persist(replicator, Persist(key, valueOption, seq), SnapshotAck(key, seq))
      }
  }

  def updatePrimary(client: ActorRef, key: String, updatedValue: Option[String], id: Long): Unit = {
    update(key, updatedValue)
    context.system.scheduler.scheduleOnce(1.second, self, OneSecondTimeMachine(client, id))
    persist(client, Persist(key, updatedValue, id), OperationAck(id))
    replications = replications.updated(id, secondaries.keySet)
    replicationClients = replicationClients.updated(id, client)
    replicators foreach { replicator =>
      replicator ! Replicate(key, updatedValue, id)
    }
  }

  def update(key: String, valueOption: Option[String]): Unit = {
    valueOption match {
      case Some(value) => kv = kv.updated(key, value)
      case None => kv -= key
    }
  }

  def persist(source: ActorRef, persistRequest: Persist, ackMessage: Object): Unit = {
    persists = persists.updated(persistRequest.id, (source, persistRequest, ackMessage))
    persistence ! persistRequest
  }
}

