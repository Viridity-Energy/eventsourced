/*
 * Copyright 2012-2013 Eligotech BV.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.eligosource.eventsourced.journal.hbase

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.util._

import akka.actor._

import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{KeyValue, DeleteRequest, HBaseClient, PutRequest}

import org.eligosource.eventsourced.core._
import org.eligosource.eventsourced.core.Journal._
import org.eligosource.eventsourced.journal.common.AsynchronousWriteReplaySupport

/**
 * HBase journal with asynchronous, non-blocking IO and concurrent reads/writes.
 *
 * <strong>EXPERIMENTAL AND NOT OPTIMIZED</strong>.
 */
private [hbase] class HBaseJournal(props: HBaseJournalProps) extends AsynchronousWriteReplaySupport {
  import context.dispatcher

  val serialization = Serialization(context.system)
  var client: HBaseClient = _

  def journalProps = props
  def asyncWriteTimeout = props.writeTimeout
  def asyncWriterCount = props.writerCount
  def partition(snr: Long) = snr % props.partitionCount toInt

  def writer(id: Int) = new Writer {
    def executeWriteInMsg(cmd: WriteInMsg): Future[Any] = {
      val snr = cmd.message.sequenceNr
      val key = InMsgKey(partition(snr), cmd.processorId, snr).toBytes
      val msg = serialization.serializeMessage(cmd.message.clearConfirmationSettings)
      val putMsg = new PutRequest(TableNameBytes, key, ColumnFamilyNameBytes, MsgColumnNameBytes, msg)
      val putCtr = new PutRequest(TableNameBytes, key, ColumnFamilyNameBytes, SequenceNrColumnNameBytes, longToBytes(snr))
      val putMsgFuture: Future[Any] = client.put(putMsg)
      val putCtrFuture: Future[Any] = client.put(putCtr)
      client.flush()
      val wrt = for {
        _ <- putMsgFuture
        a <- putCtrFuture
      } yield a
      wrt.map(_ => ())
    }

    def executeWriteOutMsg(cmd: WriteOutMsg): Future[Any] = {
      val snr = cmd.message.sequenceNr
      val key = OutMsgKey(partition(snr), cmd.channelId, snr).toBytes
      val msg = serialization.serializeMessage(cmd.message.clearConfirmationSettings)
      val putMsg = new PutRequest(TableNameBytes, key, ColumnFamilyNameBytes, MsgColumnNameBytes, msg)
      val putCtr = new PutRequest(TableNameBytes, key, ColumnFamilyNameBytes, SequenceNrColumnNameBytes, longToBytes(snr))
      val putMsgFuture: Future[Any] = client.put(putMsg)
      val putCtrFuture: Future[Any] = client.put(putCtr)
      val putAckFuture: Future[Any] = if (cmd.ackSequenceNr != SkipAck) {
        val snr = cmd.ackSequenceNr
        val key = InMsgKey(partition(snr), cmd.ackProcessorId, snr).toBytes
        val putAck = new PutRequest(TableNameBytes, key, ColumnFamilyNameBytes, ackColumnBytes(cmd.channelId), Bytes.toBytes(cmd.channelId))
        client.put(putAck)
      } else Future.successful(())
      client.flush()
      val wrt = for {
        _ <- putMsgFuture
        _ <- putCtrFuture
        a <- putAckFuture
      } yield a
      wrt.map(_ => ())
    }

    def executeWriteAck(cmd: WriteAck): Future[Any] = {
      val snr = cmd.ackSequenceNr
      val key = InMsgKey(partition(snr), cmd.processorId, snr).toBytes
      val put = new PutRequest(TableNameBytes, key, ColumnFamilyNameBytes, ackColumnBytes(cmd.channelId), Bytes.toBytes(cmd.channelId))
      val wrt = client.put(put); client.flush()
      wrt.map(_ => ())
    }

    def executeDeleteOutMsg(cmd: DeleteOutMsg): Future[Any] = {
      val snr = cmd.msgSequenceNr
      val key = OutMsgKey(partition(snr), cmd.channelId, snr).toBytes
      val del = new DeleteRequest(TableNameBytes, key)
      val wrt = client.delete(del); client.flush()
      wrt.map(_ => ())
    }
  }

  def replayer = new Replayer {
    def executeBatchReplayInMsgs(cmds: Seq[ReplayInMsgs], p: (Message, ActorRef) => Unit, sdr: ActorRef, toSequenceNr: Long): Future[Any] = {
      Future.sequence(cmds.map(cmd => scan(
        InMsgKey(-1, cmd.processorId, cmd.fromSequenceNr),
        InMsgKey(-1, cmd.processorId, toSequenceNr), msg => p(msg, cmd.target)))) andThen { case _ => sdr ! ReplayDone }
    }

    def executeReplayInMsgs(cmd: ReplayInMsgs, p: (Message) => Unit, sdr: ActorRef, toSequenceNr: Long): Future[Any] = {
      scan(
        InMsgKey(-1, cmd.processorId, cmd.fromSequenceNr),
        InMsgKey(-1, cmd.processorId, toSequenceNr), p) andThen { case _ => sdr ! ReplayDone }
    }

    def executeReplayOutMsgs(cmd: ReplayOutMsgs, p: (Message) => Unit, sdr: ActorRef, toSequenceNr: Long): Future[Any] = {
      scan(
        OutMsgKey(-1, cmd.channelId, cmd.fromSequenceNr),
        OutMsgKey(-1, cmd.channelId, toSequenceNr), p)
    }

    def scan(startKey: Key, stopKey: Key, p: (Message) => Unit): Future[Any] = {
      val chunkSize = props.replayChunkSize
      val promise = Promise[Any]

      def scanPartitions(startKey: Key, stopKey: Key) = {
        def scanPartition(partition: Int) = scanLow(
          startKey.withPartition(partition),
          stopKey.withPartition(partition), p)
        Future.sequence(0 until props.partitionCount map(scanPartition)).map(_.flatten.sortBy(_.sequenceNr))
      }

      def go(startKey: Key, stopKey: Key) {
        val from = startKey.sequenceNumber
        val to = stopKey.sequenceNumber
        if (from > to) promise.success(())
        else if ((to - from) < chunkSize ) scanPartitions(startKey, stopKey) onComplete {
          case Success(s) => { s.foreach(p); promise.success(()) }
          case Failure(e) => promise.failure(e)
        }
        else scanPartitions(startKey, startKey.withSequenceNumber(_ + chunkSize)) onComplete {
          case Success(s) => { s.foreach(p); go(startKey.withSequenceNumber(_ + chunkSize + 1), stopKey) }
          case Failure(e) => promise.failure(e)
        }
      }

      go(startKey, stopKey)
      promise.future
    }

    def scanLow(startKey: Key, stopKey: Key, p: (Message) => Unit): Future[Seq[Message]] = {
      val scanner = client.newScanner(TableNameBytes)

      scanner.setFamily(ColumnFamilyNameBytes)
      scanner.setStartKey(startKey.toBytes)
      scanner.setStopKey(stopKey.withSequenceNumber(_ + 1).toBytes)

      def go(): Future[Seq[Message]] = {
        deferredToFuture(scanner.nextRows()).flatMap { rows =>
          rows match {
            case null => {
              scanner.close()
              Future.successful(Nil)
            }
            case _ => {
              val msgs = for {
                row <- rows.asScala
                msg <- rowToMessage(row.asScala)
              } yield msg
              go().map(msgs ++ _)
            }
          }
        }
      }
      go()
    }

    def rowToMessage(row: Seq[KeyValue]): Option[Message] = {
      var msgo: Option[Message] = None
      var acks: List[Int] = Nil

      row foreach { kv =>
        new String(kv.qualifier) match {
          case MsgColumnName => {
            msgo = Some(serialization.deserializeMessage(kv.value()))
          }
          case anyColumnName => if (anyColumnName.startsWith(AckColumnPrefix)) {
            val ack = anyColumnName.substring(AckColumnPrefix.length).toInt
            acks = ack :: acks
          }
        }
      }

      msgo.map(_.copy(acks = acks))
    }
  }

  def storedCounter = {
    import scala.concurrent.duration._

    // TODO: start scanning from stored counter
    // TODO: abstract over maxSequenceNr and replayer#scanLow

    def maxSequenceNr(bucket: Int): Future[Long] = {
      val scanner = client.newScanner(TableNameBytes)

      scanner.setFamily(ColumnFamilyNameBytes)
      scanner.setQualifier(SequenceNrColumnNameBytes)
      scanner.setStartKey(InMsgKey(bucket, 0, 0).toBytes)
      scanner.setStopKey(InMsgKey(bucket + 1 , 0, 0).toBytes)

      def go(): Future[Long] = {
        deferredToFuture(scanner.nextRows(1024)).flatMap { rows =>
          rows match {
            case null => {
              scanner.close()
              Future.successful(0)
            }
            case _ => {
              val vals: Seq[Long] = for {
                a <- rows.asScala
                b <- a.asScala
              } yield longFromBytes(b.value())
              go().map(_ max vals.max)
            }
          }
        }
      }
      go()
    }

    // TODO: make timeout configurable
    Await.result(Future.sequence(0 until props.partitionCount map (maxSequenceNr)).map(_.max), 1 minute)
  }


  override def start() {
    client = new HBaseClient(props.zookeeperQuorum)
  }

  override def stop() {
    client.shutdown()
  }
}
