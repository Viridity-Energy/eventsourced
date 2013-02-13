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
package org.eligosource.eventsourced.journal.leveldb

import scala.concurrent._

import org.fusesource.leveldbjni.JniDBFactory._
import org.iq80.leveldb._

import org.eligosource.eventsourced.core._
import org.eligosource.eventsourced.core.Journal._
import org.eligosource.eventsourced.journal.common._

import LeveldbReplay._
import akka.actor.ActorRef

/**
 * Experimental
 */
private [journal] class LeveldbJournalCW(props: LeveldbJournalProps, replayStrategyFactory: ReplayStrategyFactory) extends ConcurrentWriteJournal {
  import LeveldbJournalPS._
  import LeveldbJournalCW._

  val levelDbReadOptions = new ReadOptions().verifyChecksums(props.checksum)
  val levelDbWriteOptions = new WriteOptions().sync(props.fsync)
  val leveldb = factory.open(props.dir, new Options().createIfMissing(true))

  val serialization = Serialization(context.system)
  val replayStrategy = replayStrategyFactory(LeveldbReplayContext(context, props, levelDbReadOptions, leveldb))

  implicit def msgToBytes(msg: Message): Array[Byte] = serialization.serializeMessage(msg)
  implicit def msgFromBytes(bytes: Array[Byte]): Message = serialization.deserializeMessage(bytes)

  def asyncWriteTimeout = props.asyncWriteTimeout
  def asyncWriterCount = props.asyncWriterCount

  def writer(id: Int) = new Writer {

    // TODO: handle write error
    def executeWriteInMsg(cmd: WriteInMsg): Future[Any] = Future.successful {
      withBatch { batch =>
        val msg = cmd.message
        batch.put(counterKeyBytes(id), counterToBytes(msg.sequenceNr))
        batch.put(Key(cmd.processorId, 0, msg.sequenceNr, 0), msg.clearConfirmationSettings)
      }
    }

    // TODO: handle write error
    def executeWriteOutMsg(cmd: WriteOutMsg): Future[Any] = Future.successful {
      withBatch { batch =>
        val msg = cmd.message
        batch.put(counterKeyBytes(id), counterToBytes(msg.sequenceNr))
        batch.put(Key(Int.MaxValue, cmd.channelId, msg.sequenceNr, 0), msg.clearConfirmationSettings)
        if (cmd.ackSequenceNr != SkipAck)
          batch.put(Key(cmd.ackProcessorId, 0, cmd.ackSequenceNr, cmd.channelId), Array.empty[Byte])
      }
    }

    // TODO: handle write error
    def executeWriteAck(cmd: WriteAck): Future[Any] = Future.successful {
      val k = Key(cmd.processorId, 0, cmd.ackSequenceNr, cmd.channelId)
      leveldb.put(k, Array.empty[Byte], levelDbWriteOptions)
    }

    // TODO: handle write error
    def executeDeleteOutMsg(cmd: DeleteOutMsg): Future[Any] = Future.successful {
      val k = Key(Int.MaxValue, cmd.channelId, cmd.msgSequenceNr, 0)
      leveldb.delete(k, levelDbWriteOptions)
    }

    private def withBatch(p: WriteBatch => Unit) {
      val batch = leveldb.createWriteBatch()
      try {
        p(batch)
        leveldb.write(batch, levelDbWriteOptions)
      } finally {
        batch.close()
      }
    }
  }

  def replayer = new Replayer {
    /*
      Limiting replay up toSequenceNr avoids that concurrent writes, that may occur
      during a replay, are not delivered to targets (processors, channels). This is
      currently implemented using a filtering continuation in the following three
      methods.

      Actually, this filtering is only necessary for event stores that do not support
      snapshots. Reading from LevelDB is actually done from a LevelDB snapshot, so one
      could have omitted the filtering here. But it is needed for other databases where
      concurrent writes (i.e. writes made concurrently to a replay) are not isolated
      from the reads made by the replay, and therefore shown here as an example.
    */

    def executeBatchReplayInMsgs(cmds: Seq[ReplayInMsgs], p: (Message, ActorRef) => Unit, sdr: ActorRef, toSequenceNr: Long) {
      replayStrategy.batchReplayInMsgs(sdr, cmds, (m, a) => if (m.sequenceNr <= toSequenceNr) p(m, a) )
    }

    def executeReplayInMsgs(cmd: ReplayInMsgs, p: (Message) => Unit, sdr: ActorRef, toSequenceNr: Long) {
      replayStrategy.replayInMsgs(sdr, cmd, m => if (m.sequenceNr <= toSequenceNr) p(m))
    }

    def executeReplayOutMsgs(cmd: ReplayOutMsgs, p: (Message) => Unit, sdr: ActorRef, toSequenceNr: Long) {
      replayStrategy.replayOutMsgs(sdr, cmd, m => if (m.sequenceNr <= toSequenceNr) p(m))
    }
  }

  def storedCounter = {
    def counterValue(writerId: Int) = leveldb.get(counterKeyBytes(writerId), levelDbReadOptions) match {
      case null  => 0L
      case bytes => counterFromBytes(bytes)
    }

    val maxWritersLength = leveldb.get(WritersKeyBytes, levelDbReadOptions) match {
      case null  => 0
      case bytes => counterFromBytes(bytes).toInt
    }

    if (writers.length > maxWritersLength) {
      leveldb.put(WritersKeyBytes, counterToBytes(writers.length))
    }

    1 to maxWritersLength map(counterValue) toList match {
      case Nil => 0L
      case cvs => cvs.max
    }
  }

  override def stop() {
    leveldb.close()
  }
}

object LeveldbJournalCW {
  import LeveldbJournalPS._

  val WritersKeyBytes = keyToBytes(Key(0, 0, 0L, 0)) // highest number of concurrent writers ever used
  def counterKeyBytes(writerId: Int) = keyToBytes(Key(0, writerId, 0L, 0))
}
