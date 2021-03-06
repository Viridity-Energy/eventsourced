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
package org.eligosource.eventsourced.journal.mongodb.casbah

import org.scalatest.BeforeAndAfterEach
import com.mongodb.casbah.Imports._
import org.eligosource.eventsourced.journal.common.JournalSpec

class MongodbJournalSpec extends JournalSpec with MongodbSpecSupport with BeforeAndAfterEach {

  // Since multiple embedded instances will run, each one must have a different port.
  override def mongoDBConnPort = 12345

  def journalProps = MongodbJournalProps(journalColl)

  def journalColl: MongoCollection = mongoConn(mongoDBName)(mongoDBColl)

  override def afterEach() {
    journalColl.dropCollection()
    mongoConn.dropDatabase(mongoDBName)
  }
}
