/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.operation

import org.apache.kyuubi.DeltaSuiteMixin

trait DeltaRowLevelOperationTests extends HiveJDBCTestHelper with DeltaSuiteMixin {

  private def createAndInitTable(tableName: String)(records: Seq[(Int, String)]): Unit = {
    withJdbcStatement() { stmt =>
      stmt.execute(
        s"""CREATE TABLE $tableName (
           |  id   INT,
           |  city STRING
           |) USING delta
           |""".stripMargin)
      stmt.execute(
        s"""INSERT INTO $tableName VALUES
           |${records.map(r => s"(${r._1}, '${r._2}')").mkString(",\n")}
           |""".stripMargin)
    }
  }

  test("delta update operation") {
    val testTbl = "delta_update"
    withJdbcStatement(testTbl) { stmt =>
      createAndInitTable(testTbl)((1, "HangZhou") :: (2, "Seattle") :: (3, "Beijing") :: Nil)
      stmt.execute(s"UPDATE $testTbl SET city = 'Shanghai' WHERE id IN (1)")
      stmt.execute(s"UPDATE $testTbl SET id = -1 WHERE city = 'Seattle'")

      val rs1 = stmt.executeQuery(s"SELECT * FROM $testTbl ORDER BY id")
      assert(rs1.next())
      assert(rs1.getInt("id") === -1)
      assert(rs1.getString("city") === "Seattle")
      assert(rs1.next())
      assert(rs1.getInt("id") === 1)
      assert(rs1.getString("city") === "Shanghai")
      assert(rs1.next())
      assert(rs1.getInt("id") === 3)
      assert(rs1.getString("city") === "Beijing")
      assert(!rs1.next())
    }
  }

  test("delta delete operation") {
    val testTbl = "delta_delete"
    withJdbcStatement(testTbl) { stmt =>
      createAndInitTable(testTbl)((1, "HangZhou") :: (2, "Seattle") :: (3, "Beijing") :: Nil)
      stmt.execute(s"DELETE FROM $testTbl WHERE WHERE id = 1")
      stmt.execute(s"DELETE FROM $testTbl WHERE WHERE city = 'Seattle'")

      val rs1 = stmt.executeQuery(s"SELECT * FROM $testTbl ORDER BY id")
      assert(rs1.next())
      assert(rs1.getInt("id") === 3)
      assert(rs1.getString("city") === "Beijing")
      assert(!rs1.next())
    }
  }

  test("delta merge into operation") {
    val testTblBase = "delta_merge_into_base"
    val testTblDelta = "delta_merge_into_delta"
    withJdbcStatement(testTblBase, testTblDelta) { stmt =>
      createAndInitTable(testTblBase)((1, "HangZhou") :: (2, "Seattle") :: (3, "Beijing") :: Nil)
      createAndInitTable(testTblDelta)((2, "Chicago") :: (3, "HongKong") :: (4, "London") :: Nil)
      stmt.execute(
        s"""MERGE INTO $testTblBase t
           |USING (SELECT * FROM $testTblDelta) s
           |ON t.id = s.id
           |WHEN MATCHED AND t.id = 2 THEN UPDATE SET *
           |WHEN MATCHED AND t.city = 'Beijing' THEN DELETE
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin)

      val rs1 = stmt.executeQuery(s"SELECT * FROM $testTblBase ORDER BY id")
      assert(rs1.next())
      assert(rs1.getInt("id") === 1)
      assert(rs1.getString("city") === "HangZhou")
      assert(rs1.next())
      assert(rs1.getInt("id") === 2)
      assert(rs1.getString("city") === "Chicago")
      assert(rs1.next())
      assert(rs1.getInt("id") === 4)
      assert(rs1.getString("city") === "London")
      assert(!rs1.next())
    }
  }
}
