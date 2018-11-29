import java.sql.Connection
import java.sql.SQLException

/**
 * @author andrew
 */
class Sync {

  fun syncDatabaseTables(serverCon: Connection, localCon: Connection) {
    try {
      val databaseMetaData = localCon.metaData
      val rs = databaseMetaData.getTables(null, null, null, null)
      if (!rs.next()) {
        println("Unable to find any tables")
        rs.close()
      } else {
        do {
          if ("TABLE".equals(rs.getString("TABLE_TYPE"), ignoreCase = true)) {
            dumpTable(localCon, serverCon, rs.getString("TABLE_NAME"))
          }
        } while (rs.next())
        rs.close()
      }
    } catch (e: SQLException) {
      println("Error building query")
      e.printStackTrace()
    }
  }

  private fun dumpTable(localCon: Connection, serverCon: Connection, tableName: String) {
    try {
      serverCon.prepareStatement("DELETE FROM $tableName").executeUpdate()
      val rs = localCon.prepareStatement("SELECT * FROM $tableName").executeQuery()
      val columnCount = rs.metaData.columnCount

      val result = StringBuilder()
      result.append("INSERT INTO ").append(tableName).append(" VALUES (")
      for (i in 1..columnCount) {
        result.append(if (i != columnCount) "?," else "?")
      }
      result.append(")")

      val ps = serverCon.prepareStatement(result.toString())
      println("Table: $result")
      while (rs.next()) {
        for (i in 1..columnCount) {
          val value = rs.getObject(i)
          ps.setObject(i, value)
        }
        ps.addBatch()
      }
      ps.executeLargeBatch()
      rs.close()
      ps.close()
    } catch (e: SQLException) {
      println("Unable to dump table $tableName")
      e.printStackTrace()
    }

  }
}