package calc

import model.Result
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction
import queries.SqlQueries
import java.io.FileInputStream
import java.nio.file.Paths
import java.util.*
import kotlin.random.Random

class SqlCalculator(propertiesFile: String, contactsFile: String,
                    employeesFile: String, private val currentDay: Int,
                    private val randomFactor: Int) : Calculator {
    private var contactsFilePath: String
    private var employeesFilePath: String
    private var minimalIncubation = 2
    private val properties = Properties()

    init {
        properties.load(FileInputStream(propertiesFile))
        employeesFilePath = prepareFilePath(employeesFile)
        contactsFilePath = prepareFilePath(contactsFile)
        val dbUrl = "jdbc:${properties["sql.host"].toString()}/${properties["sql.database_name"].toString()}"
        val dbUser = properties["sql.username"].toString()
        val dbPass = properties["sql.password"].toString()

        Database.connect(dbUrl, driver = "org.postgresql.Driver", user = dbUser, password = dbPass)
    }

    override fun type(): String {
        return "postgres"
    }

    override fun cleanDatabase() {
        transaction {
            exec(SqlQueries.DROP_TABLES)
        }
    }

    override fun loadEmployees() {
        transaction {
            exec(SqlQueries.QUERY_CREATE_TABLES)
            exec(SqlQueries.QUERY_ADD_INDEX_ON_EMPLOYEE_ID)
            exec(SqlQueries.QUERY_ADD_INDEX_ON_IS_INFECTED)
        }
        transaction {
            exec(SqlQueries.QUERY_INSERT_EMPLOYEES(employeesFilePath))
        }
    }

    override fun loadContacts() {
        transaction {
            exec(SqlQueries.QUERY_INSERT_CONTACTS(contactsFilePath))
        }
    }

    override fun findR(): Int {
        var r = 0
        transaction {
            exec(SqlQueries.QUERY_FIND_R_BASED_ON_INFECTED) { result ->
                while (result.next()) {
                    r = result.getInt(1)
                }
            }
        }
        return r
    }

    override fun findIncubation(): Int {
        var maxIncubation = 0
        transaction {
            exec(SqlQueries.QUERY_FIND_INCUBATION_TIMES(currentDay)) { result ->
                while (result.next()) {
                    maxIncubation = result.getInt(1)
                }
            }
        }
        return maxIncubation

    }

    private fun rMap(): MutableMap<Int, Double> {
        val rMap = mutableMapOf<Int, Double>()
        transaction {
            exec(SqlQueries.QUERY_R_BASED_ON_ITERATION) { result ->
                while (result.next()) {
                    rMap[result.getInt(1)] = result.getDouble(2)
                }
            }
        }
        return rMap
    }

    override fun simulation(r: Int, incubationTime: Int): Result {
        transaction {
            exec(SqlQueries.QUERY_PREPARE_FOR_SIMULATION(currentDay - minimalIncubation - incubationTime))
        }
        addFunctions()
        runSimulation(r)
        return Result(findAllInfected(), maxIteration(), rMap())

    }

    private fun maxIteration(): Int {
        var iter = 0
        transaction {
            exec(SqlQueries.QUERY_MAX_ITERATION) { result ->
                while (result.next()) {
                    iter = result.getInt(1)
                }
            }
        }
        return iter
    }

    override fun close() {
    }

    private fun addFunctions() {
        transaction {
            exec(SqlQueries.FUNCTION_POSSIBLE_CONTACTS)
            exec(SqlQueries.FUNCTION_POSSIBLE_INFECTIONS)
            exec(SqlQueries.FUNCTION_LIST_OF_INFECTIONS)
            exec(SqlQueries.FUNCTION_INFECT)
        }
    }

    private fun runSimulation(r: Int) {
        var iteration = 0
        transaction {
            var size = 0
            do {
                iteration++
                exec(SqlQueries.QUERY_DO_ITERATION(iteration, iteration + 1, currentDay - minimalIncubation, randomR(r))) { result ->
                    while (result.next()) {
                        size = result.getInt(1)
                    }
                }
            } while ((size > 0))
            for (i in 1..iteration) {
                exec(SqlQueries.QUERY_DO_ITERATION(i, i + 1, currentDay, randomR(r)))
            }
        }
    }

    private fun findAllInfected(): Int {
        var infected = 0
        transaction {
            exec(SqlQueries.QUERY_FIND_COUNT_OF_INFECTED) { result ->
                while (result.next()) {
                    infected = result.getInt(1)
                }
            }
        }
        return infected
    }

    private fun prepareFilePath(fileName: String): String {
        return Paths.get("").toAbsolutePath().toString()
                .replace("\\", "/") + "/" + fileName.replace("\\", "/")
    }

    private fun randomR(r: Int): Int {
        if (randomFactor == 0) return r
        val proposedR = Random.nextInt(r - randomFactor, r + randomFactor)
        return if (proposedR >= 0) proposedR else 0
    }
}