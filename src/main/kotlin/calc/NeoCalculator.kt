package calc

import model.Result
import org.neo4j.driver.*
import queries.NeoQueries
import java.io.FileInputStream
import java.nio.file.Paths
import java.util.*
import kotlin.random.Random

class NeoCalculator(propertiesFile: String,
                    contactsFile: String,
                    employeesFile: String,
                    private val currentDay: Int,
                    private val randomFactor: Int) : Calculator {
    private val driver: Driver
    private val minimalIncubation = 2
    private val employeesFilePath: String
    private val contactsFilePath: String
    private var iteration = 0
    private val properties = Properties()

    init {
        properties.load(FileInputStream(propertiesFile))
        driver = GraphDatabase.driver(
                properties["neo.host"].toString(),
                AuthTokens.basic(properties["neo.username"].toString(),
                        properties["neo.password"].toString()
                )
        )

        contactsFilePath = convertFileName(contactsFile)
        employeesFilePath = convertFileName(employeesFile)
    }

    override fun type(): String {
        return "neo"
    }

    override fun cleanDatabase() {
        driver.session().use { session ->
            session.run(NeoQueries.DROP_DATABASE)
            session.run(NeoQueries.DROP_INDEXES)
        }
    }

    override fun loadEmployees() {
        driver.session().use { session ->
            session.run(NeoQueries.ADD_INDEX_ON_EMPLOYEE_ID)
            session.run(NeoQueries.ADD_INDEX_ON_IS_INFECTED)
            session.run(NeoQueries.ADD_INDEX_ON_ITERATION)
            session.run(NeoQueries.LOAD_NODES, Values.parameters("file", employeesFilePath))
        }
    }

    override fun loadContacts() {
        driver.session().use { session ->
            session.run(NeoQueries.LOAD_EDGES, Values.parameters("file", contactsFilePath))
        }
    }


    override fun findIncubation(): Int {
        driver.session().use { session ->
            return session.run(NeoQueries.FIND_INCUBATION_BASED_ON_INFECTED(currentDay)).list()[0][0].asInt()
        }

    }

    override fun findR(): Int {
        driver.session().use { session ->
            return session.run(NeoQueries.FIND_R_BASED_ON_INFECTED).list()[0][0].asInt()
        }
    }

    override fun simulation(r: Int, incubationTime: Int): Result {
        driver.session().use { session ->
            session.run(NeoQueries.INIT_ITERATION)
            return Result(infect(session, r, incubationTime), maxIteration(session), rMap(session))
        }
    }

    private fun maxIteration(session: Session): Int {
        return session.run(NeoQueries.FIND_MAX_ITERATION)
                .single()[0].asInt()
    }

    override fun close() {
        driver.close()
    }

    private fun rMap(session: Session): MutableMap<Int, Double> {
        val rMap = mutableMapOf<Int, Double>()
        session.run(NeoQueries.FIND_R_BASED_ON_ITERATION)
                .list()
                .forEach { iteration -> rMap[iteration[0].asInt()] = iteration[1].asDouble() }
        return rMap
    }

    private fun infect(session: Session, r: Int, incubationTime: Int): Int {
        createPatientZero(session, currentDay - incubationTime - minimalIncubation)
        createSimulation(session, r, currentDay - minimalIncubation)
        removeDuplicates(session)
        return session.run(NeoQueries.FIND_ALL_INFECTED).list().size
    }

    private fun createPatientZero(session: Session, day: Int) {
        session.run(NeoQueries.CREATE_PATIENT_ZERO, Values.parameters("day", day))
    }

    private fun createSimulation(session: Session, r: Int, minimalDay: Int) {
        var infectionsNumber: Int
        do {
            iteration++
            infectionsNumber = prepareIteration(session, minimalDay, iteration, r)
        } while (infectionsNumber > 0)
        for (i in 1..iteration) {
            prepareIteration(session, currentDay, i, r)
        }
    }

    private fun prepareIteration(session: Session, minimalDay: Int, iteration: Int, r: Int): Int {
        return session.run(NeoQueries.PERFORM_ITERATION(minimalDay, iteration, randomR(r)))
                .list()
                .size
    }

    private fun randomR(r: Int): Int {
        if (randomFactor == 0) return r
        val proposedR = Random.nextInt(r - randomFactor, r + randomFactor)
        return if (proposedR >= 0) proposedR else 0
    }

    private fun removeDuplicates(session: Session) {
        session.run(NeoQueries.REMOVE_DUPLICATES)
    }

    private fun convertFileName(name: String): String {
        return "file:/" + Paths.get("").toAbsolutePath().toString().replace("\\", "/") + "/" + name.replace("\\", "/")
    }
}