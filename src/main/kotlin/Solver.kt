import calc.Calculator
import calc.NeoCalculator
import calc.SqlCalculator
import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import com.google.gson.Gson
import model.Result
import model.TotalResult
import java.io.File
import java.nio.file.Paths
import kotlin.system.measureTimeMillis

class Solver(propertiesFile: String, private val contactsFile: String,
             private val employeeFile: String, randomFactor: Int,
             private val folder: String) {

    private val times = mutableMapOf<String, List<Long>>()
    private val results = mutableMapOf<String, TotalResult>()
    private val params: List<String>
    private val calculators: List<Calculator>

    init {
        params = extractParameters()

        val dayOfInvestigation = params[3].toInt()
        calculators = listOf(
                SqlCalculator(propertiesFile, contactsFile, employeeFile, dayOfInvestigation, randomFactor),
                NeoCalculator(propertiesFile, contactsFile, employeeFile, dayOfInvestigation, randomFactor)
        )
    }

    fun solve() {
        calculators.forEach { c ->
            val t = mutableListOf<Long>()
            c.cleanDatabase()
            t.add(addNodes(c))
            t.add(addEdges(c))

            val dataPreparation = prepareData(c)
            t.add(dataPreparation.third)

            val simulationResult = doSimulation(c, dataPreparation.first, dataPreparation.second)
            val simulationTime = simulationResult.second

            t.add(simulationTime)
            c.close()

            times[c.type()] = t
            results[c.type()] = TotalResult(dataPreparation.first, dataPreparation.second, simulationResult.first)
        }
        writeResults()
        writeTimes()
    }

    private fun addNodes(c: Calculator): Long {
        return measureTimeMillis {
            c.loadEmployees()
        }
    }

    private fun addEdges(c: Calculator): Long {
        return measureTimeMillis {
            c.loadContacts()
        }
    }

    private fun prepareData(c: Calculator): Triple<Int, Int, Long> {
        var r: Int
        var incubation: Int
        val time = measureTimeMillis {
            r = c.findR()
            incubation = c.findIncubation()
        }

        return Triple(r, incubation, time)
    }

    private fun doSimulation(c: Calculator, r: Int, incubationTime: Int): Pair<Result, Long> {
        var finalResult: Result
        val time = measureTimeMillis {
            finalResult = c.simulation(r,
                    incubationTime)
        }
        return Pair(finalResult, time)
    }

    private fun writeTimes() {
        val pathToWrite = Paths.get("").toAbsolutePath().toString().replace("\\", "/")
        val folderToWrite = File("$pathToWrite/$folder")
        val fileToWrite = File("$pathToWrite/$folder/result_times.csv")

        if (!folderToWrite.exists()) {
            folderToWrite.mkdirs()
        }

        if (!fileToWrite.exists()) {
            val headers = listOf("Type", "Workers", "Workers per group", "People infected",
                    "Day of investigation", "Load nodes", "Load edges", "Parameters",
                    "Simulation")
            csvWriter().writeAll(listOf(headers), fileToWrite)
        }
        times.forEach { (type, times) ->
            val res = listOf(type) + params + times
            csvWriter().writeAll(listOf(res), fileToWrite, append = true)
        }
    }

    private fun writeResults() {
        val pathToWrite = Paths.get("").toAbsolutePath().toString().replace("\\", "/")
        val folderToWrite = File("$pathToWrite/$folder")

        val name = "result_" + params.joinToString("_")

        val fileToWrite = File("$pathToWrite/$folder/$name.json")
        if (!folderToWrite.exists()) {
            folderToWrite.mkdirs()
        }
        if (fileToWrite.exists()) {
            fileToWrite.delete()
        }
        fileToWrite.printWriter().use { out -> out.println(Gson().toJson(results)) }
    }

    private fun extractParameters(): List<String> {
        val regex = """\S+_(\d+)_(\d+)_(\d+)_(\d+).csv""".toRegex()
        val matchResultNode = regex.find(employeeFile)
        val matchResultEdge = regex.find(contactsFile)
        if (matchResultEdge != null && matchResultNode != null) {
            return matchResultEdge.destructured.toList()
        } else throw IllegalArgumentException("Invalid name of file!")
    }
}
