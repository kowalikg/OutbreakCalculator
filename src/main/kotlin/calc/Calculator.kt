package calc

import model.Result

interface Calculator {

    fun type(): String
    fun cleanDatabase()
    fun loadEmployees()
    fun loadContacts()
    fun findR(): Int
    fun findIncubation() : Int
    fun simulation(r: Int, incubationTime: Int): Result
    fun close()
}