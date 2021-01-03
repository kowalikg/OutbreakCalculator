package model

class Result(val totalInfected: Int, val iterationNumber: Int, val rMap: Map<Int, Double>) {
    override fun toString(): String {
        return "{Total infected: $totalInfected, Number of iterations: $iterationNumber, R: $rMap ]"
    }
}