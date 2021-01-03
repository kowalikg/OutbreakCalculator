object Application {
    @JvmStatic
    fun main(args: Array<String>) {
        if (args.size != 5) {
            println("Invalid number of arguments!")
        } else {
            Solver(args[0], args[1], args[2], args[3].toInt(), args[4]).solve()
        }

    }

}