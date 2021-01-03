package queries

object NeoQueries {
    const val DROP_DATABASE = "MATCH (n) DETACH DELETE n"
    const val LOAD_NODES =
            "USING PERIODIC COMMIT 1000 LOAD CSV WITH HEADERS FROM \$file AS row " +
                    " CREATE (e:Employee {employeeId: toInteger(row.id), firstName: row.firstName," +
                    " lastName: row.lastName, isInfected: toBoolean(row.isInfected)})"

    const val LOAD_EDGES =
            "USING PERIODIC COMMIT 1000 LOAD CSV WITH HEADERS FROM \$file AS row " +
                    "MATCH (c: Employee{employeeId: toInteger(row.id1)}) " +
                    "MATCH (b: Employee{employeeId: toInteger(row.id2)}) " +
                    "CREATE (c)-[:HAD_CONTACT{day: toInteger(row.day)}]->(b)"

    val FIND_INCUBATION_BASED_ON_INFECTED = { day: Int ->
        "match (a:Employee{isInfected: true})-[r:HAD_CONTACT]-(b:Employee{isInfected: true})" +
                " where r.day <> $day return max($day - r.day) as day"
    }
    const val FIND_R_BASED_ON_INFECTED = "call {match (a)-[:HAD_CONTACT]-(r) " +
            "where a.isInfected and r.isInfected return id(a) as id, count(r) - 1 as rep} " +
            "return max(rep) as max"

    const val FIND_R_BASED_ON_ITERATION = "call {match (a:Employee{isInfected:true}) where a.iteration <> 0 " +
            "optional match (a)-[r:INFECTED]->(b) " +
            "return a, count(b) as rep} " +
            "return a.iteration, avg(rep)"

    const val FIND_ALL_INFECTED = "match ()-[:INFECTED]->(n) return id(n)"

    const val CREATE_PATIENT_ZERO = "create (e:Employee{employeeId: 0, firstName: \"Patient\", lastName: \"Zero\"," +
            " isInfected: true, iteration: 0})" +
            " with e match (b) where b.isInfected = true and id(e) <> id(b)" +
            " create (e)-[:INFECTED{day: \$day}]->(b) return e, b"

    val PERFORM_ITERATION = { day: Int, iteration: Int, r: Int ->
        "MATCH (n:Employee{isInfected: true}) call { with n match (n)-[r:HAD_CONTACT]-(o), ()-[g:INFECTED]->(n) " +
                "where $day >= r.day >= g.day + 2 and n.iteration = $iteration with o order by o.employeeId desc limit $r  " +
                "return collect(o) as contacts} match (n)-[d:HAD_CONTACT]-(b) " +
                "where b in contacts and b.isInfected = false" +
                " set b.isInfected = true, b.iteration = $iteration + 1" +
                " create (n)-[r:INFECTED{day:d.day}]->(b) return b.employeeId"
    }
    const val REMOVE_TEMPORARY_LABELS = "CALL db.labels() YIELD label WHERE toInteger(label) > -1 " +
            "WITH collect(label) AS labels MATCH (p:Employee) \n" +
            "WITH collect(p) AS people, labels\n" +
            "CALL apoc.create.removeLabels(people, labels)\n" +
            "YIELD node RETURN node, labels(node) AS labels;"

    const val ADD_INFECTED_LABELS = "MATCH (n) WITH DISTINCT toString(n.iteration) AS sick," +
            " collect(DISTINCT n) AS infected  \n" +
            " CALL apoc.create.addLabels(infected, [sick]) YIELD node RETURN *"

    const val REMOVE_DUPLICATES = "call {match ()-[r:INFECTED]->(b) return  b, min(r) as maxes} " +
            "match ()-[r:INFECTED]->(b) where id(r) <> id(maxes) delete r"
    const val INIT_ITERATION = "MATCH (n) where n.isInfected = true set n.iteration = 1 return 0"
    const val DROP_INDEXES = "CALL apoc.schema.assert({},{})"
    const val ADD_INDEX_ON_EMPLOYEE_ID = "CREATE INDEX ON :Employee(employeeId);"
    const val ADD_INDEX_ON_ITERATION = "CREATE INDEX ON :Employee(iteration);"
    const val ADD_INDEX_ON_IS_INFECTED = "CREATE INDEX ON :Employee(isInfected);"
    const val FIND_MAX_ITERATION = "match (n) return max(n.iteration)"
}