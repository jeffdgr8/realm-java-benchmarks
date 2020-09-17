package io.realm.benchmark

import androidx.test.platform.app.InstrumentationRegistry
import com.couchbase.lite.*
import com.couchbase.lite.Function
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(Parameterized::class)
class CouchbaseMemTests(private val size: Long) {

    private lateinit var db: Database

    private val dataGenerator = DataGenerator()

    private fun runRepeated(block: () -> Unit) {
        repeat(LOOPS) {
            // With no gc(): memory usage climbs quickly at first, then slows but continues to rise until
            // reaching close to 2GB. If LOOPS is large, eventually may crash the app or device.
            // ~3:30 for 10 LOOPS
            // ~13:10 for 20 LOOPS

            // When run: no significant difference in memory usage, actual garbage collection only runs
            // occasionally with little immediate effect. Up to 2GB used, but backs off to as low as
            // 400MB at times before climbing again when LOOPS is large.
            // ~3:30 for 10 LOOPS
            // ~12:20 for 20 LOOPS
            //System.gc()

            // When run: garbage collection is forced to run frequently and contains memory usage usually
            // under 200MB used. Also executes much faster. Memory usage does still trend up slightly
            // over time when LOOPS is large (from ~180MB up to ~280MB when LOOPS = 100).
            // ~1:10 for 10 LOOPS
            // ~3:30 for 20 LOOPS
            //Runtime.getRuntime().gc()

            block()
        }
    }

    @Before
    fun before() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        CouchbaseLite.init(context)
        val dbName = "benchmark.couchbase"
        db = Database(dbName)
    }

    @After
    fun after() {
        db.close()
    }

    private fun addObjects() {
        val list: MutableList<MutableDocument> = mutableListOf()
        for (i in 0 until size) {
            list.add(MutableDocument(i.toString(), mapOf(
                    NAME to dataGenerator.getEmployeeName(i),
                    AGE to dataGenerator.getEmployeeAge(i),
                    HIRED to dataGenerator.getHiredBool(i)
            )))
        }
        db.inBatch {
            list.forEach { db.save(it) }
        }
    }

    private fun deleteObjects() {
        val query = QueryBuilder.select(SelectResult.expression(Meta.id))
                .from(DataSource.database(db))
        val results = query.execute()
        for (result in results) {
            val id = result.getString(0)
            db.getDocument(id)?.let {
                db.delete(it)
            }
        }
    }

    @Test
    fun simpleQuery() {
        runRepeated {
            addObjects()

            val query = QueryBuilder.select(
                    SelectResult.expression(Meta.id),
                    SelectResult.all()
            ).from(DataSource.database(db))
                    .where(Expression.property(HIRED).equalTo(Expression.booleanValue(false))
                            .and(Expression.property(AGE).between(Expression.intValue(20), Expression.intValue(50)))
                            .and(Expression.property(NAME).equalTo(Expression.string("Foo0"))))

            query.execute()

            deleteObjects()
        }
    }

    @Test
    fun simpleWrite() {
        var i: Long = 0
        runRepeated {
            deleteObjects()
            val obj = MutableDocument(i.toString(), mapOf(
                    NAME to dataGenerator.getEmployeeName(i),
                    AGE to dataGenerator.getEmployeeAge(i),
                    HIRED to dataGenerator.getHiredBool(i)
            ))
            db.save(obj)
            i++
        }
    }

    @Test
    fun batchWrite() {
        val list: MutableList<MutableDocument> = mutableListOf()
        for (i in 0 until size) {
            list.add(MutableDocument(i.toString(), mapOf(
                    NAME to dataGenerator.getEmployeeName(i),
                    AGE to dataGenerator.getEmployeeAge(i),
                    HIRED to dataGenerator.getHiredBool(i)
            )))
        }
        runRepeated {
            deleteObjects()
            db.inBatch {
                list.forEach { db.save(it) }
            }
        }
    }

    @Test
    fun fullScan() {
        runRepeated {
            addObjects()

            val query = QueryBuilder.select(
                    SelectResult.expression(Meta.id),
                    SelectResult.all()
            ).from(DataSource.database(db))
                    .where(Expression.property(HIRED).equalTo(Expression.booleanValue(true))
                            .and(Expression.property(AGE).between(Expression.intValue(-2), Expression.intValue(-1)))
                            .and(Expression.property(NAME).equalTo(Expression.string("Smile1"))))

            query.execute()

            deleteObjects()
        }
    }

    @Test
    fun delete() {
        runRepeated {
            addObjects()
            val query = QueryBuilder.select(SelectResult.expression(Meta.id))
                    .from(DataSource.database(db))
            val results = query.execute()
            for (result in results) {
                val id = result.getString(0)
                db.getDocument(id)?.let {
                    db.delete(it)
                }
            }
        }
    }

    @Test
    fun sum() {
        runRepeated {
            addObjects()
            val query = QueryBuilder.select(SelectResult.expression(Function.sum(Expression.property(AGE))))
                    .from(DataSource.database(db))
            @Suppress("UNUSED_VARIABLE")
            val sum = query.execute().first().getLong(0)
            deleteObjects()
        }
    }

    @Test
    fun count() {
        runRepeated {
            addObjects()
            val query = QueryBuilder.select(SelectResult.expression(Function.count(Expression.all())))
                    .from(DataSource.database(db))
            @Suppress("UNUSED_VARIABLE")
            val count = query.execute().first().getLong(0)
            deleteObjects()
        }
    }

    companion object {
        private const val NAME = "name"
        private const val AGE = "age"
        private const val HIRED = "hired"

        private const val LOOPS = 10

        @JvmStatic
        @Parameterized.Parameters(name = "size={0}")
        fun data(): Array<Long> {
            return arrayOf(10, 100, 1000)
        }
    }

}
