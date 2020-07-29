package io.realm.benchmark

import androidx.benchmark.junit4.measureRepeated
import androidx.test.platform.app.InstrumentationRegistry
import com.couchbase.lite.*
import com.couchbase.lite.Function


class CouchbaseBenchmarks(size: Long): Benchmarks(size) {

    private lateinit var db: Database

    override fun before() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        CouchbaseLite.init(context)
        val dbName = "benchmark.couchbase"
        db = Database(dbName)
    }

    override fun after() {
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

    override fun simpleQuery() {
        benchmarkRule.measureRepeated {
            runWithTimingDisabled { addObjects() }

            val query = QueryBuilder.select(
                    SelectResult.expression(Meta.id),
                    SelectResult.all()
            ).from(DataSource.database(db))
                    .where(Expression.property(HIRED).equalTo(Expression.booleanValue(false))
                            .and(Expression.property(AGE).between(Expression.intValue(20), Expression.intValue(50)))
                            .and(Expression.property(NAME).equalTo(Expression.string("Foo0"))))

            query.execute()

            runWithTimingDisabled { deleteObjects() }
        }
    }

    override fun simpleWrite() {
        var i: Long = 0
        benchmarkRule.measureRepeated {
            runWithTimingDisabled { deleteObjects() }
            val obj = MutableDocument(i.toString(), mapOf(
                    NAME to dataGenerator.getEmployeeName(i),
                    AGE to dataGenerator.getEmployeeAge(i),
                    HIRED to dataGenerator.getHiredBool(i)
            ))
            db.save(obj)
            i++
        }
    }

    override fun batchWrite() {
        val list: MutableList<MutableDocument> = mutableListOf()
        for (i in 0 until size) {
            list.add(MutableDocument(i.toString(), mapOf(
                    NAME to dataGenerator.getEmployeeName(i),
                    AGE to dataGenerator.getEmployeeAge(i),
                    HIRED to dataGenerator.getHiredBool(i)
            )))
        }
        benchmarkRule.measureRepeated {
            runWithTimingDisabled { deleteObjects() }
            db.inBatch {
                list.forEach { db.save(it) }
            }
        }
    }

    override fun fullScan() {
        benchmarkRule.measureRepeated {
            runWithTimingDisabled { addObjects() }

            val query = QueryBuilder.select(
                    SelectResult.expression(Meta.id),
                    SelectResult.all()
            ).from(DataSource.database(db))
                    .where(Expression.property(HIRED).equalTo(Expression.booleanValue(true))
                            .and(Expression.property(AGE).between(Expression.intValue(-2), Expression.intValue(-1)))
                            .and(Expression.property(NAME).equalTo(Expression.string("Smile1"))))

            query.execute()

            runWithTimingDisabled { deleteObjects() }
        }
    }

    override fun delete() {
        benchmarkRule.measureRepeated {
            runWithTimingDisabled { addObjects() }
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

    override fun sum() {
        benchmarkRule.measureRepeated {
            runWithTimingDisabled { addObjects() }
            val query = QueryBuilder.select(SelectResult.expression(Function.sum(Expression.property(AGE))))
                    .from(DataSource.database(db))
            @Suppress("UNUSED_VARIABLE")
            val sum = query.execute().first().getLong(0)
            runWithTimingDisabled { deleteObjects() }
        }
    }

    override fun count() {
        benchmarkRule.measureRepeated {
            runWithTimingDisabled { addObjects() }
            val query = QueryBuilder.select(SelectResult.expression(Function.count(Expression.all())))
                    .from(DataSource.database(db))
            @Suppress("UNUSED_VARIABLE")
            val count = query.execute().first().getLong(0)
            runWithTimingDisabled { deleteObjects() }
        }
    }

    companion object {
        private const val NAME = "name"
        private const val AGE = "age"
        private const val HIRED = "hired"
    }

}
