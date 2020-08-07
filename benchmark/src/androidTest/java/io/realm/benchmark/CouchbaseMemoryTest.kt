package io.realm.benchmark

import android.app.ActivityManager
import android.content.Context.ACTIVITY_SERVICE
import android.util.Log
import androidx.test.platform.app.InstrumentationRegistry
import com.couchbase.lite.*
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.text.NumberFormat

@RunWith(Parameterized::class)
class CouchbaseMemoryTest(val size: Long) {

    private lateinit var db: Database
    private val dataGenerator = DataGenerator()

    private lateinit var activityManager: ActivityManager
    private val memoryInfo = ActivityManager.MemoryInfo()
    private var freeMemory: Long = 0
    private val formatter = NumberFormat.getInstance()

    @Before
    fun before() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        CouchbaseLite.init(context)
        val dbName = "benchmark.couchbase"
        db = Database(dbName)
        activityManager = InstrumentationRegistry.getInstrumentation().targetContext.getSystemService(ACTIVITY_SERVICE) as ActivityManager
        logBeginMemory()
    }

    @After
    fun after() {
        db.delete()
        db.close()
    }

    private fun logBeginMemory() {
        Log.i(TAG, "----------")
        Log.i(TAG, "Testing $size documents looping $LOOPS times")
        activityManager.getMemoryInfo(memoryInfo)
        Log.i(TAG, "Total memory = ${formatter.format(memoryInfo.totalMem / 1024)} KB")
        val runtime = Runtime.getRuntime()
        Log.i(TAG, "Max memory = ${formatter.format(runtime.maxMemory() / 1024)} KB")
        freeMemory = runtime.freeMemory() / 1024
        Log.i(TAG, "Free memory = ${formatter.format(freeMemory)} KB")
    }

    private fun logFreeMemory() {
        val oldFreeMemory = freeMemory
        freeMemory = Runtime.getRuntime().freeMemory() / 1024
        val diff = freeMemory - oldFreeMemory
        Log.i(TAG, "Free memory = ${formatter.format(freeMemory)} KB ${if (diff > 0) "+" else ""}${formatter.format(diff)} KB")
    }

    private fun addObjects(size: Long) {
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
    fun saveAndDeleteDocs() {
        repeat(LOOPS) {
            addObjects(size)
            deleteObjects()
            logFreeMemory()
        }
    }

    companion object {
        private const val TAG = "Couchbase"

        private const val LOOPS = 100

        @JvmStatic
        @Parameterized.Parameters(name = "size={0}")
        fun data(): Array<Long> {
            return arrayOf(10, 100, 1000);
        }

        private const val NAME = "name"
        private const val AGE = "age"
        private const val HIRED = "hired"
    }
}
