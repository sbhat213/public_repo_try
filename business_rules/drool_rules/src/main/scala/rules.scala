
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.kie.api.{KieBase, KieServices}
import org.drools.core.factmodel.ClassDefinition
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql._;


object rules extends Serializable {
  Logger.getLogger("org").setLevel(Level.WARN)
  val rules = loadRules()

  def main(args: Array[String]): Unit = {

  }

  def applyDroolRules(df: DataFrame, packageName: String, typeName: String): DataFrame = {
    val index = df.schema
    val encoder = RowEncoder(index)

    val df3 = df.map(row => {
      val ksession = rules.newKieSession();
      val mainObjectFact = rules.getFactType(packageName, typeName)
      val mainObject = mainObjectFact.newInstance
      val fields = mainObjectFact.getFields()
      for (i <- 0 until fields.size()) {
        val schema = fields.get(i)
        mainObjectFact.set(mainObject, schema.getName, row.getAs(schema.getName))
      }
      ksession.insert(mainObject)
      ksession.fireAllRules();
      getARow(row, mainObjectFact, ksession.getObjects.toArray().head)
    })(encoder)
    df3
  }

  def getARow(row: Row, mainObjectFactVal: Object, mainObject: Object): Row = {

    val mainObjectFact = mainObjectFactVal.asInstanceOf[ClassDefinition]

    val columnArray = new Array[Any](row.length)
    var i = 0;
    for (field <- row.schema) {
      var value: Any = mainObjectFact.get(mainObject, field.name)
      if (value == null && row.get(i) != null) {
        value = row.get(i)
      }
      columnArray(i) = value
      i = i + 1;
    }
    Row.fromSeq(columnArray)
  }

  def loadRules(): KieBase = {
    val kieServices = KieServices.Factory.get()
    val kieContainer = kieServices.getKieClasspathContainer()
    kieContainer.getKieBase
  }

}
