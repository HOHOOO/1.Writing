import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import scala.Array.;
import scala.Function0;
import scala.MatchError;
import scala.Option;
import scala.Predef.;
import scala.Serializable;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.SeqLike;
import scala.collection.immutable.Range.Inclusive;
import scala.collection.immutable.StringOps;
import scala.collection.mutable.ArrayBuffer;
import scala.collection.mutable.ArrayOps;
import scala.collection.mutable.StringBuilder;
import scala.reflect.ClassTag.;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction1.mcVI.sp;
import scala.runtime.BoxedUnit;
import scala.runtime.BoxesRunTime;
import scala.runtime.IntRef;
import scala.runtime.ObjectRef;
import scala.runtime.RichInt.;
import scala.runtime.ScalaRunTime.;

public final class sparkToMysql$
{
  public static final  MODULE$;

  static
  {
    new ();
  }

  public void toMySQL(Iterator<String[]> iterator, Function0<String> tbName, Function0<String> schema)
  {
    Tuple2 localTuple22 = iterator.duplicate();
    Iterator iter1;
    Iterator iter2;
    final String valueParams;
    final int batchRows;
    final ObjectRef conn;
    final ObjectRef ps;
    if (localTuple22 != null) { Iterator iter1 = (Iterator)localTuple22._1(); Iterator iter2 = (Iterator)localTuple22._2(); Tuple2 localTuple23 = new Tuple2(iter1, iter2); Tuple2 localTuple21 = localTuple23; iter1 = (Iterator)localTuple21._1(); iter2 = (Iterator)localTuple21._2();
      String[] fields = getFieldArray(schema);
      valueParams = multiplyValue("?", ",", fields.length);
      String sql = new StringOps(Predef..MODULE$.augmentString("insert into %s (%s) ")).format(Predef..MODULE$.genericWrapArray(new Object[] { tbName.apply(), Predef..MODULE$.refArrayOps((Object[])fields).mkString(",") }));
      batchRows = 50;
      null; conn = new ObjectRef(null);
      null; ps = new ObjectRef(null);
      Predef..MODULE$.print(iter1); }
    try {
      Class.forName("com.mysql.jdbc.Driver");
      conn.elem = DriverManager.getConnection("jdbc:mysql://smzdm_recommend_mysql_m01_184:3306/recommendDB", "recommendUser", "pVhXTntx9ZG");

      int parRows = iter1.length();
      final int div = parRows / batchRows;
      final int rec = parRows % batchRows;
      if (div > 0) {
        String multiplyValueParams = multiplyValue(new StringOps(Predef..MODULE$.augmentString("(%s)")).format(Predef..MODULE$.genericWrapArray(new Object[] { valueParams })), ",", batchRows);
        String divSql = new StringBuilder().append("insert into ").append(tbName.apply()).append("(device_id,user_id,article_channel_name,level_type,level_id,level_num,level_standardization,article_channel_id) values ").append(multiplyValueParams).toString();

        ps.elem = ((Connection)conn.elem).prepareStatement(divSql);
      }
      else if ((div == 0) && (rec > 0)) {
        String recValueParams = multiplyValue(new StringOps(Predef..MODULE$.augmentString("(%s)")).format(Predef..MODULE$.genericWrapArray(new Object[] { valueParams })), ",", rec);
        String recSql = new StringBuilder().append("insert into ").append(tbName.apply()).append("(device_id,user_id,article_channel_name,level_type,level_id,level_num,level_standardization,article_channel_id) values ").append(recValueParams).toString();

        ps.elem = ((Connection)conn.elem).prepareStatement(recSql);
      }
      Iterator indexIter2 = iter2.zipWithIndex();

      final IntRef batch = new IntRef(1);
      final IntRef cur = new IntRef(0);
      indexIter2.foreach(new AbstractFunction1() { public static final long serialVersionUID = 0L;
        private final Function0 tbName$1;
        private final String valueParams$1;
        private final int batchRows$1;
        private final ObjectRef conn$1;
        private final ObjectRef ps$1;
        private final int div$1;
        private final int rec$1;
        private final IntRef batch$1;
        private final IntRef cur$1;

        public final Object apply(Tuple2<String[], Object> x0$1) { Tuple2 localTuple2 = x0$1; if (localTuple2 != null) { String[] d = (String[])localTuple2._1(); int index = localTuple2._2$mcI$sp();
            sparkToMysql..MODULE$.setRecommendData((PreparedStatement)ps.elem, d, cur.elem);
            cur.elem += d.length;
            if ((index == batch.elem * batchRows - 1) && (batch.elem <= div)) {
              ((PreparedStatement)ps.elem).executeUpdate();
              if ((batch.elem == div) && (rec > 0)) {
                String recValueParams = sparkToMysql..MODULE$.multiplyValue(new StringOps(Predef..MODULE$.augmentString("(%s)")).format(Predef..MODULE$.genericWrapArray(new Object[] { valueParams })), ",", rec);
                String recSql = new StringBuilder().append("insert into ").append(this.tbName$1.apply()).append("(device_id,user_id,article_channel_name,level_type,level_id,level_num,level_standardization,article_channel_id) values ").append(recValueParams).toString();

                ps.elem = ((Connection)conn.elem).prepareStatement(recSql);
              }
              batch.elem += 1;
              cur.elem = 0;
            }
            BoxedUnit localBoxedUnit =
              (batch.elem == div + 1) && (rec > 0) && (index == div * batchRows + rec - 1) ?
              BoxesRunTime.boxToInteger(((PreparedStatement)ps.elem).executeUpdate()) : BoxedUnit.UNIT;

            return localBoxedUnit; } throw new MatchError(localTuple2);
        }
      });
      if ((PreparedStatement)ps.elem != null) {
        ((PreparedStatement)ps.elem).close();
      }
      if ((Connection)conn.elem == null) return;
      ((Connection)conn.elem).close();
      return;
      throw new MatchError(localTuple22);
    }
    catch (Exception localException)
    {
      Predef..MODULE$.println(new Tuple2("Mysql Exception", localException));
    }
    finally
    {
      if ((PreparedStatement)ps.elem != null) {
        ((PreparedStatement)ps.elem).close();
      }
      if ((Connection)conn.elem != null)
        ((Connection)conn.elem).close();
    }
    if ((PreparedStatement)ps.elem != null) {
      ((PreparedStatement)ps.elem).close();
    }
    if ((Connection)conn.elem != null)
      ((Connection)conn.elem).close();
  }

  public String multiplyValue(String value, String sep, int count)
  {
    final ArrayBuffer resBuffer = new ArrayBuffer(); RichInt..MODULE$
      .to$extension0(Predef..MODULE$.intWrapper(1), count).foreach$mVc$sp(new AbstractFunction1.mcVI.sp() { public static final long serialVersionUID = 0L;
      private final String value$1;
      private final ArrayBuffer resBuffer$1;

      public final void apply(int i) { apply$mcVI$sp(i); }
      public void apply$mcVI$sp(int i) { resBuffer.append(Predef..MODULE$.wrapRefArray((Object[])new String[] { this.value$1 })); }

    });
    return resBuffer.mkString(sep);
  }
  public String[] getFieldArray(Function0<String> schemaStr) {
    String[] fields = (String[])Predef..MODULE$.refArrayOps((Object[])((String)schemaStr.apply()).split(",")).map(new AbstractFunction1() { public static final long serialVersionUID = 0L;

      public final String apply(String x) { String[] arrayOfString = x.trim().split(":"); Option localOption = Array..MODULE$.unapplySeq(arrayOfString); if ((!localOption.isEmpty()) && (localOption.get() != null) && (((SeqLike)localOption.get()).lengthCompare(2) == 0)) { String field = (String)((SeqLike)localOption.get()).apply(0); String str1 = field; String field = str1;
          return field.trim();
        }
        throw new MatchError(arrayOfString);
      }
    }
    , Array..MODULE$.canBuildFrom(ClassTag..MODULE$.apply(String.class)));

    return fields;
  }

  public void setRecommendData(PreparedStatement p, String[] row, int cur)
  {
    p.setString(cur + 1, row[0].replaceAll("\\s", ""));
    p.setString(cur + 2, row[1]);
    p.setString(cur + 3, row[2]);
    p.setInt(cur + 4, new StringOps(Predef..MODULE$.augmentString(row[3])).toInt());
    p.setInt(cur + 5, new StringOps(Predef..MODULE$.augmentString(row[4])).toInt());
    p.setInt(cur + 6, new StringOps(Predef..MODULE$.augmentString(row[5])).toInt());
    String str = " "; row[6]; if (str != null);
    new StringOps(Predef..MODULE$.augmentString(row[6]));
  }

  public void main(String[] args)
  {
    SparkConf conf = new SparkConf();
    SparkContext sc = new SparkContext(conf);
    String table_str = "device_id:String,user_id:String,article_channel_name:String,level_type:String,level_id:String,level_num:String,level_standardization:String,article_channel_id:String";

    RDD dataFromHDFS = sc.textFile(args[0], sc.textFile$default$2()).map(new AbstractFunction1() { public static final long serialVersionUID = 0L;

      public final String[] apply(String line) { return line.split("\001"); }

    }, ClassTag..MODULE$.apply(ScalaRunTime..MODULE$.arrayClass(String.class)));

dataFromHDFS.foreachPartition(
  new AbstractFunction1() { public static final long serialVersionUID = 0L;
  private final String[] args$1;

  public final void apply(Iterator<String[]> iter) { final String table_str = "device_id:String,user_id:String,article_channel_name:String,level_type:String,level_id:String,level_num:String,level_standardization:String,article_channel_id:String";
    final String tableName = this.args$1[1];
    sparkToMysql..MODULE$.toMySQL(iter, new AbstractFunction0() { public static final long serialVersionUID = 0L;
      private final String tableName$1;

      public final String apply() { return tableName; }

    }
    , new AbstractFunction0() { public static final long serialVersionUID = 0L;
      private final String table_str$1;

      public final String apply() { return table_str; }

    });
  }
});
Predef..MODULE$.println("5555555555555555555");
}

private sparkToMysql$()
{
MODULE$ = this;
}
}
