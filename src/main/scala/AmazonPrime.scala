
import AmazonPrime.connection
import java.beans.Statement
import java.sql.DriverManager
import java.sql.Connection
import scala.io.StdIn.readLine
import scala.io.StdIn.readInt
import java.sql.PreparedStatement
import java.sql.ResultSet
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.security.MessageDigest

object AmazonPrime {

  var ses = true
  val driver = "com.mysql.cj.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/p1"
  val dbusername = "root"
  val dbpassword = "toor"
  var LoginStatus = false


  //val connection: Connection = null
  var session_user = "default"


  val connection:Connection = DriverManager.getConnection(url, dbusername, dbpassword)
  val statement = connection.createStatement()


  System.setProperty("hadoop.home.dir", "C:\\Hadoop") //spark session for windows
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("Sucessfully Created Spark Session")
  spark.sparkContext.setLogLevel("ERROR")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  def md5(text: String):String = {
    var value=MessageDigest.getInstance("MD5").digest(text.getBytes)
    var finalv = value.mkString("")
    return finalv
  }

  def AdminQuery(usname2: String)={


    while (ses == true) {
      println(Console.MAGENTA + "----------------------------------------------------------- " + Console.RESET);
      println(Console.YELLOW + "---------------------Select Query------------------------ " + Console.RESET);
      println(Console.MAGENTA + "----------------------------------------------------------- " + Console.RESET);
      println("1 - List Movies or TV Shows based on Released year and Type   ")
      println("2 - List Movies and TV Shows based on rating ? ")
      println("3 - How many released for Every Year ? ")
      println("4 - Percentage of releases based Country in Year ?")
      println("5 - List movies by Type , Genre , Year  ?")
      println("6 - Back to Admin Dashboard: ")
      println("Select between 1 - 5 to Query or Select 6 to Go Back to Admin Dashboard")
      val select = readInt()

      if (select == 1) {

        println("Which year you want to pull : ")
        val year = readLine()
        println("Select Type :  TV Show or Movie")
        val qurytype = readLine()
        val df = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/hive/CSVInput/ap.csv")
        df.createOrReplaceTempView("YearTvshow")
        spark.sql("SELECT ID, Title,Type, release_year, Genre FROM YearTvshow where release_year ='" + year + "' and Type ='" + qurytype + "' ;").show()

        println("Do you want to export as json file?\n 0 - No \n1 - Yes")
        val x = readInt()
        x match {
          case 0 => "Okay"
          case 1 => {
            spark.sql("SELECT ID, Title,Type, release_year, Genre FROM YearTvshow where release_year ='" + year + "' and Type = 'TV Show';").write.format("org.apache.spark.sql.json").mode("overwrite").save(s"hdfs://localhost:9000/user/hive/JSONOutput/$year-ReleseTvShow")
            println(s"Saved Successfully")
          }
          case _ => println("Invalid input")
        }
      }
      else if (select == 2) {
        val rate = 8
        val df1 = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/hive/CSVInput/ap.csv")
        df1.createOrReplaceTempView("RateView")
        spark.sql("SELECT ID, Title,Rating, Genre FROM RateView where Rating >'" + rate + "';").show()

        println("Do you want to export as json file?\n 0 - No \n1 - Yes")
        val x = readInt()
        x match {
          case 0 => "Okay"
          case 1 => {
            spark.sql("SELECT ID, Title,Rating, Genre FROM RateView where Rating >'" + rate + "';").write.format("org.apache.spark.sql.json").mode("overwrite").save(s"hdfs://localhost:9000/user/hive/JSONOutput/$rate-Rating")
            println(s"Saved Successfully")
          }
          case _ => println("Invalid input")
        }
      }
      else if (select == 3) {

        val df2 = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/hive/CSVInput/ap.csv")
        df2.createOrReplaceTempView("YearReleaseCount")
        spark.sql("SELECT release_year as ReleaseYear , count(*) as CountPerYear FROM YearReleaseCount GROUP BY release_year order by count(*) Desc  ;").show()

        println("Do you want to export as json file?\n 0 - No \n1 - Yes")
        val x = readInt()
        x match {
          case 0 => "Okay"
          case 1 => {
            spark.sql("SELECT release_year as ReleaseYear , count(*) as CountPerYear FROM YearReleaseCount GROUP BY release_year order by count(*) Desc  ;").write.format("org.apache.spark.sql.json").mode("overwrite").save(s"hdfs://localhost:9000/user/hive/JSONOutput/YearReleaseCount ")
            println(s"Saved Successfully")
          }
          case _ => println("Invalid input")
        }
      }

      else if (select == 4) {
        println("Load  Percentage of Releases Genre in Year. ")
        println("Enter year")
        val year = readInt()
        println("Type Genre -  (Eg -Drama , Comedy, Action etc)")
        val dbGenre = readLine()
        println("Loading percentage of "+Console.YELLOW + dbGenre + Console.RESET+" Genre Releases in "+Console.YELLOW +year+ " ..........."+ Console.RESET)
        val df3 = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/hive/CSVInput/ap.csv")
        df3.createOrReplaceTempView("PercentageByCountry")


        spark.sql(s"select round( ( ( (select count(Title) as `Count` from PercentageByCountry where release_year = $year AND Genre LIKE '%" + dbGenre + "%' )/(SELECT count(*) FROM PercentageByCountry where Genre Like '%" + dbGenre + "%')) * 100),2) as `Percentage` ").show()

      }

      else if (select == 5) {
        println("Enter release year")
        val year = readInt()
        println("Which Genre -  (Eg -Drama , Comedy, Action) ")
        val dbGenre = readLine()
        println("What type  -  (Type either one -  TV Show or Movie) ")
        val mtype = readLine()
        println("")
        val df4 = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/hive/CSVInput/ap.csv")
        df4.createOrReplaceTempView("threeQuery")
        spark.sql("SELECT Title, Director,Type,Cast, release_year, Genre  FROM threeQuery where Type  ='" + mtype + "' AND release_year ='" + year + "' AND Genre = '" + dbGenre + "';").show()
      }
      else if (select == 6)
      {
        Admin(usname2)
      }

    }

    }

  def UpdatePassword(usname: String): Unit = {
    println("Enter Current password:")
    val CurPassword = readLine
    val hashedCurrpPassword = md5(CurPassword)
    println("Enter new password:")
    val newPassword = readLine
    val hashedNewPw = md5(newPassword)
    var sql = "SELECT  password from user WHERE username ='" + usname + "'"
    var rs = statement.executeQuery(sql)

    while (rs.next()) {
      val pwFromDb = rs.getString("password")

      if (hashedCurrpPassword.equals(pwFromDb)) {
        val st2 = connection.createStatement()
        var updatesql = "UPDATE user SET password = '" + hashedNewPw + "' WHERE username = '" + usname + "'"
        st2.executeUpdate(updatesql)
      }
   else {
        println("Incorrect Password")

      }
    }
  }
  def Admin(usName:String) {
    println("Select options : ")
    println("1 - Run Query : \n" +
      "2 - Update Admiin Password : " + "\n" +
      "3 - Logout : ")
    val admin_choice = readInt()
    if (admin_choice==2) {
      UpdatePassword(usName)
    }
    else if (admin_choice==1)
      {
        AdminQuery(usName)
      }
  }
  def User(usName:String) {

    println("Select an option....")
    println("1 - Search by title")
    println("2 - Search by director")
    println("3 - Change Password")
    println("4 - Logout")
    println("Enter your choice:")

    val user_Choice = readInt()
    if (user_Choice==2) {
      UpdatePassword(usName)
    }
    else if (user_Choice==1)
    {
      AdminQuery(usName)
    }
  }

  def CreateUser(options: Int) = {
    //Make a new user based on user input
    println("Enter Name : ")
    val name = readLine()
    println("Enter User Name : ")
    val uname = readLine()
    println("Enter password : ")
    val pword = readLine()
    val pwhashed=md5(pword)
    (name, uname, pwhashed)
  }

  /////////////////////////////////


  //---- user ------------------------------------------------------------//

  def Userlogin(){

    println("Enter username : ")
    val username1 = readLine()
    println("Enter password : ")
    val password1 = readLine()
    val userpwhash=md5(password1)

    try {
      var sql = "SELECT COUNT(username) AS count, username, password, type , name from user WHERE username ='"+username1 + "' AND  password ='"+userpwhash + "'"
      var resultSet = statement.executeQuery(sql)

      while (resultSet.next()) {
        val uname = resultSet.getString("username")
        val userpw = resultSet.getString("password")

        val typeofuser=resultSet.getString("type")
        val namefromdb=resultSet.getString("name")
        //val countCol=resultSet.getInt("count")


        if (username1.equals(uname) && userpwhash.equals(userpw) && typeofuser.equals("User") ) {
          println("Successful Login! " + "Welcome " + namefromdb);
          println("********************************************************************************")
          println("User Home")
          println("********************************************************************************")

          User(uname)
        }
        else if(username1.equals(uname) && userpwhash.equals(userpw) && typeofuser.equals("Admin"))
        {
          println("Successful Login! " + Console.YELLOW + "Welcome Admin " + namefromdb + Console.RESET);
          println("====================================")
          println("Admin Dashboard")
          println("====================================")
          Admin(uname)
        }

        else
        {
          println("Incorrect Credentials.")
        }
      }
    }
    catch {
      case e: Throwable => e.printStackTrace
    }


  }
  ///////////////////////////*******************************************************************************///////////////
  def main(args: Array[String]) {

    println(md5("abc"))
    println()
    println(Console.GREEN +"=======================================")
    println("Welcome to Amazon Prime Movies Dataset")
    println("======================================"+ Console.RESET)
    println(Console.YELLOW + "If you are Admin Please Login with your Credential" + Console.RESET)
    println("1 - Login : ")
    println("2 - Signup " +Console.YELLOW + "(Only Users Can Signup) : " +Console.RESET)
    println("0 - Exit")
    val userinput = readInt

    try {
      val statement = connection.createStatement()

      if (userinput ==1) {
        Userlogin()
      }

      else if (userinput == 2) {
        val (f, u, p) = CreateUser(userinput)
        val insertsql = s"insert into user (name,type, username, password) values (?,'User',?,?)"
        val preparedStmt: PreparedStatement = connection.prepareStatement(insertsql)
        preparedStmt.setString(1, f)
        preparedStmt.setString(2, u)
        preparedStmt.setString(3, p)
        preparedStmt.execute
        preparedStmt.close
        println("User Created Successfully")

      }


    } catch {
      case e: Throwable => e.printStackTrace
    }

  }

}