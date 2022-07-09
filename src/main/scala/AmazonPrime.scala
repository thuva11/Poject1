
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
    println("1 -Run Query : \n" +
      "2 - Update Admiin Password : " + "\n" +
      "3 - Logout : ")
    val admin_choice = readInt()
    if (admin_choice==2) {
      UpdatePassword(usName)
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
          println("welcome user! ");
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


  ////////////////////////////////////////////////////



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