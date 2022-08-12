package edu.uob;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertTrue;

// PLEASE READ:
// The tests in this file will fail by default for a template skeleton, your job is to pass them
// and maybe write some more, read up on how to write tests at
// https://junit.org/junit5/docs/current/user-guide/#writing-tests
final class DBTests {

  private DBServer server;

  // we make a new server for every @Test (i.e. this method runs before every @Test test case)
  @BeforeEach
  void setup(@TempDir File dbDir) {
    // Notice the @TempDir annotation, this instructs JUnit to create a new temp directory somewhere
    // and proceeds to *delete* that directory when the test finishes.
    // You can read the specifics of this at
    // https://junit.org/junit5/docs/5.4.2/api/org/junit/jupiter/api/io/TempDir.html

    // If you want to inspect the content of the directory during/after a test run for debugging,
    // simply replace `dbDir` here with your own File instance that points to somewhere you know.
    // IMPORTANT: If you do this, make sure you rerun the tests using `dbDir` again to make sure it
    // still works and keep it that way for the submission.

    server = new DBServer(dbDir);
  }

  // Here's a basic test for spawning a new server and sending an invalid command,
  // the spec dictates that the server respond with something that starts with `[ERROR]`

  @Test
  void testInvalidCommandIsAnError() {
    assertTrue(server.handleCommand("foo").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("SELECT * FROM marks pass == TRUE;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("SELECT * FROM marks").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("SELECT  FROM marks").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("SELECT  FROM marks;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand(" name FROM marks WHERE mark>60;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand(" JOIN coursework AND marks ON grade AN i;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand(" drop name;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand(" drop tables name;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand(" alter table name;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand(" delete table name;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand(" create table ;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand(" create data ;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("use data ;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand(" use;").startsWith("[ERROR]"));
  }



  @Test
  void testCorrectCommandIsAnOk(){
    assertTrue(server.handleCommand("CREATE DATABASE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("USE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("CREATE TABLE marks (name, mark, pass);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Steve', 65, TRUE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Dave', 55, TRUE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Bob', 35, FALSE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Clive', 20, FALSE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT * FROM marks WHERE pass == TRUE;").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT * FROM marks WHERE (pass == FALSE) AND (mark > 35);").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT * FROM marks WHERE name != 'Dave';").startsWith("[OK]"));
    assertTrue(server.handleCommand("UPDATE marks SET mark = 38 WHERE name == 'Clive';").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT * FROM marks WHERE name == 'Clive';").startsWith("[OK]"));
    assertTrue(server.handleCommand("DELETE FROM marks WHERE name == 'Dave';").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT * FROM marks WHERE (pass == FALSE) AND (mark > 35);").startsWith("[OK]"));
    assertTrue(server.handleCommand("CREATE TABLE coursework (task,grade);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO coursework VALUES ('OXO',3);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO coursework VALUES ('DB', 1);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO coursework VALUES ('OXO',4);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO coursework VALUES ('STAG',2);").startsWith("[OK]"));
    assertTrue(server.handleCommand("JOIN coursework AND marks ON grade AND id;").startsWith("[OK]"));
    assertTrue(server.handleCommand("UPDATE marks SET mark = 38 WHERE name == 'Clive';").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT * FROM marks WHERE name == 'Clive';").startsWith("[OK]"));
    assertTrue(server.handleCommand("DELETE FROM marks WHERE name == 'Dave';").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT * FROM marks WHERE (pass == FALSE) AND (mark > 35);").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT * FROM marks WHERE name LIKE 've';").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT id FROM marks WHERE pass == FALSE;").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT name FROM marks WHERE mark>60;").startsWith("[OK]"));
    assertTrue(server.handleCommand("DELETE FROM marks WHERE mark<40;").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT * FROM marks;").startsWith("[OK]"));
    assertTrue(server.handleCommand("USE markbook;").startsWith("[OK]"));
  }




  @Test
  void testCreate(){
    //use 'CREATE TABLE' under database that doesn't exist
    assertTrue(server.handleCommand("create table name;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("Create table 123;").startsWith("[ERROR]"));
    //Use a database that does not exist
    assertTrue(server.handleCommand("USE markbook;").startsWith("[ERROR]"));
    //USE database after 'CREATE DATABASE'
    assertTrue(server.handleCommand("CREATE DATABASE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("USE markbook;").startsWith("[OK]"));
    //CREATE TABLE after 'USE DATABASE'
    assertTrue(server.handleCommand("USE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("CREATE table 123;").startsWith("[OK]"));
    //CREATE table/database that already exist
    assertTrue(server.handleCommand("CREATE table 123;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("CREATE Database markbook;").startsWith("[ERROR]"));
    //CREATE TABLE with valid attributes name
    assertTrue(server.handleCommand("CREATE table 1234(a,12,90,true,null,false);").startsWith("[OK]"));
    assertTrue(server.handleCommand("CREATE Database markbook;").startsWith("[ERROR]"));

  }



  @Test
  void testInsert(){
    //insert error test
    //insert under an undefined database and unknown table
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Steve', 65, TRUE;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("INSERT  marks VALUES ('Dave', 55, TRUE);").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("INSERT INTO marks  ('Bob', 35, FALSE);").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES 'Clive', 20, FALSE);").startsWith("[ERROR]"));
    //insert with invalid  attributes
    assertTrue(server.handleCommand("CREATE DATABASE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("USE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("CREATE TABLE marks (name, mark, pass);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Steve', TRUE);").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Dave, 55, TRUE);").startsWith("[ERROR]"));
    //integer overflow error
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Bob', 3500000000000000, FALSE);").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Bob', -3500000000000000, FALSE);").startsWith("[ERROR]"));


    //insert ok test
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Steve', 65, TRUE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Dave', 55, TRUE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Bob', 35, FALSE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Clive', 20, FALSE);").startsWith("[OK]"));
  }



  @Test
  void testAlter(){
    //alter error
    //1. alter under undefined database/name
    assertTrue(server.handleCommand("alter table 123 drop name;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("alter table 1 drop name;").startsWith("[ERROR]"));

    //alter ok
    assertTrue(server.handleCommand("CREATE DATABASE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("USE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("CREATE TABLE marks (name, mark, pass);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Steve', 65, TRUE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("alter table marks add task;").startsWith("[OK]"));
    assertTrue(server.handleCommand("alter table marks add age;").startsWith("[OK]"));
    assertTrue(server.handleCommand("alter table marks drop task;").startsWith("[OK]"));
    assertTrue(server.handleCommand("alter table marks drop age;").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT name FROM marks;").startsWith("[OK]"));

    //alter error
    assertTrue(server.handleCommand("alter table marks add name;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("alter tab marks add task;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("alter table mar add task;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("alter table marks  task;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("alter table marks drop task;").startsWith("[ERROR]"));
  }



  @Test
  void testSelect(){
    //select error
    assertTrue(server.handleCommand("SELECT name FROM marks WHERE mark>60;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("SELECT name FROM marks mark>60;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("SELECT FROM marks WHERE mark>60;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("SELECT name FROM marks ;").startsWith("[ERROR]"));

    //select ok
    assertTrue(server.handleCommand("CREATE DATABASE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("USE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("CREATE TABLE marks (name, mark, pass);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Steve', 65, TRUE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT  into marks VALUES ('Dave', 55, TRUE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks  values ('Bob', 35, FALSE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Clive', 20, FALSE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT * FROM marks;").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT * FROM marks WHERE (pass == FALSE) AND (mark > 35);").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT * FROM marks WHERE name LIKE 'S';").startsWith("[OK]"));
    assertTrue(server.handleCommand("SELECT id FROM marks WHERE pass == FALSE;").startsWith("[OK]"));

    //select error
    assertTrue(server.handleCommand("SELECT ok FROM marks;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("SELECT * FROM marks WHERE (pass = FALSE) AND (mark > 35);").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("SELECT * FROM marks WHERE OK LIKE 'S';").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("SELECT id FROM marks WHERE pass == OPH;").startsWith("[ERROR]"));
  }



  @Test
  void testJoin(){
    //join ok
    assertTrue(server.handleCommand("CREATE DATABASE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("USE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("CREATE TABLE marks (name, mark, pass);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Steve', 65, TRUE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT  into marks VALUES ('Dave', 55, TRUE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks  values ('Bob', 35, FALSE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Clive', 20, FALSE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("create table coursework(task, grade);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO coursework VALUES ('OXO', 3);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO coursework VALUES ('DB', 1);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO coursework VALUES ('OXO', 4);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO coursework VALUES ('STAG', 2);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks  values ('Bob', 35, FALSE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("JOIN coursework AND marks ON grade AND id;").startsWith("[OK]"));

    //join error
    assertTrue(server.handleCommand("JOIN course AND marks ON grade AND id;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("JOIN coursework AND mark ON grade AND id;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("JOIN coursework AND marks ON g AND id;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("JOIN coursework AND marks ON grade AND d;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("JOIN marks AND coursework ON grade AND id;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("JOIN coursework ON grade AND id;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("JOIN coursework AND marks ON  id;").startsWith("[ERROR]"));
  }



  @Test
  void testUpdate(){
    //update ok
    assertTrue(server.handleCommand("CREATE DATABASE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("USE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("CREATE TABLE marks (name, mark, pass);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Steve', 65, TRUE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT  into marks VALUES ('Dave', 55, TRUE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks  values ('Bob', 35, FALSE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Clive', 20, FALSE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("UPDATE marks SET mark = 38 WHERE name == 'Clive';").startsWith("[OK]"));
    assertTrue(server.handleCommand("UPDATE marks SET mark = 38 WHERE name like 'v';").startsWith("[OK]"));
    assertTrue(server.handleCommand("UPDATE marks SET pass=true WHERE name == 'Clive';").startsWith("[OK]"));
    assertTrue(server.handleCommand("UPDATE marks SET mark = 3800 WHERE name != 'Clive';").startsWith("[OK]"));

    //update error
    assertTrue(server.handleCommand("UPDATE marks SET mark = 3800 WHERE name > 'Clive';").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("UPDATE marks SET mark = 3800 WHERE name < 'Clive';").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("UPDATE marks SET mark = 3800 WHERE name <= 'Clive';").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("UPDATE marks SET mark = 3800 WHERE name >= 'Clive';").startsWith("[ERROR]"));
  }



  @Test
  void testDelete(){
    //DELETE ok
    assertTrue(server.handleCommand("CREATE DATABASE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("USE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("CREATE TABLE marks (name, mark, pass);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Steve', 65, TRUE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT  into marks VALUES ('Dave', 55, TRUE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks  values ('Bob', 35, FALSE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("INSERT INTO marks VALUES ('Clive', 20, FALSE);").startsWith("[OK]"));
    assertTrue(server.handleCommand("DELETE FROM marks WHERE name == 'Dave';").startsWith("[OK]"));
    assertTrue(server.handleCommand("DELETE FROM marks WHERE (mark<40) and (mark>30);").startsWith("[OK]"));

    //DELETE error
    assertTrue(server.handleCommand("DELETE FROM marks WHERE mark<40 and mark>30;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("DELETE FROM marks WHERE OK<40 and ERROR>30;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("DELETE FROM marks WHERE mark<40 OR0 mark>30;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("DELETE FROM marks WHERE markS<40 and markS>30;").startsWith("[ERROR]"));
  }



  @Test
  void testDrop(){
    //drop ok
    assertTrue(server.handleCommand("CREATE DATABASE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("USE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("CREATE TABLE marks (name, mark, pass);").startsWith("[OK]"));
    assertTrue(server.handleCommand("DROP DATABASE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("USE markbook;").startsWith("[ERROR]"));
    //OK
    assertTrue(server.handleCommand("CREATE DATABASE markbook;").startsWith("[OK]"));
    assertTrue(server.handleCommand("CREATE TABLE OK (name, mark, pass);").startsWith("[OK]"));
    assertTrue(server.handleCommand("DROP TABLE ok;").startsWith("[OK]"));

    //drop error
    assertTrue(server.handleCommand("DROP TABLE a;").startsWith("[ERROR]"));
    assertTrue(server.handleCommand("DROP DATABASE ok;").startsWith("[ERROR]"));
  }


  // Add more unit tests or integration tests here.
  // Unit tests would test individual methods or classes whereas integration tests are geared
  // towards a specific usecase (i.e. creating a table and inserting rows and asserting whether the
  // rows are actually inserted)

}
