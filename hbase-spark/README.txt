Running Tests under hbase-spark module
Tests are run via ScalaTest Maven Plugin and Surefire Maven Plugin
The following are examples to run the tests:

Run tests under root dir or hbase-spark dir
 $ mvn test                  //run all small and medium java tests, and all scala tests
 $ mvn test -PskipSparkTests //skip all scale and java test in hbase-spark
 $ mvn test -P runAllTests   //run all tests, including scala and all java test including the large test

Run specified test case
  $ mvn test -Dtest=TestJavaHBaseContext -DwildcardSuites=None                        //java unit test
  $ mvn test -Dtest=None -DwildcardSuites=org.apache.hadoop.hbase.spark.BulkLoadSuite //scala unit test
