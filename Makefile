# /**
#  * @author kahua.li
#  * @email moflowerlkh@gmail.com
#  * @date 2022/12/30
#  **/

# // ------------------------------------ //
SPACE := $(EMPTY) $(EMPTY)

# // ------------------ JAVA ------------------ //
JARS = $(shell find ./dist -name "*.jar")
JARS_WITH_COLON = $(subst ${SPACE},,$(foreach jar,${JARS},${jar}:))
MAIN_CLASS = org.apache.spark.examples.sql.kaihua.MyTest
# MAIN_CLASS = org.apache.spark.examples.sql.SparkSQLExample


all:
	export MAVEN_OPTS="-Xss128m -Xmx6g -XX:ReservedCodeCacheSize=1g"
	./dev/make-distribution.sh --name custom-spark

run:
	java -cp ${JARS_WITH_COLON} ${MAIN_CLASS}

.PHONY:	obliop
obliop:
	mvn package -pl obliop 
	# mv -f obliop/target/obliop-1.0-jar-with-dependencies.jar dist/jars/
	mv -f obliop/target/obliop-1.0.jar dist/jars/

sql-core:
	mvn package -pl sql/core -DskipTests
	mv -f sql/core/target/spark-sql_2.12-3.3.1.jar dist/jars/

.PHONY: examples
examples:
	mvn package -pl examples
	mv -f examples/target/original-spark-examples_2.12-3.3.1.jar dist/examples/jars/original-spark-examples_2.12-3.3.1.jar