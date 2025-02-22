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
	# ./dev/make-distribution.sh --name custom-spark -Dmaven.test.skip=true
	./dev/make-distribution.sh --name custom-spark

run:
	java -cp ${JARS_WITH_COLON} ${MAIN_CLASS}

.PHONY:	obliop
obliop:
	mvn install -pl obliop 
	# mvn package -pl obliop 
	# mv -f obliop/target/obliop-1.0-jar-with-dependencies.jar dist/jars/
	cp -f obliop/target/obliop-1.0.jar /home/huahua/Projects/optee/optee_rust/out/spark/dist/jars/obliop-1.0.jar
	mv -f obliop/target/obliop-1.0.jar dist/jars/

.PHONY: sql
sql:
	mvn package -pl sql/core  -Dmaven.test.skip=true
	# cp -f sql/core/target/spark-sql_2.12-3.3.1.jar /home/huahua/Projects/optee/optee_rust/out/spark/dist/jars/spark-sql_2.12-3.3.1.jar
	# mv -f sql/core/target/spark-sql_2.12-3.3.1.jar dist/jars/

.PHONY: examples
examples:
	mvn package -pl examples -Dmaven.test.skip=true 
	cp -f examples/target/original-spark-examples_2.12-3.3.1.jar /home/huahua/Projects/optee/optee_rust/out/spark/dist/jars/original-spark-examples_2.12-3.3.1.jar
	mv -f examples/target/original-spark-examples_2.12-3.3.1.jar dist/examples/jars/original-spark-examples_2.12-3.3.1.jar