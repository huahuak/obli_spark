export PATH=$PATH:../res/jdk1.8.0_351/bin
dep=""
for jar in `find ./dist -name '*.jar'`
do
  dep="${jar}:${dep}"
done
# echo ${dep}
java -cp ${dep} org.apache.spark.examples.sql.kaihua.MyTest
