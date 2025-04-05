export PATH=$PATH:./res/jdk1.8.0_351/bin
dep=""
for jar in `ls ./java`
do
  dep="./java/${jar}:${dep}"
done
# echo ${dep}
java -cp ${dep} org.kaihua.obliop.Main
