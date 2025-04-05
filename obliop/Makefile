# // ------------------ shell ------------------ //
SHELL = /usr/bin/zsh
COLOR=\033[0;33m
NC=\033[0m # No Color

# // ------------------ java ------------------ //
OPTEE = /home/huahua/Projects/optee
OPTEE_RUST = ${OPTEE}/optee_rust
OPTEE_SPARK = ${OPTEE_RUST}/examples/obli/obliop
OPTEE_CJNI = ${OPTEE_SPARK}/src/main/java/org/kaihua/obliop/interfaces/cjni

OUT_PATH = ${OPTEE_RUST}/out
JAVA_OUT_PATH = ${OUT_PATH}/java

mkdir:
	@if [ ! -d "${OUT_PATH}" ]; then\
		mkdir ${OUT_PATH};\
	fi
	@if [ ! -d "${JAVA_OUT_PATH}" ]; then\
		mkdir ${JAVA_OUT_PATH};\
	fi

all: dep java
	@echo "${COLOR}\n// ------------------ END JAVA ------------------ //\n${NC}"

java: mkdir
# copy java module interface dependency jars
	@echo "${COLOR}\n// ------------------ BEGIN JAVA ------------------ //\n${NC}"
	@mvn package
	# cp -f ./target/obliop-1.0-jar-with-dependencies.jar ${JAVA_OUT_PATH}
	cp -f ./target/obliop-1.0.jar ${JAVA_OUT_PATH}
# copy start shell script
	cp -f ${OPTEE_SPARK}/optee.sh ${OUT_PATH}
# c-JNI make
	@make -C ${OPTEE_CJNI}
	@cp ${OPTEE_CJNI}/libcjni.so ${JAVA_OUT_PATH}

dep: mkdir
	@echo "${COLOR}\n// ------------------ BEGIN DEP ------------------ //\n${NC}"
	mvn dependency:copy-dependencies
	@cp -f ./target/dependency/* ${JAVA_OUT_PATH}

clean: 
	@echo "${COLOR}\n// ------------------ BEGIN CLEAN ------------------ //\n${NC}"
	mvn clean