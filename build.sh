export CLASSPATH=./bin:./libs/asm-all-4.1.jar:./libs/kilim.jar:$CLASSPATH 

rm -rf ./bin

mkdir ./bin

javac -Xlint:unchecked -g -d ./bin `find . -name *.java`


java -ea kilim.tools.Weaver -d ./bin -x "ExInvalid | test" ./bin
