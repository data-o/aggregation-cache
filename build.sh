WORKROOT=$(pwd)
cd ${WORKROOT}

export GOPATH=$(pwd)

cd $WORKROOT/src/main

tags=$1

go build
if [ $? -ne 0 ];
then
	echo "fail to build data-o"
	exit 1
fi

cd ../../
if [ -d "./output" ]
then
	rm -rf output
fi

mkdir output
mv src/main/main "output/datao"
chmod +x output/datao 

cp output/datao ./test/

echo "OK for build datao"
