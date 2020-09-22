WORKROOT=$(pwd)
cd ${WORKROOT}

export GOPATH=$(pwd)

go build
if [ $? -ne 0 ];
then
	echo "fail to build data-o"
	exit 1
fi

if [ -d "./output" ]
then
	rm -rf output
fi

mkdir output
mv s/main "output/datao"
chmod +x output/datao 

cp output/datao ./test/

echo "OK for build datao"
