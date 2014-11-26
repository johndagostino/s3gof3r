mkdir /tmp/test
for i in $(seq 1 10); do dd if=/dev/zero of=/tmp/test/file$i bs=`expr 1024 \* 1000 \* 1024` count=1; done
./gof3r sync --path /tmp/test --bucket rto-inspireeducation --debug --no-md5 --prefix test-directory/
