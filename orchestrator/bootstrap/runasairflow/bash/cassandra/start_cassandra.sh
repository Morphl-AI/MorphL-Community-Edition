cassandra &>/dev/null
while true
do
  sleep 1
  netstat -lntp 2>/dev/null | grep 9042.*java > /dev/null && break
done
sleep 1
