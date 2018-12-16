#!/bin/bash

sed -i -e "s/greenlightning: //g" greenlightning.log
sed -i -e "s/Telemetry://g" greenlightning.log

grep \d*.dot greenlightning.log > fileNames.txt
blockEnd="GraphManager"

while read p; do

cat greenlightning.log | grep -A100000 $p | grep -B100000 -m1 $blockEnd > $p
sed -i '$ d' $p
sed -i '1,1d' $p

dot -Tsvg -o$p.svg $p
rm $p

done < fileNames.txt



