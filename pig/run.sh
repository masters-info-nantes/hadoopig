#!/bin/bash
rm -rf out
$PIG_ROOT/bin/pig -x local -p INPUT=../input/input.txt -p OUTPUT=out/iter1 pagerank.pl
for i in {1..4}
do
    let next=$i+1
    $PIG_ROOT/bin/pig -x local -p INPUT=out/iter$i/part-r-00000 -p OUTPUT=out/iter$next pagerank.pl
done
$PIG_ROOT/bin/pig -x local -p INPUT=out/iter5/part-r-00000  -p OUTPUT=out/result outpagerank.pl