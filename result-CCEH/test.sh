for i in {1..16}
do
    grep -i "^*SUM*" WR-Only-th$i.log >> test.log
    echo "\n" >> test.log
done