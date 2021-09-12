for i in {1..16}
do
    # grep -i "  MB  " nometa-noleft-th$i.log >> nntest.log
    grep -i "# of key left : " nometa-noleft-th$i.log >> nntest.log
    # grep -i "real_elapsed" nometa-noleft-th$i.log >> nntest.log
    # echo "\n" >> nntest.log
done