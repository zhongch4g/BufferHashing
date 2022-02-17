def get_throughput(path):
    w = 0
    r = 0
    with open (path, "r") as f:
        cnt = 0
        for line in f.readlines():
            if ("real_elapse" in line):
                cnt += 1
                if (cnt == 1):
                    data = line.strip().split(",")
                    operations = float(data[0].strip("[0m").split()[0])
                    real_elapse = float(data[1].strip().split()[0])
                    w = operations/real_elapse/1024/1024
                elif (cnt == 2):
                    data = line.strip().split(",")
                    operations = float(data[0].strip("[0m").split()[0])
                    real_elapse = float(data[1].strip().split()[0])
                    r = operations/real_elapse/1024/1024
    return w, r

write = []
read = []   
for idx in range(1, 41):
    path = "CCEH_buflog_Neg_th" + str(idx) + ".txt"
    w, r = get_throughput(path)
    write.append(w)
    read.append(r)

for i in write:
    print(i)
print('-------------')
for j in read:
    print(j)




