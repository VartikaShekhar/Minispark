import random
random.seed(1234)

granularity = 8

for j in range(1024//granularity):


    keys = [i for i in range(granularity*1024)]
    random.shuffle(keys)
    for k in range(granularity):
        f1 = open(f'./test_files/hiddenvals{j*granularity+ k}.txt', 'w')
        for i in range(1024):
            f1.write(f"{keys[k*1024+i]}\t{random.randrange(10000)}\n")
        f1.close()
