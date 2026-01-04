import random

random.seed(1234)

class RDD:
    def __init__(self, trans, backed, numpartitions = 8):
        self.backed = backed
        self.numpartitions = numpartitions
        self.trans = trans
        if trans == 0:    
            self.dep0 = None
            self.dep1 = None
        elif trans == 2:
            
            self.dep0 = RDD(3, 0, numpartitions)
            self.dep1 = RDD(3, 0, numpartitions)
        elif trans == 3:
            self.dep0 = RDD(0, random.randint(0,1023), random.randint(1,128))
        else:
            self.dep0 = RDD(0, random.randint(0,1023), numpartitions)
            
    def push(self, rdd):
        if self.trans == 2:
            direction = random.randint(0,1)
            if direction == 0:
                if self.dep0.trans != 0:
                    self.dep0.push(rdd)
                else:
                    self.dep0 = rdd
            else:
                if self.dep1.trans != 0:
                    self.dep1.push(rdd)
                else:
                    self.dep1 = rdd
        elif self.trans != 0:
            if self.trans != 3:
                rdd.numpartitions = self.numpartitions
            if self.dep0.trans != 0:
                self.dep0.push(rdd)
            else:
                self.dep0 = rdd
        else:
            return
    def __str__(self):
        if self.trans == 2:
            return f"2 {self.dep0}{self.dep1}"
        elif self.trans == 3:
            return f"3 {self.numpartitions} {self.dep0}" 
        elif self.trans == 0:
            return f"0 {self.backed} {self.numpartitions} "
        else:
            return f"{self.trans} {self.dep0}"
        
r = RDD(2, 0, 8)
for i in range(3000):
    r.push(RDD(random.randint(0,3), random.randint(0,1023), random.randint(1,64)))
print(r)