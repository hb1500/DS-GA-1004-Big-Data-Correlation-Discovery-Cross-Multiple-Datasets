from collections import defaultdict
import pandas as pd
log2= lambda x:log(x,2)
from math import log

class Mutual_Info():

    def __init__(self, x, y, x_bin, y_bin, bins):
        self.x = x
        self.y = y
        self.x_bin = x_bin
        self.y_bin = y_bin
        self.bins = bins
        
    def mutual_information(self):
        if self.x_bin:
            x = pd.cut(self.x, self.bins, labels=False)
        if self.y_bin:
            y = pd.cut(self.y, self.bins,  labels=False)
        return entropy(self.y) - self.conditional_entropy()    

    def conditional_entropy(self):

        Py= self.compute_distribution(self.y)
        Px= self.compute_distribution(self.x)
        res= 0
        for ey in set(self.y):
            print(self.x, self.y)
            x1 = self.x[self.y==ey]
            print(x1)
            condPxy= self.compute_distribution(x1)

            for k, v in condPxy.items():
                res+= (v*Py[ey]*(log2(Px[k]) - log2(v*Py[ey])))
        return res

    def entropy(self):
        
        Py= self.compute_distribution(self.y)
        res=0.0
        for k, v in Py.items():
            res+=v*log2(v)
        return -res
   
    def compute_distribution(self, v):
        d= defaultdict(int)
        for e in v: d[e]+=1
        s= float(sum(d.values()))
        return dict((k, v/s) for k, v in d.items())  

### function used to calculate mutual information between any two features given a dataset.

def calu_MI(data,x_bin,y_bin, bins):
    df = pd.read_csv(data)
    # some datasets with dtype as objective, need to convert into numeric type
    df_num = df.convert_objects(convert_numeric=True)

    cc = {}     
    for s in df_num.columns:
        for t in df_num.columns:
            if (str(t)+ '>*<' + str(s)) in cc.keys() or t == s:
                continue
            else:
                key = str(s)+ '>*<' + str(t)
                df[s].values
                cc[key] = mutual_information(df_num[s], df_num[t], x_bin,y_bin, bins=bins)
            output = pd.Series(cc,index=cc.keys())
            # rank mutual information by its values
    return output.sort_values(ascending=False)

# output.to_csv('')


