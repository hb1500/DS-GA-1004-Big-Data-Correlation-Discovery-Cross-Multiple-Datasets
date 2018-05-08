import pandas as pd
import numpy as np
log2= lambda x:log(x,2)
from collections import defaultdict
from math import log

def mutual_information(x, y, x_bin ,y_bin, bins=10):
    if x_bin:
        x = pd.cut(x, bins=bins, labels=False)
    if y_bin:
        y = pd.cut(y, bins=bins,  labels=False)
    return entropy(y) - conditional_entropy(x,y)

def conditional_entropy(x, y):
    """
    x: a list of number 
    y: a list of number 
    """
    Py= compute_distribution(y)
    Px= compute_distribution(x)

    res= 0
    for ey in set(y):

        x1= x[y==ey]
        condPxy= compute_distribution(x1)

        for k, v in condPxy.items():
            res+= (v*Py[ey]*(log2(Px[k]) - log2(v*Py[ey])))
    return res
        
def entropy(y):

    Py= compute_distribution(y)
    res=0.0
    for k, v in Py.items():
        res+=v*log2(v)
    return -res

def compute_distribution(v):

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
                cc[key] = Mutual_Info.mutual_information(df_num[s], df_num[t], x_bin,y_bin, bins=bins)
            output = pd.Series(cc,index=cc.keys())
            # rank mutual information by its values
    return output.sort_values(ascending=False)

# output.to_csv('')


