#This script is used to down sampling to get a balance dataset.
#Sampled 100 times to bagging the results from models
import pandas as pd

#init train data
print('getting train data...')
#define iterator function
def num_gene():
    readers = pd.read_csv('data/train_numeric.csv', chunksize = 10000)
    for reader in readers:
        yield reader

def cate_gene():
    readers = pd.read_csv('data/train_categorical.csv', chunksize = 10000)
    for reader in readers:
        yield reader

def date_gene():
    readers = pd.read_csv('data/train_date.csv', chunksize = 10000)
    for reader in readers:
        yield reader
#init iterator
num_generator = num_gene()
cate_generator = cate_gene()
date_generator = date_gene()
#There are about 6800+ positive samples in dataset, 1183000+ samples. So, for every 10000 samples,
#we need sample about 60 negetive samples and combine the positive samples in dateset.

#for every chunk in data,
for k in range(0, 118):
    num_data = num_generator.__next__()
    cate_data = cate_generator.__next__()
    date_data = date_generator.__next__()
    data = num_data.merge(cate_data).merge(date_data)
    #sample 100 times
    for total_num in range(0, 100):
        #combine positive and negetive data
        print('this is ' + str(total_num) + ' samples and ' + str(k) + ' parts')
        t_data = data.sample(60)
        t_data = t_data.append(data[data['Response'] == 1])
        #save into .csv
        print('saving...')
        if k == 0:
            t_data.to_csv('samples/total' + str(total_num) + '.csv', index = False)
        else:
            t_data.to_csv('samples/total' + str(total_num) + '.csv', index = False, header = False, mode = 'a')