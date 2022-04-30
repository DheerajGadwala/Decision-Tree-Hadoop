import pandas as pd
import numpy as np
import sys
import random

def main():

	random.seed(27)

	file_name = sys.argv[1]
	
	cols = pd.read_csv(file_name, nrows=1, sep=",").columns
	cols_list = cols.tolist()
	print(cols_list)



	df = pd.read_csv(file_name, sep=',',index_col=0)
	mask = np.random.rand(len(df)) <= 0.80

	train = df[mask]
	test = df[~mask]

	print (len(test))
	print (len(train))

	train.to_csv('train.csv', index=False, sep=",", header=None)
	test.to_csv('test.csv',index=False, sep=",", header=None)

	#del test['class']
	test.to_csv('test_copy.csv',index=False, sep=",", header=None)


	

if __name__ == "__main__":
	main()