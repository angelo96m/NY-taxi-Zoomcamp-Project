import sys 
import pandas as pd 

print("arguments: ", sys.argv)

month = int(sys.argv[1]) #0= pipeline, 1= primo valore passato 

print(f"hello pipeline, we are in month = {month}")
#####

df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})

print(df.head())
#create parquet file
df.to_parquet(f"output_month_{sys.argv[1]}.parquet")
