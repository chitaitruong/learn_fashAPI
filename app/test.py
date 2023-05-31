import pandas as pd
list1 = [10,20,30,40]
list2 = [40,30,20,10]
col1 = "X"
col2 = "Y"
data = pd.DataFrame({col1:list1,col2:list2})
data.to_excel('sample_data.xlsx', sheet_name='sheet1', index=False)