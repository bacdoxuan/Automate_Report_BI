🚀 DATA PROCESSOR - Testing Module

============================================================
📥 IMPORTING 3G DATA FILES
============================================================
📊 Importing: Automate_3G_Throughput.xlsx
   📄 Sheet: User_TP_DL
c:\Bac.DX\Python\virtual_env\Lib\site-packages\openpyxl\styles\stylesheet.py:237: UserWarning: Workbook contains no default style, apply openpyxl's default
  warn("Workbook contains no default style, apply openpyxl's default")
   ✅ Loaded 8,590 rows × 28 columns

📊 Importing: Automate_3G_Traffic_User.xlsx
   📄 Sheet: Voice_Erlang
c:\Bac.DX\Python\virtual_env\Lib\site-packages\openpyxl\styles\stylesheet.py:237: UserWarning: Workbook contains no default style, apply openpyxl's default
  warn("Workbook contains no default style, apply openpyxl's default")
   ✅ Loaded 9,275 rows × 28 columns
   📄 Sheet: Data_MB
c:\Bac.DX\Python\virtual_env\Lib\site-packages\openpyxl\styles\stylesheet.py:237: UserWarning: Workbook contains no default style, apply openpyxl's default
  warn("Workbook contains no default style, apply openpyxl's default")
   ✅ Loaded 9,275 rows × 28 columns
   📄 Sheet: HS_User
c:\Bac.DX\Python\virtual_env\Lib\site-packages\openpyxl\styles\stylesheet.py:237: UserWarning: Workbook contains no default style, apply openpyxl's default
  warn("Workbook contains no default style, apply openpyxl's default")
   ✅ Loaded 9,275 rows × 28 columns

============================================================
✅ Successfully loaded 4 datasets
============================================================

📋 Summary:
   • User_TP_DL: 8,590 rows × 28 columns
   • Voice_Erlang: 9,275 rows × 28 columns
   • Data_MB: 9,275 rows × 28 columns
   • HS_User: 9,275 rows × 28 columns

============================================================
📋 DETAILED DATAFRAME INFORMATION
============================================================

============================================================
📊 DataFrame: User_TP_DL
============================================================
Shape: 8,590 rows × 28 columns

Columns: ['Date', 'RNC Id', 'RBS Id', 'Ucell Id', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23']

Data types:
Date        datetime64[ns]
RNC Id              object
RBS Id              object
Ucell Id            object
0                  float64
1                  float64
2                  float64
3                  float64
4                  float64
5                  float64
6                  float64
7                  float64
8                  float64
9                  float64
10                 float64
11                 float64
12                 float64
13                 float64
14                 float64
15                 float64
16                 float64
17                 float64
18                 float64
19                 float64
20                 float64
21                 float64
22                 float64
23                 float64
dtype: object

First 5 rows:
        Date  RNC Id   RBS Id  ...           21           22           23
0 2025-11-26  HLRE01  U106038  ...  1872.942876  2868.797507  2803.545344
1 2025-11-26  HLRE01  U106038  ...  4456.609071  3873.530502  5310.632097
2 2025-11-26  HLRE01  U106038  ...  2671.690027  2277.089244  2104.200916
3 2025-11-26  HLRE01  U124002  ...  2413.967394  2349.992599  2573.323716
4 2025-11-26  HLRE01  U124002  ...  1535.758946  1611.755940  1689.204830

[5 rows x 28 columns]

Memory usage: 3.24 MB

============================================================
📊 DataFrame: Voice_Erlang
============================================================
Shape: 9,275 rows × 28 columns

Columns: ['Date', 'RNC Id', 'RBS Id', 'UCell Id', 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]

Data types:
Date        datetime64[ns]
RNC Id              object
RBS Id              object
UCell Id            object
0                  float64
1                  float64
2                  float64
3                  float64
4                  float64
5                  float64
6                  float64
7                  float64
8                  float64
9                  float64
10                 float64
11                 float64
12                 float64
13                 float64
14                 float64
15                 float64
16                 float64
17                 float64
18                 float64
19                 float64
20                 float64
21                 float64
22                 float64
23                 float64
dtype: object

First 5 rows:
        Date  RNC Id   RBS Id     UCell Id  ...        20        21        22        23 
0 2025-11-26  HLRE01   33333A       33333A  ...  0.000000  0.000000  0.000000  0.000000 
1 2025-11-26  HLRE01  U106038  U106038A10M  ...  0.051389  0.000000  0.005556  0.000000 
2 2025-11-26  HLRE01  U106038  U106038A20M  ...  0.025000  0.002778  0.000000  0.000000 
3 2025-11-26  HLRE01  U106038  U106038A30M  ...  0.059722  0.073611  0.000000  0.004167 
4 2025-11-26  HLRE01  U124002  U124002A10M  ...  0.040278  0.006944  0.000000  0.000000 

[5 rows x 28 columns]

Memory usage: 3.49 MB

============================================================
📊 DataFrame: Data_MB
============================================================
Shape: 9,275 rows × 28 columns

Columns: ['Date', 'RNC Id', 'RBS Id', 'UCell Id', 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]

Data types:
Date        datetime64[ns]
RNC Id              object
RBS Id              object
UCell Id            object
0                  float64
1                  float64
2                  float64
3                  float64
4                  float64
5                  float64
6                  float64
7                  float64
8                  float64
9                  float64
10                 float64
11                 float64
12                 float64
13                 float64
14                 float64
15                 float64
16                 float64
17                 float64
18                 float64
19                 float64
20                 float64
21                 float64
22                 float64
23                 float64
dtype: object

First 5 rows:
        Date  RNC Id   RBS Id  ...           21           22           23
0 2025-11-26  HLRE01   33333A  ...     0.000000     0.000000     0.000000
1 2025-11-26  HLRE01  U106038  ...  1158.904053  1979.854736  1855.056885
2 2025-11-26  HLRE01  U106038  ...  1093.696167   863.473511  1122.749146
3 2025-11-26  HLRE01  U106038  ...  2201.526733  1092.657593   594.137451
4 2025-11-26  HLRE01  U124002  ...   457.250000   315.675293   311.291504

[5 rows x 28 columns]

Memory usage: 3.49 MB

============================================================
📊 DataFrame: HS_User
============================================================
Shape: 9,275 rows × 28 columns

Columns: ['Date', 'RNC Id', 'RBS Id', 'UCell Id', 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]

Data types:
Date        datetime64[ns]
RNC Id              object
RBS Id              object
UCell Id            object
0                  float64
1                  float64
2                  float64
3                  float64
4                  float64
5                  float64
6                  float64
7                  float64
8                  float64
9                  float64
10                 float64
11                 float64
12                 float64
13                 float64
14                 float64
15                 float64
16                 float64
17                 float64
18                 float64
19                 float64
20                 float64
21                 float64
22                 float64
23                 float64
dtype: object

First 5 rows:
        Date  RNC Id   RBS Id     UCell Id  ...        20        21        22        23   
0 2025-11-26  HLRE01   33333A       33333A  ... -1.000000 -1.000000 -1.000000 -1.000000   
1 2025-11-26  HLRE01  U106038  U106038A10M  ...  4.633333  5.818056  5.413889  5.409722   
2 2025-11-26  HLRE01  U106038  U106038A20M  ...  2.690278  2.813889  2.248611  2.041667   
3 2025-11-26  HLRE01  U106038  U106038A30M  ...  4.908333  6.472222  4.615278  3.188889   
4 2025-11-26  HLRE01  U124002  U124002A10M  ...  2.270833  2.290278  1.451389  1.277778   

[5 rows x 28 columns]

Memory usage: 3.49 MB