import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark_pyspark = SparkSession.builder.master(
    "local[1]").appName("cci").getOrCreate()
#sc = SparkContext('local', 'ccisc')

columns = ["patid", "mi", "chf", "pvd", "cevd", "dementia", "cpd", "rheumd", "pud", "diab", "diabwc", "hp", "rend",
           "mld", "msld", "canc", "metacanc", "aids", "expected_all", "expected_mutual_exclusive"]
data = [
    ("cat_1", "0", "0", "0", "0", "0", "0", "0", "0", "0",
     "0", "0", "0", "0", "0", "1", "0", "1", "6", "6"),
    ("cat_12", "0", "0", "0", "0", "0", "0", "0", "0", "0",
     "0", "0", "0", "0", "0", "1", "1", "1", "12", "10"),
    ("cat_13", "0", "0", "0", "0", "0", "0", "0", "0", "0",
     "0", "0", "0", "0", "0", "1", "0", "0", "2", "2"),
    ("cat_14", "0", "0", "0", "0", "0", "0", "0", "0", "0",
     "0", "0", "0", "0", "0", "1", "1", "0", "8", "6"),
    ("cat_2", "1", "1", "1", "1", "1", "1", "1", "1", "1",
     "1", "1", "1", "1", "1", "1", "1", "1", "28", "24"),
    ("cat_3", "0", "0", "0", "0", "0", "0", "0", "0", "1",
     "1", "0", "0", "1", "1", "1", "1", "0", "15", "11"),
    ("cat_31", "0", "0", "0", "0", "0", "0", "0", "0", "1",
     "1", "0", "0", "1", "0", "0", "1", "0", "9", "9"),
    ("cat_4", "0", "0", "0", "0", "0", "0", "0", "0", "0",
     "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
    ("cat_5", "1", "0", "1", "1", "0", "0", "0", "1", "1",
     "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
    ("cat_6", "0", "0", "0", "0", "0", "0", "0", "0", "0",
     "0", "0", "0", "0", "0", "0", "0", "1", "4", "4"),
]

df = spark_pyspark.createDataFrame(data).toDF(*columns)

map_all = {
    "mi": 0,
    "chf": 2,
    "pvd": 0,
    "cevd": 0,
    "dementia": 2,
    "cpd": 1,
    "rheumd": 1,
    "pud": 0,
    "diabwc": 1,
    "hp": 2,
    "rend": 1,
    "msld": 4,
    "metacanc": 6,
    "aids": 4,
    "diab": 0,
    "canc": 2,
    "mld": 2
}
map_all_bc = spark_pyspark.sparkContext.broadcast(map_all)
map_mutual_exclusive = {
    "diabwc": "diab",
    "metacanc": "canc",
    "msld": "mld"
}
map_mutual_exclusive_bc = spark_pyspark.sparkContext.broadcast(map_mutual_exclusive)
# Add columns to store weights
col_list = map_all_bc.value.keys()
prefix = "quan"
new_col_list = ["_".join([prefix, x]) for x in col_list]
for col, new_col in zip(col_list, new_col_list):
    df = df.withColumn(new_col, F.when(
        F.col(col) == 1, F.lit(map_all_bc.value.get(col))).otherwise(F.lit(0)))

# Add columns for mild conditions of mutually-exclusive comorbidity pairs
prefixed_mild_cols = ["_".join([prefix, x]) for x in map_mutual_exclusive_bc.value.values()]

new_col_list_mex = list(filter(lambda x: x not in prefixed_mild_cols, new_col_list))
for key in map_mutual_exclusive_bc.value.keys():
    weighted_mex_col = "_".join(["mex", prefix, map_mutual_exclusive_bc.value.get(key)])
    new_col_list_mex.append(weighted_mex_col)
    df = df.withColumn(weighted_mex_col, F.when(F.col(key) != 0, F.lit(
        0)).otherwise(F.col("_".join([prefix, map_mutual_exclusive_bc.value.get(key)]))))

# Calculate Quan CCI
df = df.withColumn("Experimental_quan_all", F.expr('+'.join(new_col_list)))
df = df.withColumn("quan_mutual_exclusive", F.expr('+'.join(new_col_list_mex)))

df = df.drop(*new_col_list)
df = df.drop(*new_col_list_mex)
df.show()
print(df.explain(extended=True))