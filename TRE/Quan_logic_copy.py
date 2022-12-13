import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark_pyspark = SparkSession.builder.master(
    "local[1]").appName("cci").getOrCreate()

# columns = ["PERSON_ID", "cov_hx_out_mi_flag", "cov_hx_out_mi_date", "cov_hx_out_ckd_flag", "cov_hx_out_ckd_date",
#        "cov_hx_out_dm_flag", "cov_hx_out_dm_date", "cov_hx_out_chd_flag", "cov_hx_out_chd_date"]

columns = ["patid", "mi", "chf", "pvd", "cevd", "dementia", "cpd", "rheumd", "pud", "diab", "diabwc", "hp", "rend",
           "mld", "msld", "canc", "metacanc", "aids", "expected_all", "expected_mutual_exclusive_"]
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

map_mutual_exclusive = {
    "diabwc": "diab",
    "metacanc": "canc",
    "msld": "mld"
}
# Add columns to store weights
col_list = map_all.keys()
prefix = "quan"
new_col_list = ["_".join([prefix, x]) for x in col_list]
for col, new_col in zip(col_list, new_col_list):

    df = df.withColumn(new_col, F.when(
        F.col(col) == 1, F.lit(map_all.get(col))).otherwise(F.lit(0)))

# Add columns for mild conditions of mutually-exclusive comorbidity pairs
prefixed_mild_cols = ["_".join([prefix, x]) for x in map_mutual_exclusive.values()]
new_col_list_mex = [
    item for item in new_col_list if item not in prefixed_mild_cols]

for key in map_mutual_exclusive.keys():
    weighted_mex_col = "_".join(["mex", prefix, map_mutual_exclusive.get(key)])
    new_col_list_mex.append(weighted_mex_col)
    df = df.withColumn(weighted_mex_col, F.when(F.col(key) != 0, F.lit(
        0)).otherwise(F.col("_".join([prefix, map_mutual_exclusive.get(key)]))))

# Calculate Quan CCI
df = df.withColumn("quan_mutual_exclusive", F.expr('+'.join(new_col_list_mex)))
df = df.withColumn("_DO_NOT_USE_quan_all", F.expr('+'.join(new_col_list)))

df = df.drop(*new_col_list)
df = df.drop(*new_col_list_mex)
df.show()
