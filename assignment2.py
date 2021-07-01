def election_donations_analysis(input, output1, output2):
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    spark = SparkSession.builder.getOrCreate()

    reader = spark.read
    reader.option("header",True).option("inferSchema",True)
    df = reader.csv(input)

    cand_info = ['cand_id', 'cand_nm']

    grouped_df = df.groupBy(cand_info)\
        .agg(
            F.count(F.when(F.col('contb_receipt_amt') >= '0', 1)).alias("NumDon"),
        F.sum('contb_receipt_amt').alias("Total_cntrb"),
        F.countDistinct(F.when(F.col('contb_receipt_amt') >= '0', F.array('contbr_nm', 'contbr_zip', 'contbr_employer', 'contbr_occupation'))).alias('UnqCtb'),
        F.mean("contb_receipt_amt").alias("Mean"),
        F.stddev("contb_receipt_amt").alias("SD"),
        F.count(F.when((F.col('contb_receipt_amt') < '50') & (F.col('contb_receipt_amt') >= '0'), 1)).alias("smCtb")
        ).withColumn("%", (F.col("smCtb") /  F.col("NumDon"))*100)

    grouped_df.sort(F.desc('cand_id')).show()
    grouped_df.write.format('csv').mode('overwrite').save(output1)

    required_cand_ids = ["P80001571", "P80000722"]
    df_6 = df.filter(df.cand_id.isin(required_cand_ids)).filter("contb_receipt_amt >= '0'").select('cand_nm','contb_receipt_amt')

    df_6.sort(F.desc('contb_receipt_amt')).show()
    df_6.coalesce(1).write.format('csv').mode('overwrite').save(output2)

def files_from_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', metavar= "input")
    parser.add_argument('-o1', '--output1', metavar= "output1")
    parser.add_argument('-o2', '--output2', metavar= "output2")
    args = parser.parse_args()
    return (args.input, args.output1, args.output2)

if __name__ == "__main__":
    inputfile, outputfile1, outputfile2 = files_from_args()
    election_donations_analysis(inputfile, outputfile1, outputfile2)
