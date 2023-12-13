import os
import re, glob, csv, pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

SPARK_OUTPUT_FOLDER = "output/"
CSV_RESULT_FILE = 'merged_spark_output.csv'
EXTENDED_CSV_FILE = 'extended_output.csv'
DUMPS_FILE_PATH = 'wiki_dumps'


# UDF for extracting the year from wikipedia XML dumps
def extract_year(text):
    if text is None:
        return None

    match = re.search(r"\|\s*founded\s*=.*?(\d{4})", text)
    return match.group(1) if match else None


# the main method for parsing the wikipedia dumps
def parse_wiki_dump_to_csv_format():
    # spark session initialization
    spark = SparkSession.builder \
        .appName("WikipediaParser") \
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0") \
        .getOrCreate()

    files = os.listdir(DUMPS_FILE_PATH)
    count = 0

    # processing of each dump
    for xml_file_path in files:
        # reading the XML file into the dataframe
        print(os.path.join(DUMPS_FILE_PATH, xml_file_path))
        df = spark.read.format("xml").option("rowTag", "page").load(os.path.join(DUMPS_FILE_PATH, xml_file_path))

        parsed_df = df.select(
            col("title"),
            col("revision.text._VALUE").alias("text")
        )

        extract_year_udf = udf(extract_year, StringType())

        # creation of the new column with extracted data from wiki dump
        parsed_df = parsed_df.withColumn("Publisher founded in", extract_year_udf(col("text")))

        parsed_df.select("title", "Publisher founded in").write.csv(SPARK_OUTPUT_FOLDER + '/' + str(count), header=True,
                                                                    mode="overwrite")
        count += 1
    spark.stop()


# spark produces the result split to parts which i have to join in this method
def merge_csv_files():
    csv_files = glob.glob(os.path.join(SPARK_OUTPUT_FOLDER, '**', '*.csv'), recursive=True)

    with open(CSV_RESULT_FILE, 'w', newline='') as output_csv:
        csv_writer = csv.writer(output_csv)

        for csv_file in csv_files:
            with open(csv_file, 'r') as input_csv:
                csv_reader = csv.reader(input_csv)

                if csv_file != csv_files[0]:
                    next(csv_reader)

                for row in csv_reader:
                    if row[1]:
                        csv_writer.writerow(row)


# this method is used for merging the crawled data with the csv files produces by spark
def extend_crawled_data():
    df1 = pd.read_csv("books.csv", quoting=csv.QUOTE_MINIMAL)
    df2 = pd.read_csv("final_output.csv", quoting=csv.QUOTE_MINIMAL)

    merged_df = pd.merge(df1, df2, left_on=df1["Publisher"].str.lower(), right_on=df2["title"].str.lower(), how="left")
    merged_df = merged_df.drop(columns=['key_0', 'title'])

    merged_df.to_csv(EXTENDED_CSV_FILE, index=False)
    mask = merged_df["Publisher founded in"].notnull()
    print(merged_df[mask])


if __name__ == "__main__":
    parse_wiki_dump_to_csv_format()
    merge_csv_files()
    extend_crawled_data()