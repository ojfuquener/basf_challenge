
import nltk
from nltk.corpus import stopwords

nltk.download('stopwords')

from awsglue.context import GlueContext
from awsglue.job import Job
from delta import DeltaTable
from pyspark.context import SparkConf, SparkContext
from pyspark.ml.feature import StopWordsRemover, Tokenizer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.window import Window

XML_SCHEMA = StructType([
    StructField("_doc-number", StringType(), True),
    StructField("abstract", ArrayType(
        StructType([
            StructField("_lang", StringType(), True),
            StructField("p", StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_num", StringType(), True)
            ]), True)
        ]),
        True
    )),
    StructField("ca-bibliographic-data", StructType([
        StructField("invention-title", ArrayType(
            StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_lang", StringType(), True)
            ]),
            True
        ))
    ]), True),
    StructField("description", StructType([
        StructField("disclosure", StructType([
            StructField("p", StringType(), True)
        ]), True)
    ]), True)
])

FILES_LOCATION = 's3://patents-challenge/xml-with-description/*.xml'
REGEX_TO_CLEAN = "<\/?br>|[^a-zA-Z0-9() %,-]|^\([^)]*\)|[ \t]+$|\s+$"
BLANK = ''
LIMIT_TOP_WORDS = 1000
LANGUAGE_STOP_WORDS = 'english'
S3_PATH_DELTA_TABLES = 's3://basf-challenge-tables'
DELTA_TABLE_SCHEMA = 'patents'

def load_xml_files(files_path: str, spark_session: SparkSession) -> DataFrame:
    files_location = files_path
    df = spark_session.read.format('com.databricks.spark.xml')\
    .options(rowTag='ca-patent-document')\
    .options(inferSchema=False)\
    .load(files_location, schema=XML_SCHEMA)
    return df


def clean_column_df(input_df: DataFrame, column_to_clean: str, new_column_cleaned: str, regex_to_use: str, str_used_to_replace: str) -> DataFrame:
    column_cleaned = input_df.withColumn(f'{new_column_cleaned}', regexp_replace(col(f'{column_to_clean}'), f'{regex_to_use}', f'{str_used_to_replace}'))
    return column_cleaned


def explode_df_array_column(input_df: DataFrame, column_to_explode: str, column_explode_destination: str):
    column_exploded_df = input_df.withColumn(
                                    f"{column_explode_destination}", 
                                    explode(col(f"{column_to_explode}")))
    return column_exploded_df


def tokenize_df_string_column(input_df: DataFrame, column_to_tokenize: str, column_tokenized: str):
    xml_tokenize_string_df = input_df.withColumn('column_no_trail_spaces', concat_ws(',', col(f'{column_to_tokenize}')))
    tokenizer = Tokenizer(inputCol='column_no_trail_spaces', outputCol=f"{column_tokenized}")
    description_tokens_df = tokenizer.transform(xml_tokenize_string_df)
    return description_tokens_df


def remove_english_stop_words(input_df: DataFrame, column_to_remove_stopwords: str, column_result: str, stop_words_language: str):
    
    stop_words = set(stopwords.words(f'{stop_words_language}'))
    remover_stop_words_english = StopWordsRemover(inputCol=column_to_remove_stopwords, 
                                                  outputCol=column_result, 
                                                  stopWords=list(stop_words))
    drop_stop_words_english_df = remover_stop_words_english.transform(input_df)
    return drop_stop_words_english_df


def get_top_most_frequent_words(frequent_words_df: DataFrame, column_with_words: str, partition_column: str, num_top_words: int):
    window_spec = Window.partitionBy(f"{partition_column}").orderBy(frequent_words_df["count"].desc())
    ranked_words = frequent_words_df.withColumn("rank_col", rank().over(window_spec))
    top_words = ranked_words.filter(ranked_words["rank_col"] <= num_top_words)
    most_freq_words_df = top_words.groupBy(f"{partition_column}")\
                                      .agg(collect_list(f'{column_with_words}')\
                                      .alias('most_frequent_words'))
    return most_freq_words_df


def create_delta_schema(spark_session: SparkSession, schema_name: str):
    spark_session.sql(f"""CREATE SCHEMA IF NOT EXISTS {schema_name}
                          COMMENT 'A new Unity Catalog schema called {schema_name}'""")
    

def create_delta_table_top_words_by_patent(spark_session: SparkSession):
    create_delta_schema(spark_session, 'patents')
    spark_session.sql(f"USE patents")
    spark_session.sql("""CREATE TABLE top_words_by_patent (`_doc-number` BIGINT, `description` STRING, `most_frequent_words` INT)
                         USING parquet
                         LOCATION 's3://basf-challenge-tables/patents/top_words_by_patent'""")
    

    
def create_delta_table_top_words_whole_patents(spark_session: SparkSession):
    create_delta_schema(spark_session, 'patents')
    spark_session.sql(f"USE patents")
    spark_session.sql("""CREATE TABLE top_words_whole_patents (`timestamp` TIMESTAMP, `word` STRING, `num_ocurrences` INT)
                         USING parquet
                         LOCATION 's3://basf-challenge-tables/patents/top_words_whole_patents'""")
    
    
def drop_delta_table(spark_session: SparkSession, table_name: str):
    spark_session.sql(F"DROP TABLE {table_name}")


def persist_df_as_delta_table(df: DataFrame, table_name: str, schema_name: str):    
    # Save the DataFrames as Delta tables in the specified schema  
    df.write.format("delta").mode("overwrite").saveAsTable(f"{schema_name}.{table_name}")


def process_xml_data(xml_df: DataFrame):
    description_cleaned = clean_column_df(input_df=xml_df,
                                          column_to_clean='description.disclosure.p', 
                                          new_column_cleaned='description_cleaned', 
                                          regex_to_use=REGEX_TO_CLEAN, 
                                          str_used_to_replace=BLANK)
    
    dq_xml_with_mandatory_fields_df = description_cleaned.select(
        '_doc-number', 'ca-bibliographic-data.invention-title', 
        'abstract', 'description_cleaned'
    ).filter(
        col('_doc-number').isNotNull() &
        col('ca-bibliographic-data.invention-title').isNotNull() & 
        col('abstract').isNotNull() &
        col('description_cleaned').isNotNull()
    )

    # Explode `abstract` field to split records by language 
    dq_xml_abstract_by_language_df = explode_df_array_column(input_df=dq_xml_with_mandatory_fields_df,
                                                            column_to_explode='abstract', 
                                                            column_explode_destination='abstract_by_languages')


    count_abstract_in_english_df=dq_xml_abstract_by_language_df.select('_doc-number', 'abstract_by_languages')\
                                                            .filter(col('abstract_by_languages._lang') == 'en')\
                                                            .groupBy('_doc-number')\
                                                            .agg(count('abstract_by_languages')\
                                                            .alias('num_abstract_in_english'))

    num_abstract_in_english_df = dq_xml_abstract_by_language_df.join(count_abstract_in_english_df, '_doc-number', 'inner')

    # Filter the records that have at least 1 abstract in english language
    xml_ready_to_process_df = num_abstract_in_english_df.filter((col('num_abstract_in_english') > 0) & (col('abstract_by_languages._lang') == 'en'))

    abstract_cleaned_df = clean_column_df(input_df=xml_ready_to_process_df,
                                        column_to_clean='abstract_by_languages.p._VALUE', 
                                        new_column_cleaned='abstract_cleaned', 
                                        regex_to_use=REGEX_TO_CLEAN, 
                                        str_used_to_replace=BLANK)\
                                        .drop('abstract')\
                                        .drop('abstract_by_languages')\
                                        .drop('num_abstract_in_english')

    # Explode `invention-title` field to split records by language 
    invention_title_explode = explode_df_array_column(input_df=dq_xml_with_mandatory_fields_df,
                                        column_to_explode='invention-title', 
                                        column_explode_destination='invention-title_by_languages')

    invention_title_english = invention_title_explode.filter(col('invention-title_by_languages._lang') == 'en')
    
    regex_to_clean = "[^a-zA-Z0-9 \\s]"
    invention_cleaned_df = clean_column_df(input_df=invention_title_english,
                                          column_to_clean='invention-title_by_languages._VALUE', 
                                          new_column_cleaned='invention_cleaned', 
                                          regex_to_use=regex_to_clean, 
                                          str_used_to_replace=BLANK)

    result_tokenized_invention_df = tokenize_df_string_column(input_df=invention_cleaned_df,
                                                            column_to_tokenize='invention_cleaned',
                                                            column_tokenized='invention_tokenized')

    drop_stop_words_english_invention_df = remove_english_stop_words(input_df=result_tokenized_invention_df,
                                                                    column_to_remove_stopwords='invention_tokenized',
                                                                    column_result='invention_no_stop_words',
                                                                    stop_words_language=LANGUAGE_STOP_WORDS)


    most_frequent_words_invention_df = drop_stop_words_english_invention_df.withColumn(
        'invention_single_word', explode(col('invention_no_stop_words'))
    )

    df_frequent_words_invention = most_frequent_words_invention_df.groupBy('_doc-number', 'invention_single_word')\
                                                                .count().orderBy('count', ascending=False)\
                                                                .select('_doc-number', 'invention_single_word', 'count')

    result_tokenized_descrip_df = tokenize_df_string_column(input_df=abstract_cleaned_df,
                                                            column_to_tokenize='description_cleaned',
                                                            column_tokenized='description_tokenized')

    result_tokenized_abstract_df = tokenize_df_string_column(input_df=abstract_cleaned_df,
                                                            column_to_tokenize='abstract_cleaned',
                                                            column_tokenized='abstract_tokenized')


    drop_stop_words_english_descrip_df = remove_english_stop_words(input_df=result_tokenized_descrip_df,
                                                                column_to_remove_stopwords='description_tokenized',
                                                                column_result='description_tokenized_no_stop_words',
                                                                stop_words_language=LANGUAGE_STOP_WORDS)


    drop_stop_words_english_abstract_df = remove_english_stop_words(input_df=result_tokenized_abstract_df,
                                                                    column_to_remove_stopwords='abstract_tokenized',
                                                                    column_result='abstract_tokenized_no_stop_words',
                                                                    stop_words_language=LANGUAGE_STOP_WORDS)

    most_frequent_words_description_df = drop_stop_words_english_descrip_df.withColumn(
        'descrip_single_word', explode(col('description_tokenized_no_stop_words'))
    )

    most_frequent_words_abstract_df = drop_stop_words_english_abstract_df.withColumn(
        'abstract_single_word', explode(col('abstract_tokenized_no_stop_words'))
    )

    df_frequent_description_words = most_frequent_words_description_df.groupBy('_doc-number', 'descrip_single_word')\
                                                                      .count().orderBy('count', ascending=False)\
                                                                      .select('_doc-number', 'descrip_single_word', 'count')

    df_frequent_abstract_words = most_frequent_words_abstract_df.groupBy('_doc-number', 'abstract_single_word')\
                                                                .count().orderBy('count', ascending=False)\
                                                                .select('_doc-number', 'abstract_single_word', 'count')

    column_with_words = 'descrip_single_word'
    partition_column = '_doc-number'
    num_top_words = 3
    top_description_words_df = get_top_most_frequent_words(df_frequent_description_words, column_with_words, partition_column, num_top_words)

    df_frequent_abstract_words = df_frequent_abstract_words.withColumnRenamed('abstract_single_word', 'word')\
                                                           .withColumn('timestamp', current_timestamp())

    df_frequent_description_words = df_frequent_description_words.withColumnRenamed('descrip_single_word', 'word')\
                                                                 .withColumn('timestamp', current_timestamp())

    df_frequent_words_invention = df_frequent_words_invention.withColumnRenamed('invention_single_word', 'word')\
                                                             .withColumn('timestamp', current_timestamp())

    total_most_frequent_words_df = df_frequent_abstract_words.unionAll(df_frequent_description_words).unionAll(df_frequent_words_invention)
    total_most_frequent_words_all = total_most_frequent_words_df.select('timestamp', 'word', 'count')\
                                                                .orderBy('count', ascending=False)\
                                                                .filter(col("word").cast("int").isNull())\
                                                                .limit(LIMIT_TOP_WORDS)

    print("DATAFRAME CALCULATION 1")
    top_description_words_df.show()
    
    print("DATAFRAME CALCULATION 2")
    total_most_frequent_words_all.show()
    
    # Save dataframes as delta tables    
    persist_df_as_delta_table(df=top_description_words_df, table_name='top_words_by_patent', schema_name=DELTA_TABLE_SCHEMA)
    
    persist_df_as_delta_table(df=total_most_frequent_words_all, table_name='top_words_whole_patents', schema_name=DELTA_TABLE_SCHEMA)

    
if __name__=='__main__':
    conf = SparkConf()
    conf.set('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    conf.set('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    sc = SparkContext.getOrCreate(conf=conf)
    glueContext = GlueContext(sc)
    spark_session = glueContext.spark_session
    job = Job(glueContext)

    #drop_delta_table(spark_session, "patents.top_words_by_patent")
    #drop_delta_table(spark_session, "patents.top_words_whole_patents")

    create_delta_table_top_words_by_patent(spark_session)    
    create_delta_table_top_words_whole_patents(spark_session)

    xml_df = load_xml_files(FILES_LOCATION, spark_session)
    xml_df.printSchema()
    process_xml_data(xml_df)

    job.commit()
