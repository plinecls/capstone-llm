import argparse
import logging
from typing import List
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as psf
from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

s3_bucket = "s3a://dataminded-academy-capstone-llm-data-us/"

def read(spark: SparkSession, json_path):
    return spark.read.json(json_path)

def clean(
    df:DataFrame, col_to_explode:str, 
    fields_to_extract:List, 
    alias:str="items") -> DataFrame:

    return (
        df.select(
            psf.explode(col_to_explode).alias(alias)
        ).select(fields_to_extract)
    )

def read_clean_write(spark: SparkSession, environment: str, tag: str, user: str):

    questions = read(spark, json_path=f"{s3_bucket}/input/{tag}/questions.json")
    answers = read(spark, json_path=f"{s3_bucket}/input/{tag}/answers.json")

    cleaned_questions = (clean(
        df=questions, col_to_explode="items", 
        fields_to_extract=['items.question_id', 'items.title', 'items.body', 'items.link'], 
        alias="items")
        .withColumnRenamed("body", "question")
        # .show()
        )

    cleaned_answers = (clean(
        df=answers, col_to_explode="items", 
        fields_to_extract=['items.answer_id', 'items.question_id', 'items.body'], 
        alias="items")
        .withColumnRenamed("body", "answer")
        )

    q_a = (
        cleaned_questions.join(
            cleaned_answers, 
            on="question_id", 
            how='inner'
        )
    )
    
    (q_a.repartition(q_a.count())
        .write
        .mode("overwrite")
        .json(f"{s3_bucket}/cleaned/{user}/{tag}/"))


    
def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=False, default="local"
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="pyspark", required=False
    )
    parser.add_argument(
        "-u", "--user", dest="user", help="the user to write",
        default="paulinec", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        print("This is a local execution of the capestonellm project")
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .getOrCreate()
        )
        read_clean_write(session, args.env, args.tag, args.user)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            read_clean_write(session, args.env, args.tag, args.user)


if __name__ == "__main__":
    main()