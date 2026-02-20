import os
from pyspark.sql import SparkSession


def main() -> None:

    spark = (
        SparkSession.builder.appName("Appto_SnowFlake").getOrCreate()
    )

    # read postgres data

    jdbc_url = os.getenv("POSTGRES_URL")

    connection_properties = {
        "user" : os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    df = (
        spark.read.jdbc(
            url = jdbc_url,
            table = "users",
            properties = connection_properties
        )
    )

    print("Data Extracted from Application")

    df.show()

    # write data to snowflake
    sfOptions = {
        "sfURL" : f"{os.get("SNOWFLAKE_ACCOUNT")}.snowflakecomputing.com",
        "sfUSER" : os.getenv("SNOWFLAKE_USER"),
        "sfPassword" : os.getenv("SNOWFLAKE_PASSWORD"),
        "sfDatabase" : os.getenv("SNOWFLAKE_DATABASE"),
        "sfSchema" : os.getenv("SNOWFLAKE_SCHEMA"),
        "sfWerehouse" : os.getenv("SNOWFLAKE_WEREHOUSE"),
    }

    (
        df.write
        .format("snowflake")
        .options(**sfOptions)
        .option("dbtable", "users")
        .mode("overwrite")
        .save()
    )

    #
    print("Data written to Snowflake")

    spark.stop()

    return None


if __name__ == "__main__":
    main()