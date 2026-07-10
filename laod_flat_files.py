from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="bronze_enrollment",
    comment="Bronze layer - raw enrollment data ingested from Excel files in Volume",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_enrollment():
    """
    Ingest all Excel files from the enrollment_files volume.
    Excel files have:
    - Rows 1-3: Title, timestamp, blank row (filtered out)
    - Row 4: Header row (treated as data, need to filter)
    - Data rows starting from row 5
    - Footer notes at the end (filtered out)
    """
    # Read Excel files with Auto Loader without header
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "excel")
        .option("header", "false")  # Read all rows as data
        .option("inferSchema", "true")  # Infer data types
        .option("cloudFiles.schemaEvolutionMode", "none")  # Excel format requires 'none' mode
        .load("/Volumes/enrollment_report/enrollment/enrollment_files/")
    )
    
    # Filter out title rows (first 3 rows) and header row (4th row) and footer notes
    # Row patterns to exclude:
    # - Title: "_c0" contains "Class Enrollment Summary"
    # - Timestamp: "_c0" contains "Time run:"
    # - Header: "_c0" = "Course ID"
    # - Grand Total: "_c0" = "Grand Total"
    # - Filter criteria: "_c0" = "and" or contains "Class Acad"
    # - NULL rows and "Rows 1 -" pattern
    cleaned_df = df.filter(
        (F.col("_c0").isNotNull()) &  # Remove NULL rows
        (~F.col("_c0").contains("Class Enrollment Summary")) &  # Remove title
        (~F.col("_c0").contains("Time run:")) &  # Remove timestamp
        (F.col("_c0") != "Course ID") &  # Remove header row
        (~F.col("_c0").startswith("Grand Total")) &  # Remove Grand Total row
        (F.col("_c0") != "and") &  # Remove filter criteria rows
        (~F.col("_c1").contains("Class Acad Career Code")) &  # Remove filter criteria rows
        (~F.col("_c1").contains("Rows 1 -"))  # Remove row count footer
    )
    
    # Rename columns to match the header (based on row 4 structure observed)
    # Also include the file path metadata for downstream processing
    result_df = cleaned_df.select(
        F.col("_c0").alias("Course_ID"),
        F.col("_c1").alias("Course_Title"),
        F.col("_c2").alias("Course_Name"),
        F.col("_c3").alias("Course_Section_Code"),
        F.col("_c4").alias("Course_Department"),
        F.col("_c5").alias("Instructor_Full_Name"),
        F.col("_c6").alias("UGrad"),
        F.col("_c7").alias("Grad"),
        F.col("_c8").alias("NonDegree"),
        F.col("_c9").alias("XReg"),
        F.col("_c10").alias("VUS"),
        F.col("_c11").alias("Employee"),
        F.col("_c12").alias("Withdraw"),
        F.col("_c13").alias("Total"),
        F.col("_metadata.file_path").alias("source_file_path")  # Capture source file path
    )
    
    return result_df
