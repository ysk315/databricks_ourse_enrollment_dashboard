from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="silver_enrollment",
    comment="Silver layer - cleaned enrollment data with comprehensive NULL handling and year/term extraction",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def silver_enrollment():
    """
    Silver layer transformation:
    - Read from bronze_enrollment streaming table
    - Replace NULL values:
      * Numeric columns (UGrad, Grad, NonDegree, XReg, VUS, Employee, Withdraw, Total) → 0
      * Year → 0 if NULL
      * Term → 'Unknown' if NULL
      * String columns (Course_ID, Course_Title, etc.) → 'Unknown'
    - Add has_expectation_violation flag for rows with any NULL values
    - Extract year and term from filename
    - Keep ALL rows in the table (no dropping)
    """
    # Read from bronze streaming table
    df = spark.readStream.table("bronze_enrollment")
    
    # Extract filename from path for year/term extraction
    df = df.withColumn(
        "filename",
        F.regexp_extract(F.col("source_file_path"), r"([^/]+)\.xlsx$", 1)
    )
    
    # Extract year from filename (handles various formats like "fall_1998", "3.19.24", "10.31.25")
    df = df.withColumn(
        "year_extracted",
        F.when(
            F.col("filename").rlike(r"_(19\d{2}|20\d{2})"), 
            F.regexp_extract(F.col("filename"), r"_(19\d{2}|20\d{2})", 1).cast("int")
        ).when(
            F.col("filename").rlike(r"\d{1,2}\.\d{1,2}\.(\d{2})"),
            F.expr("CAST(CONCAT('20', regexp_extract(filename, r'\\d{1,2}\\.\\d{1,2}\\.(\\d{2})', 1)) AS INT)")
        ).otherwise(None)
    )
    
    # Extract term from filename (Fall, Spring, or from date)
    df = df.withColumn(
        "term_extracted",
        F.when(F.col("filename").contains("fall"), F.lit("Fall"))
         .when(F.col("filename").contains("Fall"), F.lit("Fall"))
         .when(F.col("filename").contains("spring"), F.lit("Spring"))
         .when(F.col("filename").contains("Spring"), F.lit("Spring"))
         .otherwise(F.lit("Unknown"))
    )
    
    # Create a flag to identify rows with NULL values (expectation violations)
    # Check for NULLs in all specified columns
    df = df.withColumn(
        "has_expectation_violation",
        F.when(
            F.col("UGrad").isNull() | 
            F.col("Grad").isNull() | 
            F.col("NonDegree").isNull() | 
            F.col("XReg").isNull() | 
            F.col("VUS").isNull() | 
            F.col("Employee").isNull() | 
            F.col("Withdraw").isNull() | 
            F.col("Total").isNull() |
            F.col("year_extracted").isNull() |
            F.col("term_extracted").isNull() |
            F.col("Course_ID").isNull() |
            F.col("Course_Title").isNull() |
            F.col("Course_Name").isNull() |
            F.col("Course_Section_Code").isNull() |
            F.col("Course_Department").isNull() |
            F.col("Instructor_Full_Name").isNull(),
            True
        ).otherwise(False)
    )
    
    # Apply NULL handling transformations
    # All rows are kept; NULLs are replaced with appropriate defaults
    result_df = df.select(
        # String columns - set to 'Unknown' if NULL
        F.coalesce(F.col("Course_ID"), F.lit("Unknown")).alias("Course_ID"),
        F.coalesce(F.col("Course_Title"), F.lit("Unknown")).alias("Course_Title"),
        F.coalesce(F.col("Course_Name"), F.lit("Unknown")).alias("Course_Name"),
        F.coalesce(F.col("Course_Section_Code"), F.lit("Unknown")).alias("Course_Section_Code"),
        F.coalesce(F.col("Course_Department"), F.lit("Unknown")).alias("Course_Department"),
        F.coalesce(F.col("Instructor_Full_Name"), F.lit("Unknown")).alias("Instructor_Full_Name"),
        
        # Numeric enrollment columns - cast to int and set to 0 if NULL
        F.coalesce(F.col("UGrad").cast("int"), F.lit(0)).alias("UGrad"),
        F.coalesce(F.col("Grad").cast("int"), F.lit(0)).alias("Grad"),
        F.coalesce(F.col("NonDegree").cast("int"), F.lit(0)).alias("NonDegree"),
        F.coalesce(F.col("XReg").cast("int"), F.lit(0)).alias("XReg"),
        F.coalesce(F.col("VUS").cast("int"), F.lit(0)).alias("VUS"),
        F.coalesce(F.col("Employee").cast("int"), F.lit(0)).alias("Employee"),
        F.coalesce(F.col("Withdraw").cast("int"), F.lit(0)).alias("Withdraw"),
        F.coalesce(F.col("Total").cast("int"), F.lit(0)).alias("Total"),
        
        # Year and term - set to 0 and 'Unknown' respectively if NULL
        F.coalesce(F.col("year_extracted"), F.lit(0)).alias("year"),
        F.coalesce(F.col("term_extracted"), F.lit("Unknown")).alias("term"),
        
        # Keep the expectation violation flag for reference
        F.col("has_expectation_violation")
    )
    
    return result_df
