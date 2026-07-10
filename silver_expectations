from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="silver_enrollment_expectations",
    comment="Data quality reference table - logs rows with NULL values for auditing purposes",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def silver_enrollment_expectations():
    """
    Expectations table for data quality auditing:
    - Read from bronze_enrollment streaming table
    - Filter for rows that have NULL values in any of the specified columns
    - Log these rows with original values for auditing
    - This is a REFERENCE TABLE ONLY - gold tables should read from silver_enrollment, not this table
    
    This table helps track data quality issues without dropping problematic rows from the main pipeline.
    """
    # Read from bronze streaming table
    df = spark.readStream.table("bronze_enrollment")
    
    # Extract filename from path for context
    df = df.withColumn(
        "filename",
        F.regexp_extract(F.col("source_file_path"), r"([^/]+)\.xlsx$", 1)
    )
    
    # Extract year from filename for context
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
    
    # Extract term from filename for context
    df = df.withColumn(
        "term_extracted",
        F.when(F.col("filename").contains("fall"), F.lit("Fall"))
         .when(F.col("filename").contains("Fall"), F.lit("Fall"))
         .when(F.col("filename").contains("spring"), F.lit("Spring"))
         .when(F.col("filename").contains("Spring"), F.lit("Spring"))
         .otherwise(F.lit("Unknown"))
    )
    
    # Filter for rows with NULL values in any of the specified columns
    expectation_violations = df.filter(
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
        F.col("Instructor_Full_Name").isNull()
    )
    
    # Add columns to identify which specific columns had NULL values
    result_df = expectation_violations.select(
        # Original columns (with NULLs preserved for auditing)
        F.col("Course_ID"),
        F.col("Course_Title"),
        F.col("Course_Name"),
        F.col("Course_Section_Code"),
        F.col("Course_Department"),
        F.col("Instructor_Full_Name"),
        F.col("UGrad"),
        F.col("Grad"),
        F.col("NonDegree"),
        F.col("XReg"),
        F.col("VUS"),
        F.col("Employee"),
        F.col("Withdraw"),
        F.col("Total"),
        F.col("year_extracted").alias("year"),
        F.col("term_extracted").alias("term"),
        
        # Source file context
        F.col("source_file_path"),
        F.col("filename"),
        
        # Timestamp when the violation was detected
        F.current_timestamp().alias("detected_at"),
        
        # Individual NULL flags for detailed tracking
        F.col("UGrad").isNull().alias("ugrad_is_null"),
        F.col("Grad").isNull().alias("grad_is_null"),
        F.col("NonDegree").isNull().alias("nondegree_is_null"),
        F.col("XReg").isNull().alias("xreg_is_null"),
        F.col("VUS").isNull().alias("vus_is_null"),
        F.col("Employee").isNull().alias("employee_is_null"),
        F.col("Withdraw").isNull().alias("withdraw_is_null"),
        F.col("Total").isNull().alias("total_is_null"),
        F.col("year_extracted").isNull().alias("year_is_null"),
        F.col("term_extracted").isNull().alias("term_is_null"),
        F.col("Course_ID").isNull().alias("course_id_is_null"),
        F.col("Course_Title").isNull().alias("course_title_is_null"),
        F.col("Course_Name").isNull().alias("course_name_is_null"),
        F.col("Course_Section_Code").isNull().alias("course_section_code_is_null"),
        F.col("Course_Department").isNull().alias("course_department_is_null"),
        F.col("Instructor_Full_Name").isNull().alias("instructor_full_name_is_null")
    )
    
    return result_df
