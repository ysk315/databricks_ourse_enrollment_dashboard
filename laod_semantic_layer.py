from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Metric 1: Total number of UGRAD students by year between 2014 and 2026
@dp.materialized_view(
    name="gold_total_ugrad_2014_2026",
    comment="Gold layer - Total number of undergraduate students enrolled by year between 2014 and 2026"
)
def gold_total_ugrad_2014_2026():
    """
    Calculates the total number of undergraduate student enrollments 
    for each year from 2014 to 2026.
    """
    df = spark.read.table("silver_enrollment")
    
    result = df.filter(
        (F.col("year") >= 2014) & (F.col("year") <= 2026)
    ).groupBy(
        "year"
    ).agg(
        F.sum("UGrad").alias("total_ugrad_enrollments")
    ).orderBy("year")
    
    return result


# Metric 2: Total number of enrollments in all classes by year since 2014 to 2026
@dp.materialized_view(
    name="gold_total_enrollments_2014_2026",
    comment="Gold layer - Total number of enrollments in all classes by year between 2014 and 2026"
)
def gold_total_enrollments_2014_2026():
    """
    Calculates the total number of student enrollments (all categories)
    for each year from 2014 to 2026.
    """
    df = spark.read.table("silver_enrollment")
    
    result = df.filter(
        (F.col("year") >= 2014) & (F.col("year") <= 2026)
    ).groupBy(
        "year"
    ).agg(
        F.sum("Total").alias("total_enrollments")
    ).orderBy("year")
    
    return result


# Metric 3: Top 5 courses by overall enrollment from 2014 to 2026
@dp.materialized_view(
    name="gold_top_5_courses_2014_2026",
    comment="Gold layer - Top 5 courses by total enrollment between 2014 and 2026"
)
def gold_top_5_courses_2014_2026():
    """
    Identifies the top 5 courses with the highest total enrollment
    from 2014 to 2026.
    """
    df = spark.read.table("silver_enrollment")
    
    result = df.filter(
        (F.col("year") >= 2014) & (F.col("year") <= 2026)
    ).groupBy(
        "Course_ID",
        "Course_Title",
        "Course_Name",
        "Course_Department"
    ).agg(
        F.sum("Total").alias("total_enrollments")
    ).orderBy(
        F.col("total_enrollments").desc()
    ).limit(5)
    
    return result


# Metric 4: Enrollments per year for the top 5 courses (all in one table)
@dp.materialized_view(
    name="gold_top_5_courses_by_year_2014_2026",
    comment="Gold layer - Yearly enrollment breakdown for the top 5 courses between 2014 and 2026"
)
def gold_top_5_courses_by_year_2014_2026():
    """
    Shows enrollment numbers by year for each of the top 5 courses
    from 2014 to 2026. All data in a single table for easy analysis.
    """
    df = spark.read.table("silver_enrollment")
    
    # Filter for the year range
    df_filtered = df.filter(
        (F.col("year") >= 2014) & (F.col("year") <= 2026)
    )
    
    # First, identify the top 5 courses
    top_5_courses = df_filtered.groupBy(
        "Course_ID",
        "Course_Title",
        "Course_Name",
        "Course_Department"
    ).agg(
        F.sum("Total").alias("total_enrollments")
    ).orderBy(
        F.col("total_enrollments").desc()
    ).limit(5).select(
        "Course_ID",
        "Course_Title",
        "Course_Name",
        "Course_Department"
    )
    
    # Join back to get yearly breakdown for only these top 5 courses
    result = df_filtered.join(
        top_5_courses,
        on=["Course_ID", "Course_Title", "Course_Name", "Course_Department"],
        how="inner"
    ).groupBy(
        "Course_ID",
        "Course_Title",
        "Course_Name",
        "Course_Department",
        "year",
        "term"
    ).agg(
        F.sum("UGrad").alias("UGrad_enrollments"),
        F.sum("Grad").alias("Grad_enrollments"),
        F.sum("NonDegree").alias("NonDegree_enrollments"),
        F.sum("XReg").alias("XReg_enrollments"),
        F.sum("VUS").alias("VUS_enrollments"),
        F.sum("Employee").alias("Employee_enrollments"),
        F.sum("Withdraw").alias("Withdraw_count"),
        F.sum("Total").alias("Total_enrollments")
    ).orderBy(
        F.col("Total_enrollments").desc(),
        "year",
        "term"
    )
    
    return result
