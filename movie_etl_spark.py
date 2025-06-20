from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc

# ------------------
# EXTRACT
# ------------------

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Movie ETL") \
    .getOrCreate()

# Load ratings data
ratings = spark.read.option("delimiter", "\t").csv("movie_data/u_data.txt")
ratings = ratings.toDF("user_id", "movie_id", "rating", "timestamp")

# Load movie titles
movies = spark.read.option("delimiter", "|").csv("movie_data/u_item.txt")
movies = movies.toDF("movie_id", "title", "release_date", "video_release_date", "IMDb_URL",
                     "unknown", "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
                     "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery",
                     "Romance", "Sci-Fi", "Thriller", "War", "Western")

# ------------------
# TRANSFORM
# ------------------

# Keep only relevant movie columns
movies = movies.select("movie_id", "title")

# Cast movie_id to match types
ratings = ratings.withColumn("movie_id", col("movie_id").cast("string"))
movies = movies.withColumn("movie_id", col("movie_id").cast("string"))

# Join ratings with movie titles
joined = ratings.join(movies, on="movie_id")

# Calculate average rating and number of ratings per movie
movie_stats = joined.groupBy("title") \
    .agg(
        avg("rating").alias("avg_rating"),
        count("rating").alias("num_ratings")
    )

# Filter for popular movies and sort by rating
popular = movie_stats.filter(col("num_ratings") >= 100) \
    .orderBy(desc("avg_rating"))

# ------------------
# LOAD
# ------------------

# Show and sace results
popular.show(15, truncate=False)
popular.write.csv("results", header=True, mode="overwrite")

# End Spark session
spark.stop()