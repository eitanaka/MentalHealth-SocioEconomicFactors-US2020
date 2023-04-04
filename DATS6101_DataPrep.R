# Data preparation and cleaning for the final project

# import libraries
library(readr)
library(tidyr)
library(googledrive)

# Import CDC data frame object
# !!!!You need to change this part!!!! You need to write local path of original CDC data set
chronic_df <- read_csv("Documents/GW_Univ/SP2023/DATS6101_Intro_to_Data_Science/DATS6101_Final_Project/PLACES/PLACES__Local_Data_for_Better_Health__Census_Tract_Data_2022_release.csv")
# Keep only some variables #
chronic_df <- chronic_df[(chronic_df$Year == 2020),]
chronic_df <- chronic_df[,c("Year","StateAbbr","StateDesc","CountyName","CountyFIPS","LocationName","Data_Value","TotalPopulation","MeasureId")]
# Keep only rows with health measures of interest #
chronic_df <- chronic_df[(chronic_df$MeasureId=="DEPRESSION" | chronic_df$MeasureId=="SLEEP" | chronic_df$MeasureId=="LPA" | chronic_df$MeasureId=="MHLTH" | chronic_df$MeasureId=="BINGE"),]
# Reshape long to wide #
new_chronic_df <- spread(chronic_df, key = MeasureId, value = Data_Value)

nrow(new_chronic_df)
View(new_chronic_df)
str(new_chronic_df)

# Step 1.1: Create Function to indexing only last 11-digit to merge them with new_chronic_df (For 6 data sets)
# Census Tract datasets "GEO_ID"
# Median Income, Marital Status, Employment Status, Educational attainment, Commute Time

# Authenticate
drive_auth()

# Import csv file on the google drive, Rewrite GEO_ID columns for appropriate form, and extract only columns of interest
import_census_tract_csv <- function(filename, col_names) {
  # Find the file in Google Drive
  file <- drive_find(n_max = 1, pattern = filename)
  # Download the file to the current working directory
  drive_download(as_id(file$id), overwrite = TRUE)
  # Read in the CSV file
  df <- read.csv(filename)
  
  # Index the last 11 digits of the GEO_ID column
  df$GEO_ID <- substr(df$GEO_ID, nchar(df$GEO_ID) - 10, nchar(df$GEO_ID))
  
  # Extract only the columns of interest
  df <- df[, colnames(df) %in% col_names]
  
  # Return the modified data frame
  return(df)
}

# Create data frames
commute_time_df <- import_census_tract_csv("ACSDT5Y2020.B08303-Data.csv")
educatinal_attainment_df <- import_census_tract_csv("ACSST5Y2020.S1501-Data.csv")
employment_status_df <- import_census_tract_csv("ACSST5Y2020.S2301-Data.csv")
martial_status_df <- import_census_tract_csv("ACSST5Y2020.S1201-Data.csv")
median_income_df <- import_census_tract_csv("ACSST5Y2020.S1903-Data.csv")
population_df <- import_census_tract_csv("ACSDT5Y2020.B01003-Data.csv")

# Step 1.2: Create Function Add prefix 0 in the Census Tract Relationship (For 1 data sets)
# Census Tract Relationship "GEOID_TRACT_20"

# Import csv file on the google drive, Rewrite "GEOID_TRACT_20" columns for appropriate form, and extract only columns of interest
import_census_relationship_csv <- function(filename, col_names) {
  # Find the file in Google Drive
  file <- drive_find(n_max = 1, pattern = filename)
  # Download the file to the current working directory
  drive_download(as_id(file$id), overwrite = TRUE)
  # Read in the CSV file
  df <- read.csv(filename)

  # Add prefix-0 in the "GEOID_TRACT_20" column
  df$GEOID_TRACT_20 <- paste0("0", df$GEOID_TRACT_20)
  
  # Extract only the columns of interest
  df <- df[, colnames(df) %in% col_names]
  
  # Return the modified data frame
  return(df)
}

# Create data frames
census_relationship_df <- import_census_relationship_csv("2020_Census_Tract_Relationship.csv")

# Step 1.3 Import Econ impact index data set from google drive


# Step 2.1: Merge Census Tract data sets on LocationName and GEO_ID


# Step 2.2: Merge Census Tract Relationship data sets on LocationName and "GEOID_TRACT_20"


# Step 2.3: Reshape the county economic index data sets and merge columns where we interested in by countyFIPS

