# Data preparation and cleaning for the final project

# import libraries
library(readr)
library(tidyr)

# Import CDC data frame object
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
# Census Tract datasets
# Median Income, Marital Status, Employment Status, Educational attainment, Commute Time


# Step 1.2: Create Function Add prefix 0 in the Census Tract Relationship (For 1 data sets)
# Census Tract Relationship


# Step 2: Import data sets and return new_df returned by function above


# Step 3.1: Merge Census Tract data sets only the columns where we interested in by LocationName


# Step 3.2: Merge Census Tract Relationship data sets only the columns where we interested in by LocationName


# Step 3.3: Reshape the county economic index data seets and merge columns where we interested in by countyFIPS

