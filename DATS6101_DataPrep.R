# Data preparation and cleaning for the final project

# Import CDC data frame object
library(readr)

chronic_df <- read_csv("Documents/GW_Univ/SP2023/DATS6101_Intro_to_Data_Science/DATS6101_Final_Project/PLACES/PLACES__Local_Data_for_Better_Health__Census_Tract_Data_2022_release.csv")

# Keep only some variables #
chronic_df <- chronic_df[(chronic_df$Year == 2020),]
chronic_df <- chronic_df[,c("Year","StateAbbr","StateDesc","CountyName","CountyFIPS","LocationName","Data_Value","TotalPopulation","MeasureId")]

# Keep only rows with health measures of interest #
chronic_df <- chronic_df[(chronic_df$MeasureId=="DEPRESSION" | chronic_df$MeasureId=="SLEEP" | chronic_df$MeasureId=="LPA" | chronic_df$MeasureId=="MHLTH" | chronic_df$MeasureId=="BINGE"),]

# Reshape long to wide #
library(tidyr)
new_chronic_df <- spread(chronic_df, key = MeasureId, value = Data_Value)

nrow(new_chronic_df)
View(new_chronic_df)
str(new_chronic_df)
