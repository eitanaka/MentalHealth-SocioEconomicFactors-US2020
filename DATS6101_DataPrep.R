# Data preparation and cleaning for the final project

# import libraries
library(readr)
library(tidyr)
library(googledrive)
# install.packages("tidycensus")
library(tidycensus)
library(tidyverse)
library(tigris)
library(purrr)
library(tidyr)
library(censusapi)
#-------------------------------------------------------------------------------------
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

#--------------------------------------------------------------------------------------
# Get ACS data
# Variables from ACS (Marital Status, Total Population, Education Attainment, Median Income, Commute Time, Employment Status)
# Function to get ACS data which geography is tract
get_ACS_tract_allState_2020 <- function(myVariables, myVarName) {
  
  myState <- c(state.abb, "DC")
  myVariables <- myVariables
  
  df <- get_acs(geography="tract",
                variables=myVariables,
                cache_table=T,
                year=2020,
                state=myState)
  
  df <- select(df, -moe)
  df <- spread(df, key=variable, value=estimate)
  
  names(df)[3:length(df)] <- myVarName
  
  return(df)
}
# Function to get ACS data which geography is block group
get_ACS_block_group_allState_2020 <- function(myVariables, myVarName) {
  
  myState <- c(state.abb, "DC")
  myVariables <- myVariables
  
  df <- get_acs(geography="block group",
                variables=myVariables,
                cache_table=T,
                year=2020,
                state=myState)
  
  df <- select(df, -moe)
  df <- spread(df, key=variable, value=estimate)
  
  df$GEOID <- substr(df$GEOID, 1, nchar(df$GEOID)-1)
  df$NAME <- substr(df$NAME, 16, nchar(df$NAME))
  
  df <- df %>%
    group_by(GEOID, NAME) %>%
    summarise(across(1:length(myVariables), sum))
  
  names(df)[3:length(df)] <- myVarName
  
  return(df)
}
# Function to merge two ACS data set on GEOID
merge_ACS_2020 <- function(df1, df2) {
  merged_df <- merge(df1, df2, by=c("GEOID","NAME"))
  return(merged_df)
}

# Access ACS API using following API key
census_api_key("22e600faf5925e380665317f69c684f0621c4e05", install=T, overwrite=T)

# Get variable names contained asc5 in 2020
v20 <- load_variables(2020, "acs5", cache = TRUE)
View(v20)

# Get ACS data by tract level in 2020 (Martial Status, Education Attainment, and Median Income) (unit is # of population)
myVariables <- c("B07008_001", "B07008_002", "B07008_003", "B07008_004", "B07008_005", "B07008_006",
                 "B07009_001", "B07009_002", "B07009_003", "B07009_004", "B07009_005", "B07009_006",
                 "B07011_001")
myVarName <- c('MT_total', 'MT_Never Married', 'MT_Now married', 'MT_Divorces', 'MT_Separated', 'MT_Widowed',
               'EA_total', 'EA_Less than high school graduate','EA_High school graduate',"EA_college or associate's degree","EA_Bachelor's degree", 'EA_Graduate or professional degree',
               'MI_Estimate')
acs_tract_2020_df <- get_ACS_tract_allState_2020(myVariables, myVarName)

# Get ACS data by block level in 2020 (Total Population, Commute Time, Employment Status) (unit is # of population)
myVariables <- c("B01003_001",
                 "B08134_001", "B08134_002","B08134_003","B08134_004","B08134_005","B08134_006","B08134_007","B08134_008","B08134_009","B08134_010",
                 "B23025_001", "B23025_002", "B23025_003", "B23025_004","B23025_005", "B23025_006", "B23025_007")
myVarName <- c("Total Population",
               "CT_Total","CT_<10", "CT_10-14", "CT_15-19","CT_20-24","CT_25-29", "CT_30-34", "CT_35-44","CT_45-59", "CT_>60",
               "ES_Total", "ES_Total_labor_force", "ES_Civilian_labor_force", "ES_Civilian_labor_force_employed", "ES_Civilian_labor_force_unemployed", "ES_Armed_Forces", "ES_Not_in_labor_force")
acs_block_group_2020_df <- get_ACS_block_group_allState_2020(myVariables, myVarName)
 
acs_2020_df <- merge_ACS_2020(acs_tract_2020_df, acs_block_group_2020_df)
#----------------------------------------------------------------------------------------------------

# Authenticate
drive_auth()

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
