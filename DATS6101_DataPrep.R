# Data preparation and cleaning for the final project team2
# We worked with four datasets: CDC, ACS, Census Relationship, and County Economic Impact Index (CEII). 
# The CDC dataset contained health measures of interest, while the ACS dataset contained socioeconomic data.
# The Census Relationship dataset contained the geographic relationship between tracts and block groups, 
# and the CEII dataset provided county-level economic data. 
# These datasets were merged on GEOID and CountyFIPS to create the final dataset. 
# The team performed some exploratory data analysis and discovered that the final dataset contains data 
# from 51 states, 1968 counties, and 60944 tracts. 
# We will be using this final dataset for their analysis.

#-------------------------------------------------------------------------------------
# import libraries

library(readr)
library(tidyr)
library(dplyr)
library(googledrive)
# install.packages("tidycensus")
library(tidycensus)
library(tidyverse)
library(tigris)
library(purrr)
library(censusapi)

#-------------------------------------------------------------------------------------
# Data set # 1: CDC

# Import CDC data frame object
# !!!!You need to change this part!!!! You need to write local path of original CDC data set
chronic_df <- read_csv("Documents/GW_Univ/SP2023/DATS6101_Intro_to_Data_Science/DATS6101_Final_Project/PLACES/PLACES__Local_Data_for_Better_Health__Census_Tract_Data_2022_release.csv")
# Keep only some variables #
chronic_df <- chronic_df[(chronic_df$Year == 2020),]
chronic_df <- chronic_df[,c("Year","StateAbbr","StateDesc","CountyName","CountyFIPS","LocationName","Data_Value","TotalPopulation","MeasureId")]
# Keep only rows with health measures of interest #
chronic_df <- chronic_df[(chronic_df$MeasureId=="DEPRESSION" | chronic_df$MeasureId=="SLEEP" | chronic_df$MeasureId=="LPA" | chronic_df$MeasureId=="MHLTH" | chronic_df$MeasureId=="BINGE"),]
# Reshape long to wide #
CDC_df <- spread(chronic_df, key = MeasureId, value = Data_Value)
# Rename LocationName and into GEOID
colnames(CDC_df)[6] <- "GEOID"

#--------------------------------------------------------------------------------------
# Data set # 2: ACS

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
myVarName <- c('MT_Total', 'MT_Never Married', 'MT_Now married', 'MT_Divorces', 'MT_Separated', 'MT_Widowed',
               'EA_Total', 'EA_Less than high school graduate','EA_High school graduate',"EA_college or associate's degree","EA_Bachelor's degree", 'EA_Graduate or professional degree',
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
# Data set #3: Census Relationship (geographical data)

# Import and cleaning Census Relationship File online
import_Census_Relationship <- function(){
  data <- read.table("https://www2.census.gov/geo/docs/maps-data/data/rel2020/blkgrp/tab20_blkgrp20_blkgrp10_natl.txt", sep = "|", header = T)
  
  # Select the columns we interest
  data <- data[, 2:5]
  
  # Convert Block-level to Tract-level to merge lows on same GEOID
  colnames(data)[1] <- c("GEOID")  
  
  # Check for duplicates in the GEOID column
  duplicate_rows <- duplicated(data$GEOID)
  # Drop the duplicated rows
  data <-data[!duplicate_rows,]
  
  # Delete the last character and add 0 at the first of a string in GEOID column
  data$GEOID <- substr(data$GEOID, 1, nchar(data$GEOID) - 1)
  # Assuming the dataframe is called 'df' and the GEOID column is called 'GEOID'
  data$GEOID <- ifelse(nchar(data$GEOID) == 10, paste0("0", data$GEOID), data$GEOID)
  
  # Drop BLKGRP_20 column
  data <- data[,-2]
  
  colnames(data)[2:3] <- c("AreaLand", "AreaWater")
  
  # Aggregate by GEOID and sum the AreaLand and AreaWater columns
  data <- aggregate(cbind(AreaLand, AreaWater) ~ GEOID, data = data, sum)
  
  return(data)
}

CR_df <-import_Census_Relationship()

#----------------------------------------------------------------------------------------------------
# Data set #4: County Economic Impact Index 

# We are here trying to download the data set from the google drive

# Authenticate with your Google account
drive_auth()

# Define the name or URL of the shared drive
shared_drive_url <- "https://drive.google.com/drive/u/3/folders/1ipCZSd5Ede9GxNQIyoMlIelOXKJ-UhVI"

# Get the ID of the shared drive
shared_drive_id <- as_id(shared_drive_url)

# List the contents of the shared drive
drive_ls(shared_drive_id)

file_id <- "12_ZB6QSB2RlX8f6kQ1VB0ECaBmh34R1R"
drive_download(as_id(file_id), overwrite = TRUE)
ceii_df <- read_excel("CEII Data 20220919.xlsx",  sheet = "econ index")
ceii_df <- ceii_df %>% select(-matches("2[12]")) # Deleting columns including 21 and 22 year data

# Adding columns: va_mean, pcEmpAct_mean, and index_mean which are calculated ftom jan20 to dec20
ceii_df <- ceii_df %>%
  select(-va_base) %>% # Drop va_base column
  mutate(va_mean = rowMeans(across(starts_with("va"))))

ceii_df <- ceii_df %>%
  mutate(pcEmpAct_mean = rowMeans(across(starts_with("pcEmpAct"))))

ceii_df <- ceii_df %>%
  mutate(index_mean = rowMeans(across(starts_with("index"))))

colnames(ceii_df)[1] <- "CountyFIPS"

#-------------------------------------------------------------------------------------------------------------

# Merge ACS, Census Relationship, and CEII into CDC dataset
# ACS and Census Relationship is merged on GEOID(tract level)
# On the other hand, CEII is merges on CountyFIPs, since CEII is conducted by county level

# Function to merge ACS, CR, CEII into CDC on GEOID(tract level)
merge_all_df <- function(CDC, ACS, CR, CEII) {
  
  df <- merge(ACS, CR, by=c("GEOID"))

  df <- merge(CDC, df[, -which(names(df)=='NAME')], by=c('GEOID'))
  
  df <- merge(df, CEII[, -which(names(CEII) == 'state' | names(CEII) == 'county')], by=c("CountyFIPS"))
  
  return(df)
}

# Final Version of data set (Still needed manipulated each column to make meaningful column)
final_df <- merge_all_df(CDC_df, acs_2020_df, CR_df, ceii_df)

#-------------------------------------------------------------------------------------------------------------

# quick EDA before exporting csv file stored on GitHub and using rmd file.
# Count the number of unique names
length(unique(final_df$StateAbbr))

table(final_df$StateAbbr)
table(final_df$CountyName)
table(final_df$GEOID)

# Count missing values in each column
colSums(is.na(final_df))

#-------------------------------------------------------------------------------------------------------------

# Covert Each dependent variables to percentages(MT, EA, ES, CT)
convert_percent_pop <- function(df, prefix) {
  df <- df %>%
    mutate(across(starts_with(prefix), ~ . / get(paste0(prefix, "_Total")) * 100))
  return(df)
}

new_final_df <- convert_percent_pop(final_df, "MT")
new_final_df <- convert_percent_pop(new_final_df, "EA")
new_final_df <- convert_percent_pop(new_final_df, "ES")
new_final_df <- convert_percent_pop(new_final_df, "CT")

# Export final_version of code to store on GitHub repo
# code below  

# Multiple Linear Regression
# y^ = b_0 + b_1*MT + b_2*EA + b_3*ES + b_4*CT + b_5*Binge + b_6*LPA + b_7*SLEEP + b_8*MI + b_9*popDense + b_10*EconIndex

