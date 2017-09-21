
#################################
######### LTV R Script  #########
#################################

# Created Date: August 30, 2017 
# Purpose: adjust D90 LTV script 

# Notes: 
# this script updates the Pre_D90_2_Josh.R script that Nebo gave me 
# it incorporates the grouping levels that Eric asked for, and keeps the overall daily level Josh had

#################################
######## Load Packages ##########
#################################

require(bigrquery)
require(binhf)
require(plyr)
require(dplyr)
require(scales)
require(flexdashboard)
require(drc)
require(nlme)
library(ggplot2)
library(forecast)
library(xlsx)
library(tibble)
library(formattable)
# library(lubridate)

setwd("/Users/andrewtynan/Desktop") # need to create a project for this work 
source('helpers.R')
source('query_body.R') #moved the main portion of query to seperate file
n_day_chunk_list <- character() # placeholder char vector to enable importing source('query_body_vars.R') 
source('query_body_vars.R') # added variables for the joindates 

#################################
######## BQ Connection ##########
#################################

# connect to the BigQuery project 
project <- '610510520745'
args <- commandArgs(trailingOnly = TRUE)
set_service_token(args[1])

#################################
########## GET DATES  ###########
#################################

# get the date range used for the ltv cohorts
  # this is used for the lifetime (eg ARPU from install date to Dn)
# date_range <- "SELECT DATEDIFF(CURRENT_DATE(),'2016-10-01')"
date_range <- "SELECT DATEDIFF(CURRENT_DATE(), DATE(DATE_ADD(CURRENT_DATE(), -7, 'DAY')))"
date_range_query_execute <- query_exec(date_range, project = project)
cohort_days <- date_range_query_execute[1,1] 

# to deal with memory issues in BQ 
  # create n day increments for the entire date range 
    # loop over and run queries in these chuncks 
      # rbind dataframes togeter to get all daily cohorts 

# get list of dates 
  # this is used for the install cohort days, to split the data into smaller sizes to run
date_list <- list()
date_list <- seq(as.Date("2017-08-28"), as.Date(format(Sys.Date()-1, format = "%Y-%m-%d")), by="days")
# list to store n day chuncks
rm_if_exists(n_day_chunk_list)
n_day_chunk_list <- list()
# create n day chuncks 
  # NOTE set n by dividing date_list (eg by 7)
n_day_chunk_list <- split(date_list, ceiling(seq_along(date_list)/7))
# update the list by collapsing elements of cday chunck lists and adding commas
  # then insert each n day chunck back into n_day_chunk_list (overwrites)
for(daily_cohort in seq_along(n_day_chunk_list)) {
  n_day_chunk <- paste(n_day_chunk_list[[daily_cohort]], collapse = ',')
  n_day_chunk_list[[daily_cohort]] <- n_day_chunk
  # print(n_day_chunk) # for testing 
}  

n_day_chunk_list #check 

#################################
##### CREATE SQL CONDITIONS #####
#################################

# creates the first non-grouping field in select statement
select_1_syntax <- function(metrics) {
  if(metrics=='Daily_ARPU'){  #if this is the list element name from the levels list 
    select_1 <- paste("SUM(CASE WHEN playerage <= 0 AND DATEDIFF(max_date,joindate) >= 0 THEN net_revenue ELSE NULL END) AS d0_ARPU,",sep='')   
  } else {
    if(metrics=='UA_Daily_Retention'){
      select_1 <- paste("EXACT_COUNT_DISTINCT(CASE WHEN playerage = 0 AND channel != 'organic' THEN gamerid ELSE NULL END) AS installs,",sep='')  
    } else {
      if(metrics=='DAU'){ # need to add if condition for this below, it uses a different body query 
                            # select_2 for this one is just empty quotes, there are no date variables 
        select_1 <- paste("EXACT_COUNT_DISTINCT(gamerid) AS DAU FROM [n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary] WHERE date >= '2016-10-01'GROUP BY 1 ORDER BY 1",sep='')  
      } else {
        if(metrics=='Organic_Installs'){  # this is a different format of query and is in a seperate source file, notes in section below
                                            # select_2 for this one is just empty quotes, there are no date variables 
                                              # there is no dates variable in Organic Installs, just the count by levels
          select_1 <- paste("EXACT_COUNT_DISTINCT(CASE WHEN playerage = 0 AND channel = 'organic' THEN gamerid ELSE NULL END) AS installs,",sep='')  
        } else {
          if(metrics=='Organic_Revenue'){
            select_1 <- paste("SUM(CASE WHEN playerage = 0 AND (channel = 'organic' ) AND DATEDIFF(max_date,joindate) >= 0 THEN revenue ELSE NULL END) AS d0_ARPU,",sep='')  
          } else {
            if(metrics=='UA_Spend'){
              select_1 <- paste("SUM(spend) AS spend",sep='')  #this is a different format of query
            } else {
              if(metrics=='UA_Revenue') {
                select_1 <- paste("SUM(CASE WHEN playerage <= 0 AND (channel != 'organic') AND DATEDIFF(max_date,joindate) >= 0 THEN revenue ELSE NULL END) AS d0_ARPU,",sep='')  
              }
            }
          }
        }
      }
    }
  }
  return(select_1) 
}

# creates the first non-grouping field in select statement
select_2_syntax <- function(metrics) {
  if(metrics=='Daily_ARPU'){  #if this is the list element name from the levels list 
    select_2_txt_vars <- paste("SUM(CASE WHEN playerage <= ",liftime_days," AND DATEDIFF(max_date,joindate) >= ",liftime_days," THEN net_revenue ELSE NULL END) AS d",liftime_days,"_ARPU,",sep='')   
  } else {
    if(metrics=='UA_Daily_Retention'){
      select_2_txt_vars <- paste("EXACT_COUNT_DISTINCT(CASE WHEN playerage = ",liftime_days," AND channel != 'organic' THEN gamerid ELSE NULL END) AS d",liftime_days,",",sep='')
    } else {
      if(metrics=='DAU'){  
        select_2_txt_vars <- paste("",sep='')  #no date variables, so just add empty string
      } else {
        if(metrics=='Organic_Installs'){       #no date variables, so just add empty string
          select_2_txt_vars <- paste("",sep='')  
        } else {
          if(metrics=='Organic_Revenue'){
            select_2_txt_vars <- paste("SUM(CASE WHEN playerage <= ",liftime_days," AND (channel = 'organic' ) AND DATEDIFF(max_date,joindate) >= ",liftime_days," THEN revenue ELSE NULL END) AS d",day,"_ARPU,",sep='')  
          } else {
            if(metrics=='UA_Spend'){
              select_2_txt_vars <- paste("",sep='')  #no date variables, so just add empty string
            } else {
              if(metrics=='UA_Revenue'){  
                select_2_txt_vars <- paste("SUM(CASE WHEN playerage <= ",liftime_days," AND (channel != 'organic' ) AND DATEDIFF(max_date,joindate) >= ",liftime_days," THEN revenue ELSE NULL END) AS d",day,"_ARPU,",sep='')  
              }
            }
          }
        }
      }
    }
  }
  return(select_2_txt_vars) 
}

# need to create a body query section 
  # some of the queries do not use the query_body from source('query_body.R')
  # need to put these other body queries in other source files or maybe here as strings 
body_syntax <- function(metrics) {
  if(metrics=='Daily_ARPU'){  #if this is the list element name from the levels list 
    body_syntax_vars <- query_body_vars # from SOURCE(query_body_vars.R) which has the variable conditions
  } else {
    if(metrics=='UA_Daily_Retention'){   
      body_syntax_vars <- query_body_vars # from SOURCE(query_body_vars.R)     
    } else {
      if(metrics=='DAU'){                 # DAU body and (removed the group by)
        body_syntax_vars <- paste("FROM [n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary] WHERE date >= '2016-10-01'",sep='')  #no date variables, so just add empty string
      } else {      
        if(metrics=='Organic_Installs'){       #need to find what goes here 
          body_syntax_vars <- paste("",sep='')  
        } else {      
          if(metrics=='Organic_Revenue'){     #need to find what goes here 
            body_syntax_vars <- paste("",sep='') 
          } else {
            if(metrics=='UA_Spend'){     # DAU body and (removed the group by)
                                            # I THINK ERIC SAID THAT WE DO NOT HAVE SPEND AT GRANULAR LEVELS 
                                                # NEED TO CHECK THIS
                                                # ALSO PROBABLY NEED TO ADD 
              body_syntax_vars <- paste("FROM (SELECT date, campaign_name, ad_network, platform, spend FROM [n3twork-marketing-analytics:INSTALL_ATTRIBUTION.tenjin_summary] WHERE date >= '2016-10-01'AND bundle_id = 'com.n3twork.legendary'GROUP BY 1, 2, 3, 4, 5 ORDER BY 1)",sep='') 
            } else {
              if(metrics=='UA_Revenue'){     #need to find what goes here 
                body_syntax_vars <- paste("",sep='')  
              }
            }
          }
        }
      }
    }
  }
  return(body_syntax_vars) 
}

# creates the group by and order by 
query_group_syntax <- function(num) {
  if(names(levels_list[num])=='L1'){  # if this is the list element name from the levels list 
    query_group_by <- paste("GROUP BY 1 ORDER BY 1",sep='')   
  } else {
    if(names(levels_list[num])=='L2'){
      query_group_by <- paste("GROUP BY 1,2,3 ORDER BY 1,2,3",sep='')  
    } else {
      if(names(levels_list[num])=='L3'){
        query_group_by <- paste("GROUP BY 1,2,3,4 ORDER BY 1,2,3,4",sep='')  
      } else {
        if(names(levels_list[num])=='L4'){
          query_group_by <- paste("GROUP BY 1,2,3 ORDER BY 1,2,3",sep='')  
        }
      }
    }
  }
  return(query_group_by) 
}

# there are N metrics pulls (eg Daily ARPU, UA DAILY RETENTION, etc.)
metrics <- list("Daily_ARPU"
                #add these levels as I go 
                ,"UA_Daily_Retention"
                # ,"DAU"
                # ,"Organic_Installs"
                # ,"Organic_Revenue"
                # ,"UA_Spend"        # might need to account for install percentages across grouping levels ???
                # ,"UA_Revenue"      # might need to account for install percentages across grouping levels ???
)

# there are 4 grouping combinations 
  # Eric said platform is implicit by campaign, but I have added it to be safe 
levels_list = list(L1 = c("joindate")
                   ,L2= c("joindate, country, platform")
                   ,L3 = c("joindate, country, platform, channel")
                   ,L4 = c("joindate, platform, campaign_name")
)

#################################
##### CREATE & STORE QUERIES ####
################################# 

# create lists to store queries (remove if they exists; )
rm_if_exists(queries_list) 
queries_list <- list() 

rm_if_exists(metrics_queries_list)
metrics_queries_list <- list()

# when it is Daily_ARPU
  #for date_chunck in n_day_chunk_list
    #insert date_chunck into query_body_vars 

# body_syntax <- function(metrics) {
#   if(metrics=='Daily_ARPU'){  #if this is the list element name from the levels list 
#     body_syntax_vars <- query_body_vars # from SOURCE(query_body_vars.R) which has the variable conditions
#   } else {

# query_maker <- function(,  ) {

for(date_chunck in n_day_chunk_list) {
  
  for(n_day_chunk in n_day_chunk_list) {
    print(DL)
  }

# create a version of the query for each level of granularity
for(kpi in metrics) {
  metrics_name <- kpi  
  # metrics_name <- paste(c(kpi, "_metrics"), collapse = "") # create name obj for each query
  print(metrics_name) # check 
  queries_list[[metrics_name]] = metrics_queries_list 
  for(num in seq_along(levels_list)) {  
  for(date_chunck in n_day_chunk_list) { # create corresponding D1 to Dn incremental periods as    
    lvls <- levels_list[num] 
    levels_text <- lvls[[1]] # create query_group_by based on grouping levels; NOTE: the [[1]] is indexing the values [1] would get names 
    select_levels <- paste("SELECT ",levels_text,",",sep='') # insert the groupings variables from levels
    select_1 <- select_1_syntax(kpi) # create D0 incremental period
    select_2 <- character() # chr vector to store D1 to Dn incremental periods as
    for(liftime_days in 1:cohort_days) { # create corresponding D1 to Dn incremental periods as
      select_2[liftime_days] <- select_2_syntax(kpi)
    }  
    select_2 <- paste(select_2,collapse = " ") # string formating
    # old way to get query_body, for one time period rather than several 
    query_body <- paste(query_body, collapse = " ")  # clean-up string space formatting
    query_body <- gsub("[\r\n]", " ", query_body)   # convert \r carriage returns and \n new lines to spaces in text string from source('query_body.R')
    # 
    query_body[XXX] <- gsub("[\r\n]", " ", body_syntax(metrics))   # convert \r carriage returns and \n new lines to spaces in text string from source('query_body.R')
    query_body <- paste(query_body, collapse = " ")  # clean-up string space formatting
    query_group_by <- query_group_syntax(num) # create query_group_by based on grouping levels 
    query <- paste(select_levels,  
                   select_1,
                   select_2,
                   query_body,
                   query_group_by,
                   sep = ' ')
    print(paste(c(kpi, "_", gsub(", ", "_", lvls[[1]]), "_query"), collapse = "")) # check
    # names(query) <- paste(c(kpi, "_", gsub(", ", "_", lvls[[1]]), "_query"), collapse = "") 
    # names(metrics_queries_list[[query]]) <- paste(c(kpi, "_", gsub(", ", "_", lvls[[1]]), "_query"), collapse = "") # create name for each query    
    metrics_queries_list[[num]] = query  # insert query  
  }
  queries_list[[kpi]] <- metrics_queries_list  # insert 
  # names(queries_list[kpi]) <- paste(c(kpi, "_", gsub(", ", "_", lvls[[1]]), "_query"), collapse = "") # create name for each query
}


# checks   
queries_list %>% length()
#check names of metric lists within queries list 
names(queries_list[1]); names(queries_list[2])
#check size of metric lists within queries list 
queries_list[[1]] %>% length()
queries_list[[2]] %>% length()
#check contents of metric lists within queries list 
queries_list[1][[1]][1]
queries_list[1][[1]][2]
queries_list[1][[1]][3]
queries_list[2][[1]][1]
queries_list[2][[1]][2]
queries_list[2][[1]][3]
#check names of queries in queries list in metrics list 
names(queries_list[1][[1]]); names(queries_list[2][[1]])

# check query contents NOTE: these are not used, they are added into queries list (just staging)
metrics_queries_list %>% length()
metrics_queries_list[1]
metrics_queries_list[2]
names(metrics_queries_list[1]); names(metrics_queries_list[2])  

#################################
##### RUN QUERIES & GET DATA ####
################################# 

rm_if_exists(datasets_list)
datasets_list <- list() #for storing datasets from running queries 

rm_if_exists(metrics_datasets_list)
metrics_datasets_list <- list() 

# run query for each of the granularity grouping levels 
for(metric in seq_along(queries_list)) { 
  # print(names(queries_list[metric])) # check 
  # metrics_name <- metrics[metrics]
  datasets_name <- names(queries_list[metric]) # get metric name
  datasets_list[[datasets_name]] = metrics_datasets_list  # name list based on metric
  for(qry in seq_along(queries_list[[metric]])) {
    # print(lvls)  #check 
    #NOTE: 1st [] is outer list, 2nd [] is inner list, 3rd [] is inner list contents (eg query)
      # 2nd [] is always 1 since we stop at the inner list, and the get each of the queries 
      # 1st [] & 3rd [] depend are # of metrics and levels 
    query <- queries_list[metric][[1]][qry]
    # print(query) #check 
    metrics_datasets_list[[qry]] <- query_exec(as.character(query), project = project) # execute query to get dataset
  }  
  datasets_list[[metric]] <- metrics_datasets_list
} 

# checks   
datasets_list %>% length()
#check names of metric lists within dataset list 
names(datasets_list[1]); names(datasets_list[2])
#check size of metric lists within dataset list 
datasets_list[[1]] %>% length()
datasets_list[[2]] %>% length()
#check contents of metric lists within queries list 
datasets_list[1][[1]][1]
datasets_list[1][[1]][2]
datasets_list[1][[1]][3]
datasets_list[1][[1]][4]
datasets_list[2][[1]][1]
datasets_list[2][[1]][2]
datasets_list[2][[1]][3]
datasets_list[2][[1]][4]
#check names of dataset in (the renamed) metrics_datasets_list in datasets_list 
names(datasets_list[1][[1]]); names(datasets_list[2][[1]])

# Warning message:
#   Only first 10 pages of size 10000 retrieved. Use max_pages = Inf to retrieve all. 

#################################
######### MERGE DATASETS ########
#################################

# due to memory errors the data sets will need to be merged after they are pulled 

# NEED TO CREATE THE CHECKS 
  # sample smaller 100 
  # label these as NA 

# need to add where clause to to check for net_revenue > 0
# this should remove all NA ARPU, but might also want to add a having clause to be sure

# in the next queries, need to add a where clause for install cohort < 100 (verify with Eric)

#################################
##### CREATE RETENTION CURVE ####
#################################

##### here is FIRST post-processing 
  ## need to double check and make sure there was no calculation or post processing after Daily_ARPU data pull 
retention_curve_sums_list <- list()

for(metric in seq_along(datasets_list)) print(names(datasets_list[metric])) # check

for(metric in seq_along(datasets_list)) {
 if(names(datasets_list[metric])=="UA_Daily_Retention") { # seems like no else condition is needed 
   for(levels in seq_along(levels_list)) { 
     retention_curve_name <- gsub(", ", "_", levels_list[1]) # create name
     datasets_list[[retention_curve_name]] = retention_curve_sums_list  # name list based on metric 
     print(name)  # check 
     
     data <- datasets_list[metric][[1]][levels]
   
     x <- data[,c(2:ncol(data))] # captures data only
     x[2:nrow(x),1] <- 0 # overwrites unneeded installs 
     ##### pivots for retention curve
     mat <- as.matrix(x)
     for (position in 3:nrow(mat)) {
       mat[i,] <- shift(mat[position,],position-2)
     }
     matrix_size <- nrow(mat) - 1  # pivoted matrix
     retention_curve <- mat[c(1:matrix_size),c(1:matrix_size)]
     dates <- c(data[c(1:(nrow(data)-1)),1])  # captures date range
     colnames(retention_curve) <- dates # reassigns retention curve to dates
     retention_curve_sums <- colSums(retention_curve) # finds retention curve sums 
     retention_curve_sums <- as.data.frame(retention_curve_sums) # assigns as a dataframe
     retention_curve_sums$ua_installs <- data[1:nrow(retention_curve_sums),2]
     }
   retention_curve_sums_list[[metric]] <- retention_curve_sums
  } 
}

##################################
############ GET DAU  ############
##################################

## NOW THIS IS DONE ABOVE 
  # SHOULD NOT NEED TO DO ANYTHING HERE, BUT DOUBLE CHECK 

#### dau sql
dau_sql <-"SELECT date, EXACT_COUNT_DISTINCT(gamerid) AS DAU FROM [n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary] WHERE date >= '2016-10-01'GROUP BY 1 ORDER BY 1"
dau_query_execute <- query_exec(dau_sql, project = project)

dau_data <- dau_query_execute

##################################
######## ORGANIC INSTALLS ########
##################################

## DATA PULL HAPPENS ABOVE 
  ## DOUBLE CHECK 

#### organic install data
  #moved to source('org_install_query.R')  
org_install_query_execute <- query_exec(org_install_query, project = project)

org_install_data <- org_install_query_execute

##### here is SECOND post-processing 
retention_curve_sums_list <- list()

for(metric in metrics_dataset_list) {
  if(names(datasets_list[metric])=="UA_Daily_Retention") { # seems like no else condition is needed   
    for(levels in seq_along(levels_list)) {     
      retention_curve_name <- gsub(", ", "_", levels_list[1]) # create name
      datasets_list[[retention_curve_name]] = retention_curve_sums_list  # name list based on metric 
      print(name)  # check 
      
      ###### orgdau calculation
      orgdau <- org_install_data$installs/ dau_data$DAU
      orgdau <- orgdau[1:(length(orgdau)-1)]
      ###### merging into ua retention curve sums
      retention_curve_sums$orgdau <- orgdau
      retention_curve_sums$org_installs_from_ua <- round(retention_curve_sums$orgdau*retention_curve_sums$retention_curve_sums,digits = 0)
      ##### calculations for ua percentage of organic revenue
      retention_curve_sums$org_installs <- org_install_data[,2][1:(nrow(retention_curve_sums))]
      retention_curve_sums$ua_perc_for_rev <- retention_curve_sums$org_installs_from_ua / retention_curve_sums$org_installs
    }
  retention_curve_sums_list[[metric]] <- 
  }
}  

##################################
###### GET ORGANIC REVENUE #######
##################################

## DATA PULL HAPPENS ABOVE 
  ## DOUBLE CHECK 

select_org_rev <- "SELECT joindate as date, SUM(CASE WHEN playerage = 0 AND (channel = 'organic' ) AND DATEDIFF(max_date,joindate) >= 0 THEN revenue ELSE NULL END) AS d0_ARPU,"

body_org_rev <- character()

for(i in 1:cohort_days) {
  body_org_rev[i] <- paste("SUM(CASE WHEN playerage <= ",i," AND (channel = 'organic' ) AND DATEDIFF(max_date,joindate) >= ",i," THEN revenue ELSE NULL END) AS d",i,"_ARPU,",sep='')
}

##### here is THIRD post-processing 

for(metric in metrics_dataset_list) {
    <- list()
    for(dataset in datasets_list) {

      body_org_rev <- paste(body_org_rev,collapse = " ")
      #replaced object 'end_org_rev' with reference to source('query_body.R')
      org_rev_sql <- paste(select_org_rev,body_org_rev,query_body,sep = ' ')
      org_rev_query_execute <- query_exec(org_rev_sql, project = project)
      org_rev_data <- org_rev_query_execute

    }
  }
}

##################################
######### GET UA SPEND ###########
##################################

## NOTE SURE IF THIS EXISTS AT LOWER LEVELS, NEED TO CHECK 

## DATA PULL HAPPENS ABOVE 
  ## DOUBLE CHECK 

spend_sql <- "SELECT date, SUM(spend) AS spend FROM (SELECT date, campaign_name, ad_network, platform, spend FROM [n3twork-marketing-analytics:INSTALL_ATTRIBUTION.tenjin_summary] WHERE date >= '2016-10-01'AND bundle_id = 'com.n3twork.legendary'GROUP BY 1, 2, 3, 4, 5 ORDER BY 1) GROUP BY 1 ORDER BY 1"
spend_query_execute <- query_exec(spend_sql, project = project)
spend_data <- spend_query_execute

##### here is FOURTH post-processing 

for(metric in metrics_dataset_list) {
     <- list()
    for(dataset in datasets_list) {

      #### pairs down to match size and only cohorts that have a posted d7
      retention_curve_sums <- retention_curve_sums[1:(nrow(retention_curve_sums)-6),]
      organic_revenue_date_match <- org_rev_data[1:nrow(retention_curve_sums),]
      
      spend_data_date_match <- spend_data[1:nrow(retention_curve_sums),]
      retention_curve_sums$spend <- spend_data_date_match$spend
      retention_curve_sums$aCPI <- retention_curve_sums$spend / (retention_curve_sums$ua_installs + retention_curve_sums$org_installs_from_ua)
      retention_curve_sums$eCPI <- retention_curve_sums$spend / (retention_curve_sums$ua_installs + retention_curve_sums$org_installs)
      ##### calculates revenue from organic revenue from ua_contribution
      percent_ua_contribution <- retention_curve_sums$ua_perc_for_rev
      organic_revenue_ua_contribution <- organic_revenue_date_match[,2:ncol(organic_revenue_date_match)]
      #### for aCPI
      # organic_revenue_ua_contribution <- organic_revenue_ua_contribution*percent_ua_contribution
      ###### for eCPI
      organic_revenue_ua_contribution <- organic_revenue_ua_contribution
      ##### adds back in dates
      dates <- organic_revenue_date_match[,1]
      organic_revenue_ua_contribution <- cbind(dates,organic_revenue_ua_contribution)
    }
    
}

##################################
######## GET UA REVENUE ##########
##################################

## DATA PULL HAPPENS ABOVE 
  ## DOUBLE CHECK 

#### query exectute for ua revenue
select_ua_rev <- "SELECT joindate AS date, SUM(CASE WHEN playerage <= 0 AND (channel != 'organic') AND DATEDIFF(max_date,joindate) >= 0 THEN revenue ELSE NULL END) AS d0_ARPU,"
body_ua_rev <- character()
for(i in 1:cohort_days) {
  body_ua_rev[i] <- paste("SUM(CASE WHEN playerage <= ",i," AND (channel != 'organic' ) AND DATEDIFF(max_date,joindate) >= ",i," THEN revenue ELSE NULL END) AS d",i,"_ARPU,",sep='')
}
body_ua_rev <- paste(body_ua_rev,collapse = " ")

#replaced object 'end_ua_rev' with reference to source('query_body.R')
ua_rev_sql <- paste(select_ua_rev,body_ua_rev,query_body,sep = ' ')
ua_rev_query_execute <- query_exec(ua_rev_sql, project = project)
ua_rev_data <- ua_rev_query_execute 

##### here is FIFTH post-processing 

for(metric in metrics_dataset_list) {
  retention_curve_sums <- list()
  for(dataset in datasets_list) {
    #### matches date range for cohorts with 7 days baked in
    ua_revenue_date_match <- ua_rev_data[1:nrow(retention_curve_sums),]
    #### blends ua revenue and organic revenue contribution
    total_ua_revenue <- organic_revenue_ua_contribution[,2:ncol(organic_revenue_ua_contribution)] + ua_revenue_date_match[,2:ncol(ua_revenue_date_match)]
    total_ua_revenue <- cbind(dates,total_ua_revenue)
    #### retrieves total ua installs with k-factor for aCPI
    # total_ua_installs <- retention_curve_sums$ua_installs + retention_curve_sums$org_installs_from_ua
    total_ua_installs <- retention_curve_sums$ua_installs + retention_curve_sums$org_installs  # for eCPI 
    total_arpi <- total_ua_revenue[,2:ncol(total_ua_revenue)] / total_ua_installs # calculates arpi
    df <- data.frame(matrix(NA, ncol = 366 - ncol(total_arpi), nrow = nrow(total_arpi))) # creates extended forecasting range
    df_column_names <- character()
    for(i in ncol(total_arpi):365) {
      df_column_names[i] <- paste("d",i,"_ARPU",sep='')
    }
    df_column_names <- df_column_names[!is.na(df_column_names)]
    colnames(df) <- df_column_names
    length(df_column_names)
    total_arpi <- cbind(dates,total_arpi,df)
    ##### transposes cohorts to dataframe in order to support prediction formats
    total_arpi_transposed <- t(total_arpi)
    cohort_days <- c(0:365)
    colnames(total_arpi_transposed) <- total_arpi_transposed[1, ]
    total_arpi_transposed <- as.data.frame(total_arpi_transposed[-1,])  
    total_arpi_transposed <- cbind(cohort_days,total_arpi_transposed)

  }
}

# dim(total_arpi_transposed)
# head(total_arpi_transposed[c(1:ncol(total_arpi_transposed)),c(1:5)])

##################################
####### SSgompertz MODELS ########
##################################

#### models the data for each cohort
for (i in 2:ncol(total_arpi_transposed)) {
  a <- as.numeric(levels(total_arpi_transposed[,i]))
  a <- sort(a)
  b <- total_arpi_transposed[1:length(a),1]
  prediction_set <- cbind(b,a)
  prediction_set <- as.data.frame(prediction_set)
  assign(paste("fit",i,sep=''),
         nls(a ~ SSgompertz(log(b+1), A, B, C) ,
             data = prediction_set,
             start = list(A=40000,B=11.8,C=.92),
             upper = list(A=40000,B=13,C=.93),
             lower = list(A=40000,B=11.0,C=.90),
             algorithm = "port",
             control = nls.control(maxiter = 500000,tol = 1e-09,warnOnly=T))
  )
}

finished_matrix <- as.matrix(total_arpi_transposed,stringsAsFactors= F)

#### pulls model coefficients and stores each cohort to a dataframe
model.list<-mget(grep("fit[0-9]+$", ls(),value=T))
coefs<-lapply(model.list,function(x)coef(x))
flattened_coefs <- as.data.frame(unlist(coefs))
colnames(flattened_coefs) <- c('coefficients')
flattened_coefs$index <- row.names(flattened_coefs)
flattened_coefs$index <- as.numeric(gsub('[a-zA-Z.]','',flattened_coefs$index))
flattened_coefs$model_component <- gsub('[a-z0-9.]','',row.names(flattened_coefs))

####### Fills in na's with predicted values
for (z in 2:ncol(finished_matrix)) {
  A <- flattened_coefs[flattened_coefs$index == z & flattened_coefs$model_component == 'A',1]
  B <- flattened_coefs[flattened_coefs$index == z & flattened_coefs$model_component == 'B',1]
  C <- flattened_coefs[flattened_coefs$index == z & flattened_coefs$model_component == 'C',1]
  for (i in 1:nrow(finished_matrix)){
    if (is.na(finished_matrix[i,z])) {
      finished_matrix[i,z] <- A*exp(-B*C^log(as.numeric(finished_matrix[i,1])))
    }
  }
}

##################################
######## REPRT PREPARATION #######
##################################

#### writes out final output
transposed_predictions <- t(finished_matrix)
myData <- as.data.frame(transposed_predictions[-c(1), ], stringsAsFactors = F)
cohort_names <- row.names(myData)
myData <- sapply( myData, as.numeric )
myDataNet <- myData*.7
myData <- as.data.frame(myData)
myDataNet <- as.data.frame(myDataNet)
rownames(myData) <- cohort_names
rownames(myDataNet) <- cohort_names

base_info <- cbind(retention_curve_sums$eCPI,
                   retention_curve_sums$ua_installs + retention_curve_sums$org_installs,
                   myData$d7_ARPU / retention_curve_sums$eCPI,
                   myData$d30_ARPU / retention_curve_sums$eCPI,
                   myData$d90_ARPU / retention_curve_sums$eCPI)
colnames(base_info) <- c('eCPI','Installs','d7 ROI','d30 ROI','d90 ROI')
final_output <- cbind(base_info,myData)


final_output <-  final_output %>%
  arrange(desc(rownames(final_output)))
rownames(final_output) <- rev(cohort_names)
days_to_backout <- NA 
for (i in 1:nrow(final_output)) {
  days_to_backout[i] <- which.min(final_output[i,'eCPI'] > final_output[i,6:ncol(final_output)])
}
days_to_backout
current_yield <- as.numeric(diag(as.matrix(final_output[,13:ncol(final_output)]))) / final_output$eCPI
final_output$current_yield <- current_yield
final_output$days_to_backout <- days_to_backout

final_output[,1] <- dollar_format()(final_output[,1])
final_output[,3] <- percent((final_output[,3]))
final_output[,4] <- percent((final_output[,4]))
final_output[,5] <- percent((final_output[,5]))
final_output$current_yield <- percent(final_output$current_yield)

base_info_net <- cbind(retention_curve_sums$eCPI,
                       retention_curve_sums$ua_installs + retention_curve_sums$org_installs,
                       myDataNet$d7_ARPU / retention_curve_sums$eCPI,
                       myDataNet$d30_ARPU / retention_curve_sums$eCPI,
                       myDataNet$d90_ARPU / retention_curve_sums$eCPI)
colnames(base_info_net) <- c('eCPI','Installs','d7 ROI','d30 ROI','d90 ROI')
final_output_net <- cbind(base_info_net,myDataNet)

final_output_net <-  final_output_net %>%
  arrange(desc(rownames(final_output_net)))
rownames(final_output_net) <- rev(cohort_names)
days_to_backout_net <- NA 
for (i in 1:nrow(final_output_net)) {
  days_to_backout_net[i] <- which.min(final_output_net[i,'eCPI'] > final_output_net[i,6:ncol(final_output_net)])
}
current_yield_net <- as.numeric(diag(as.matrix(final_output_net[,13:ncol(final_output_net)]))) / final_output_net$eCPI
final_output_net$current_yield <- current_yield_net
final_output_net$days_to_backout <- days_to_backout_net

###################################
## BQ TABLES BUILDS & XLSX FILES ##
###################################

bq_table_output <- final_output_net[,c('eCPI','Installs','d7 ROI','d30 ROI','d90 ROI')]
bq_table_output <- bq_table_output %>%
  mutate(date = rownames(bq_table_output))
colnames(bq_table_output) <- c('eCPI','Installs','d7_ROI','d30_ROI','d90_ROI','date')
bq_table_output <- as.data.frame(bq_table_output)

insert_upload_job(project = '167707601509', 
                  dataset = "LEG_Predictions", 
                  table = "eCPI_LTV_total_portfolio",  
                  values = bq_table_output, 
                  write_disposition = "WRITE_TRUNCATE")                 

final_output_net[,1] <- dollar_format()(final_output_net[,1])
final_output_net[,3] <- percent((final_output_net[,3]))
final_output_net[,4] <- percent((final_output_net[,4]))
final_output_net[,5] <- percent((final_output_net[,5]))
final_output_net$current_yield <- percent(final_output_net$current_yield)

setwd("analytics_tools/marketing/Leg_R_Analysis/")
write.xlsx(final_output[,c("eCPI","Installs","days_to_backout","current_yield","d7 ROI","d30 ROI","d90 ROI")], file = 'predictions_eCPI.xlsx', sheetName = 'Gross ARPI Figures', row.names = T)
write.xlsx(final_output_net[,c("eCPI","Installs","days_to_backout","current_yield","d7 ROI","d30 ROI","d90 ROI")], file = 'predictions_eCPI.xlsx', sheetName = 'Net ARPI Figures', row.names = T, append = T)
write.csv(file='total_cohort_revenue.csv', revenue_data, row.names = F)
write.csv(final_output_net, file = 'uncapped_net_roi.csv',row.names = T)



