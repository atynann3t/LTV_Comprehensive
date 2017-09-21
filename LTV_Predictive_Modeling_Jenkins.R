
options(max.print=100000)

require(bigrquery)

project <- '610510520745'
args <- commandArgs(trailingOnly = TRUE)
set_service_token(args[1])

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

getwd()
setwd("/Users/andrewtynan/Desktop")
list.files()

day_num <- 97 # number of days in the past to start the date range for data pull 
date_range <- paste("Select DATEDIFF(CURRENT_DATE(),DATE(DATE_ADD(DATE(current_date()), (",day_num,"*-1), 'DAY')))",sep='') 
date_range_query_execute <- query_exec(date_range, project = project)
cohort_days <- date_range_query_execute[1,1]  

###########
select_revenue <- "SELECT joindate as date, SUM(CASE WHEN playerage <= 0 AND DATEDIFF(max_date,joindate) >= 0 THEN net_revenue ELSE NULL END) AS d0_ARPU,"

body_revenue <- character()

for(i in 1:cohort_days) {
  body_revenue[i] <- paste("SUM(CASE WHEN playerage <= ",i," AND DATEDIFF(max_date,joindate) >= ",i," THEN net_revenue ELSE NULL END) AS d",i,"_ARPU,",sep='')
}

body_revenue <- paste(body_revenue,collapse = " ")

source('end_query.R') # insert day_num into the body of the query 
end_revenue <- query_body # update 

org_revenue <- paste(select_revenue,body_revenue,end_revenue,sep = ' ')
revenue_query_execute <- query_exec(org_revenue, project = project)
revenue_data <- revenue_query_execute

revenue_data %>% dim()
View(revenue_data)

#### ua retention curves
select_ua_retention_curve <- "SELECT joindate AS date, EXACT_COUNT_DISTINCT(CASE WHEN playerage = 0 AND channel != 'organic' THEN gamerid ELSE NULL END) AS installs,"

body_ua_retention_curve <- character()
for(i in 1:cohort_days) {
  body_ua_retention_curve[i] <- paste("EXACT_COUNT_DISTINCT(CASE WHEN playerage = ",i," AND channel != 'organic' THEN gamerid ELSE NULL END) AS d",i,",",sep='')
}

body_ua_retention_curve <- paste(body_ua_retention_curve,collapse = " ")

end_ua_retention_curve <- query_body # update from source('end_query.R') which was already imported above 
sql <- paste(select_ua_retention_curve,body_ua_retention_curve,end_ua_retention_curve,sep = ' ')
query_execute <- query_exec(sql, project = project)  # executes sql
data <- query_execute ###### makes data frame and stores sql output

View(data)
data %>% dim()

##### captures data only
# x <- data[,c(2:ncol(data))]
x <- data[,c(2:92)]  # get 90 days of data, the first two columns are dates and installs 

#### overwrites unneeded installs
x[2:nrow(x),1] <- 0

##### pivots for retention curve
mat <- as.matrix(x)
for (i in 3:nrow(mat)) {
  mat[i,] <- shift(mat[i,],i-2)
}

mat %>% dim()
x[c(1:20),c(1:20)]
mat[c(1:20),c(1:20)]

##### pivoted matrix
matrix_size <- nrow(mat) - 1
retention_curve <- mat[c(1:matrix_size),c(1:matrix_size)]

##### captures date range
dates <- c(data[c(1:(nrow(data)-1)),1])

# dates %>% length()  #check 

#### reassigns retention curve to dates
colnames(retention_curve) <- dates

##### finds retention curve sums
retention_curve_sums <- colSums(retention_curve)

retention_curve_sums %>% length()

### assigns as a dataframe
retention_curve_sums <- as.data.frame(retention_curve_sums)
retention_curve_sums$ua_installs <- data[1:nrow(retention_curve_sums),2]

retention_curve_sums %>% dim()

#### dau sql
# removed WHERE date >= '2016-10-01' and added the same date condition as the rev and install pulls 
dau_sql <- paste("SELECT 
                        date, 
                        EXACT_COUNT_DISTINCT(gamerid) AS DAU 
                      FROM [n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary] 
                   WHERE date BETWEEN DATE(DATE_ADD(DATE(current_date()), (",day_num,"*-1), 'DAY')) AND DATE(DATE_ADD(DATE(current_date()), ((",day_num,"*-1)+90), 'DAY'))
                 GROUP BY 1 
                 ORDER BY 1"
                 ,sep='') 

dau_query_execute <- query_exec(dau_sql, project = project)

dau_data <- dau_query_execute

dau_data %>% dim() #check
View(dau_data)

#### organic install data
# added the date variables here too 
org_install_query <- paste("SELECT
joindate AS date,
EXACT_COUNT_DISTINCT(CASE
WHEN playerage = 0 AND channel = 'organic' THEN gamerid
ELSE NULL END) AS installs,
FROM (
SELECT
base.date AS date,
user_profile.joindate AS joindate,
user_profile.gamerid AS gamerid,
user_profile.country AS country,
user_profile.platform AS platform,
user_profile.channel AS channel,
user_profile.campaign_name_id AS campaign_name_id,
user_profile.campaign_group_id AS campaign_group_id,
user_profile.campaign_name AS campaign_name,
user_profile.publisher AS publisher,
user_profile.creative AS creative,
DATEDIFF(base.date,user_profile.joindate) AS playerage,
DATEDIFF(first_purchase.first_purchase_ts,user_profile.joindate) AS purchase_age,
revenue.revenue AS revenue,
revenue.net_revenue AS net_revenue,
DATEDIFF(guild_joins.guild_join_date,user_profile.joindate) AS guild_age
FROM (
SELECT
joindate,
gamerid,
country,
platform,
CASE
WHEN LOWER(channel) CONTAINS 'vungle' THEN 'vungle'WHEN LOWER(channel) CONTAINS 'twitter' THEN 'twitter'WHEN LOWER(channel) CONTAINS 'tapjoy' THEN 'tapjoy'WHEN LOWER(channel) CONTAINS 'supersonic' THEN 'supersonic'WHEN LOWER(channel) CONTAINS 'motive' THEN 'motive'WHEN LOWER(channel) CONTAINS 'mdotm' THEN 'mdotm'WHEN LOWER(channel) CONTAINS 'google' THEN 'google'WHEN LOWER(channel) CONTAINS 'facebook' THEN 'facebook'WHEN LOWER(channel) CONTAINS 'crossinstall' THEN 'crossinstall'WHEN LOWER(channel) CONTAINS 'chartboost' THEN 'chartboost'WHEN LOWER(channel) CONTAINS 'applovin' THEN 'applovin'WHEN LOWER(channel) CONTAINS 'applift' THEN 'applift'WHEN LOWER(channel) CONTAINS 'applifier' THEN 'unity ads'WHEN LOWER(channel) CONTAINS 'appia' THEN 'appia'WHEN LOWER(channel) CONTAINS 'adcolony' THEN 'adcolony'ELSE LOWER(channel)
END AS channel,
campaign_name_id,
campaign_group_id,
campaign_name,
source_app AS publisher,
creative
FROM (
SELECT
joindate,
gamerid,
country,
IFNULL(platform,os) AS platform,
IFNULL(channel,'organic') AS channel,
campaign_name_id,
campaign_group_id,
campaign_name,
source_app,
creative
FROM (
SELECT
joindate,
gamerid,
country,
platform
FROM (
SELECT
ROW_NUMBER() OVER (PARTITION BY b.gamerid) AS row_number,
b.joindate AS joindate,
b.gamerid AS gamerid,
b.country AS country,
b.platform AS platform
FROM (
SELECT
gamerid,
MIN(joindate) AS min_joindate
FROM
[n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary]
GROUP BY
1) a
LEFT JOIN EACH (
SELECT
joindate,
gamerid,
country_code AS country,
os_system AS platform
FROM
[n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary]
WHERE
joindate = date
GROUP BY
1,
2,
3,
4) b
ON
a.gamerid = b.gamerid
AND a.min_joindate = b.joindate)
WHERE
row_number = 1
AND joindate BETWEEN DATE(DATE_ADD(DATE(current_date()), (",day_num,"*-1), 'DAY')) AND DATE(DATE_ADD(DATE(current_date()), ((",day_num,"*-1)+90), 'DAY'))
) a
LEFT JOIN (
SELECT
custom_user_id,
LOWER(campaign_source) AS channel,
campaign_name_id,
campaign_group_id,
campaign_name,
source_app,
creative,
os
FROM (
SELECT
custom_user_id,
campaign_source,
campaign_name_id,
campaign_group_id,
campaign_name,
source_app,
creative,
install_timestamp,
os,
MIN(install_timestamp) OVER (PARTITION BY custom_user_id) AS min_time
FROM (
SELECT
*
FROM
[n3twork-marketing-analytics:INSTALL_ATTRIBUTION.apsalar_installs]
WHERE
longname = 'com.n3twork.legendary'AND attribution = 'Install'AND LENGTH(custom_user_id) > 2)
GROUP BY
1,
2,
3,
4,
5,
6,
7,
8,
9)
WHERE
install_timestamp = min_time
GROUP BY
1,
2,
3,
4,
5,
6,
7,
8) b
ON
a.gamerid = b.custom_user_id
ORDER BY
1 DESC)) user_profile
LEFT JOIN (
SELECT
date,
gamerid
FROM
[n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary]
WHERE
gamerid IN (
SELECT
gamerid
FROM
[n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary]
WHERE
joindate = date
GROUP BY
1)
GROUP BY
1,
2) base
ON
base.gamerid = user_profile.gamerid
LEFT JOIN (
SELECT
gamerid,
DATE(first_purchase_ts) AS first_purchase_ts
FROM (
SELECT
gamerid,
MIN(date) OVER (PARTITION BY gamerid) AS first_purchase_ts
FROM
[n3twork-legendary-analytics:DAILY_IAPS.iaps_summary_poole]
GROUP BY
gamerid,
date)
GROUP BY
1,
2) first_purchase
ON
user_profile.gamerid = first_purchase.gamerid
LEFT JOIN (
SELECT
DATE(date) AS date,
gamerid,
SUM(iap) AS revenue,
SUM(nrv) AS net_revenue
FROM
[n3twork-legendary-analytics:DAILY_IAPS.iaps_summary_poole]
GROUP BY
1,
2 ) revenue
ON
base.date = revenue.date
AND base.gamerid = revenue.gamerid
LEFT JOIN (
SELECT
gamerid,
guild_join_date
FROM
[n3twork-marketing-analytics:Metrics.first_guild_join_date_LEG] ) guild_joins
ON
user_profile.gamerid = guild_joins.gamerid
WHERE
user_profile.joindate >= '2016-10-01') a
CROSS JOIN (
SELECT
MAX(date) AS max_date
FROM
[n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary]) b
WHERE
a.joindate <= b.max_date
GROUP BY
1
ORDER BY
1"
,sep='') 

org_install_query_execute <- query_exec(org_install_query, project = project)

org_install_data <- org_install_query_execute

dau_data %>% dim()
org_install_data %>% dim()
View(org_install_data)

###### orgdau calculation
orgdau <- org_install_data$installs/ dau_data$DAU
orgdau <- orgdau[1:(length(orgdau)-1)]

orgdau %>%  length()
retention_curve_sums %>% dim()
View(retention_curve_sums)

###### merging into ua retention curve sums
retention_curve_sums$orgdau <- orgdau
retention_curve_sums$org_installs_from_ua <- round(retention_curve_sums$orgdau*retention_curve_sums$retention_curve_sums,digits = 0)

##### calculations for ua percentage of organic revenue
retention_curve_sums$org_installs <- org_install_data[,2][1:(nrow(retention_curve_sums))]
retention_curve_sums$ua_perc_for_rev <- retention_curve_sums$org_installs_from_ua / retention_curve_sums$org_installs

select_org_rev <- "SELECT joindate as date, SUM(CASE WHEN playerage = 0 AND (channel = 'organic' ) AND DATEDIFF(max_date,joindate) >= 0 THEN revenue ELSE NULL END) AS d0_ARPU,"

body_org_rev <- character()

for(i in 1:cohort_days) {
  body_org_rev[i] <- paste("SUM(CASE WHEN playerage <= ",i," AND (channel = 'organic' ) AND DATEDIFF(max_date,joindate) >= ",i," THEN revenue ELSE NULL END) AS d",i,"_ARPU,",sep='')
}

body_org_rev <- paste(body_org_rev,collapse = " ")

end_org_rev <- query_body # update from source('end_query.R') which was already imported above 
org_rev_sql <- paste(select_org_rev,body_org_rev,end_org_rev,sep = ' ')
org_rev_query_execute <- query_exec(org_rev_sql, project = project)

org_rev_data <- org_rev_query_execute

View(org_rev_data)
org_rev_data %>% dim()

# updated this: date >= '2016-10-01' to use the same dates as the rev and install pulls 
spend_sql <- paste("SELECT date, 
                            SUM(spend) AS spend 
                       FROM (SELECT date, 
                                    campaign_name, 
                                    ad_network, 
                                    platform, spend 
                                  FROM [n3twork-marketing-analytics:INSTALL_ATTRIBUTION.tenjin_summary] 
                                WHERE date BETWEEN DATE(DATE_ADD(DATE(current_date()), (",day_num,"*-1), 'DAY')) AND DATE(DATE_ADD(DATE(current_date()), ((",day_num,"*-1)+90), 'DAY'))
                                AND bundle_id = 'com.n3twork.legendary'
                              GROUP BY 1, 2, 3, 4, 5 
                              ORDER BY 1
                            ) 
                   GROUP BY 1 
                   ORDER BY 1"
                   ,sep='')


spend_query_execute <- query_exec(spend_sql, project = project)
spend_data <- spend_query_execute

View(spend_data)
spend_data %>% dim()

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

#### query exectute for ua revenue
select_ua_rev <- "SELECT joindate AS date, SUM(CASE WHEN playerage <= 0 AND (channel != 'organic') AND DATEDIFF(max_date,joindate) >= 0 THEN revenue ELSE NULL END) AS d0_ARPU,"

body_ua_rev <- character()

for(i in 1:cohort_days) {
  body_ua_rev[i] <- paste("SUM(CASE WHEN playerage <= ",i," AND (channel != 'organic' ) AND DATEDIFF(max_date,joindate) >= ",i," THEN revenue ELSE NULL END) AS d",i,"_ARPU,",sep='')
}

body_ua_rev <- paste(body_ua_rev,collapse = " ")

end_ua_rev <- query_body # update from source('end_query.R') which was already imported above 

ua_rev_sql <- paste(select_ua_rev,body_ua_rev,end_ua_rev,sep = ' ')
ua_rev_query_execute <- query_exec(ua_rev_sql, project = project)
ua_rev_data <- ua_rev_query_execute

View(ua_rev_data)
ua_rev_data %>% dim()

#### matches date range for cohorts with 7 days baked in
ua_revenue_date_match <- ua_rev_data[1:nrow(retention_curve_sums),]

#### blends ua revenue and organic revenue contribution
total_ua_revenue <- organic_revenue_ua_contribution[,2:ncol(organic_revenue_ua_contribution)] + ua_revenue_date_match[,2:ncol(ua_revenue_date_match)]
total_ua_revenue <- cbind(dates,total_ua_revenue)

View(total_ua_revenue)
total_ua_revenue %>% dim()

total_ua_revenue_test <- as.matrix(total_ua_revenue)

# set sub diagonal to NA 
total_ua_revenue_test[ row(total_ua_revenue_test) - 1 == col(total_ua_revenue_test)] <- NULL

View(total_ua_revenue_test)

#### retrieves total ua installs with k-factor for aCPI
# total_ua_installs <- retention_curve_sums$ua_installs + retention_curve_sums$org_installs_from_ua
#### for eCPI
total_ua_installs <- retention_curve_sums$ua_installs + retention_curve_sums$org_installs

#### calculates arpi
total_arpi <- total_ua_revenue[,2:ncol(total_ua_revenue)] / total_ua_installs

View(total_arpi)
total_arpi %>% dim()
# total_arpi[c(1:5),c(1:5)]

##### creates extended forecasting range
df <- data.frame(matrix(NA, ncol = 366 - ncol(total_arpi), nrow = nrow(total_arpi)))

df_column_names <- character()
for(i in ncol(total_arpi):365) {
  df_column_names[i] <- paste("d",i,"_ARPU",sep='')  
}

df_column_names <- df_column_names[!is.na(df_column_names)]
colnames(df) <- df_column_names
length(df_column_names)  
total_arpi <- cbind(dates,total_arpi,df)

total_arpi[c(1:20),c(1)]

##### transposes cohorts to dataframe in order to support prediction formats
total_arpi_transposed <- t(total_arpi)
# changed from 365
cohort_days <- c(0:90)
colnames(total_arpi_transposed) <- total_arpi_transposed[1, ]  #first row becomes the row names 
total_arpi_transposed <- as.data.frame(total_arpi_transposed[-1,])  # remove the row that was used to make names 
total_arpi_transposed <- cbind(cohort_days,total_arpi_transposed)  # add cohort_days as first column 

total_arpi_transposed %>%  dim()
View(total_arpi_transposed)
total_arpi_transposed  %>%  colnames()
total_arpi_transposed[c(1:20),c(1:20)]

mean(total_arpi_transposed[c(1:20),c(1:20)])

total_arpi_transposed <- sapply( total_arpi_transposed, as.numeric )
total_arpi_transposed  %>% class()
total_arpi_transposed  %>%  as.numeric()  %>% colMeans()



total_arpi_transposed_mean <- sapply( total_arpi_transposed, mean )


total_arpi_transposed[c(1:2),c(1:2)]

apply(total_arpi_transposed,1,sum, na.rm = T) 

head(total_arpi_transposed[,2:ncol(total_arpi_transposed)])

library(purrr) 

map_dbl(total_arpi_transposed, mean, na.rm = T) 

AJT_progress runs fine to here, need to do col means before inserting into model below 

View(total_arpi_transposed)


# total_arpi_transposed %>%  dim()
head(total_arpi_transposed[c(1:5),c(1:5)])  
# head(total_arpi_transposed[1,])
# dim(total_arpi_transposed)
# total_arpi_transposed[c(1:20),c(330:dim(total_arpi_transposed)[2])]
# total_arpi_transposed[c(1:5),c(330:dim(total_arpi_transposed)[2])]

#### models the data for each cohort
for (i in 2:ncol(total_arpi_transposed)) { #slightly different way to do seq_along 
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

total_arpi_transposed[1,2] %>% head()

finished_matrix <- as.matrix(total_arpi_transposed,stringsAsFactors= F)  #[c(1:5),c(1:5)]

#### pulls model coefficients and stores each cohort to a dataframe
model.list<-mget(grep("fit[0-9]+$", ls(),value=T))

length(model.list)
model.list[1]
model.list[length(model.list)]
model.list$fit10
model.list$fit7
model.list[1:2]

coefs<-lapply(model.list,function(x)coef(x))

coefs %>% class()
coefs %>% length()
coefs[111]

flattened_coefs <- as.data.frame(unlist(coefs))

flattened_coefs %>% dim()
flattened_coefs %>% head()

colnames(flattened_coefs) <- c('coefficients')
flattened_coefs$index <- row.names(flattened_coefs)  # create index colum from row names 
flattened_coefs$index <- as.numeric(gsub('[a-zA-Z.]','',flattened_coefs$index)) # remove 'fit' and '.A' (or '.B', '.C') from the names 
flattened_coefs$model_component <- gsub('[a-z0-9.]','',row.names(flattened_coefs))  # create colum with model component (eg A, B or C)

# Note at this point the flattened_coefs is 3x number of cohorts, because each of the 3 model components has been made into a row 
flattened_coefs %>% dim()
flattened_coefs %>% head() 
flattened_coefs[,1] %>% head()  
flattened_coefs %>% 
  arrange(index) %>% head()

library(tidyr)
flattened_coefs_AJT <- cbind(flattened_coefs, flattened_coefs[,1])
flattened_coefs_AJT <- flattened_coefs_AJT[,2:4]
flattened_coefs_AJT <- data.frame(flattened_coefs_AJT, row.names=NULL)
names(flattened_coefs_AJT)[[3]] <- "coefficients"
flattened_coefs_AJT %>% 
  spread(model_component,coefficients) %>% head()
flattened_coefs_AJT <- flattened_coefs_AJT %>% arrange(index) 
flattened_coefs_AJT %>% head(20)

flattened_coefs_AJT %>%  head()
flattened_coefs_AJT %>%  tail()


dd <- colnames(finished_matrix)
dd <- as.data.frame(dd)
dd <- as.data.frame(dd[2:nrow(dd),]) # get all elements from 2nd to the last (dropping "cohort_days")
names(dd) <- 'joindate' 
dd %>%  head()




####### Fills in na's with predicted values
  # Note row names are d0_ARPU, first column is cohort_days, rest are individual daily cohorts 
for (z in 2:ncol(finished_matrix)) {
  A <- flattened_coefs[flattened_coefs$index == z & flattened_coefs$model_component == 'A',1]
  B <- flattened_coefs[flattened_coefs$index == z & flattened_coefs$model_component == 'B',1]
  C <- flattened_coefs[flattened_coefs$index == z & flattened_coefs$model_component == 'C',1]
  for (i in 1:nrow(finished_matrix)){
    if (is.na(finished_matrix[i,z])) {
      finished_matrix[i,z] <- A*exp(-B*C^log(as.numeric(finished_matrix[i,1])))   # A*exp(-B*C^log("nth Day")) PROVIDE TO MARTIN 
    }
  }
}

head(total_arpi_transposed[c(1:5),c(2:3)])  

model.list$fit2
# > model.list$fit2
# Nonlinear regression model
# model: a ~ SSgompertz(log(b + 1), A, B, C)
# data: prediction_set
# A        B        C 
# 4.00e+04 1.30e+01 9.08e-01 
# residual sum-of-squares: 123.6

for(i in seq_along(c(1:5))) {
  print(4.00e+04*exp(-1.30e+01*9.08e-01^log(i)))
}

# > model.list$fit7
# Nonlinear regression model
# model: a ~ SSgompertz(log(b + 1), A, B, C)
# data: prediction_set
# A         B         C 
# 4.000e+04 1.298e+01 9.192e-01 
# residual sum-of-squares: 24.66

#model added to table
4.000e+04*exp(-1.298e+01*9.192e-01^log(90))

# load Martin's data and update it 
getwd()
setwd("/Users/andrewtynan/Desktop")
list.files()
Martin_Table_Example <- read.csv("Martin_Table_Example.csv", sep = ",")
Martin_Table_Example %>%  head()
# update the equation 
Martin_Table_Example[["equation"]] <- "4.000e+04*exp(-1.298e+01*9.192e-01^log(90))"
# check that it updated 
Martin_Table_Example %>%  head()
#update the table in BigQuery
insert_upload_job(project = '167707601509', 
                  dataset = "TEST_AGAMEMNON", 
                  table = "test_ltv_profiles",  
                  values = Martin_Table_Example, 
                  write_disposition = "WRITE_TRUNCATE")  


A*exp(-B*C^log(as.numeric("329")))
finished_matrix[c(1:5),c(1:5)]
dim(finished_matrix)


#### writes out final output
  # now the data is transposed and each row is a cohort (then top row "cohort_days" is removed)
transposed_predictions <- t(finished_matrix)
myData <- as.data.frame(transposed_predictions[-c(1), ], stringsAsFactors = F)

transposed_predictions[c(1:5),c(1:5)] 
myData[c(1:5),c(1:5)]
myData[c(1:5),c(ncol(myData-5):ncol(myData))]
ncol(myData)

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

base_info[c(1:5),c(1:5)]
final_output[c(1:5),c(1:5)]

final_output <-  final_output %>%
  arrange(desc(rownames(final_output)))
rownames(final_output) <- rev(cohort_names)

dim(final_output)
final_output[c(1:5),c(1:5)]


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

AJT_prog

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

