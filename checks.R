
for(i in seq_along(d)) {
  print(d[i][[1]][[2]])
}

for(i in seq_along(d)) {
  print(paste(shQuote(d[i], type="cmd"), collapse=", "))
}

for(i in seq_along(d)) {
  print(dput(as.character(d[i])))
}

for(date_chunck in n_day_chunk_list) { # create corresponding D1 to Dn incremental periods as    
  print(date_chunck)
}

for(liftime_days in 1:cohort_days) { # create corresponding D1 to Dn incremental periods as
  print(liftime_days)
}  

for(cohorts in cohort_days) { # create corresponding D1 to Dn incremental periods as
  print(cohorts)
}  

for(i in metrics) {
    print(i)
  for(d in levels) {
    print(d)
  }
}

for(i in metrics) {
  select_1 <- select_1_syntax(i)
  print(select_1)
}

# check 
for(n in seq_along(levels_list)) { 
  k <- levels_list[n]
  # print(names(k))  #get naems of inner list 
  print(k[[1]])  #get contents of inner list
}

class(queries_list)
class(names(levels_list[n]))
queries_list %>% length()
class(queries_list[[1]])
queries_list[1]

dataset[c(1:5),c(1:5)]  


for(i in levels) {
  # create each of the three parts of the select statement
  select_levels <- paste("SELECT ",i,",",sep='') # insert the groupings variables from levels 
  # create D0 incremental period
  select_1 <- "SELECT joindate AS date, EXACT_COUNT_DISTINCT(CASE WHEN playerage = 0 AND channel != 'organic' THEN gamerid ELSE NULL END) AS installs,"
  # create D1 to Dn incremental periods (this will be D90 eventually)
  select_2 <- character() # chr vector to store them
  for(i in 1:cohort_days) {
    select_2[i] <- paste("EXACT_COUNT_DISTINCT(CASE WHEN playerage = ",i," AND channel != 'organic' THEN gamerid ELSE NULL END) AS d",i,",",sep='')
  }  
  # renamed org_revenue as arpu_query 
  select_2 <- paste(select_2,collapse = " ") # string formating 
  query_num <- query_num + 1  # increment corresponding to number of queries being created, based on number of strings in levels
  query_group_by <- query_group_syntax(query_num) # create query_group_by based on grouping levels 
  query_body <- gsub("[\r\n]", " ", query_body)   # convert \r carriage returns and \n new lines to spaces in text string from source('query_body.R')
  query_body <- paste(query_body, collapse = " ")  # clean-up string space formatting           
  # renamed org_revenue as arpu_query   
  query_syntax <- paste(select_levels, 
                        select_1,
                        select_2,
                        query_body,   
                        query_group_by,
                        sep = ' ')  
  query <- paste(c("query_", query_num), collapse = "")   # create name obj for each query 
  query <- query_syntax                                   # insert current query into name obj 
  daily_arpu_queries <- c(daily_arpu_queries, c=query)    # append queries
}


