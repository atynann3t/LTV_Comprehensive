
for number in the seq
insert num into query 
  
pull and check dates 
makes sure subsequent data pulls work right

AND DATE(install_timestamp) BETWEEN DATE(DATE_ADD(DATE(current_date()), (2*-1), 'DAY')) AND DATE(DATE_ADD(DATE(current_date()), (-1*1), 'DAY'))



total_arpi  %>% dim()
total_arpi  %>% head()
total_arpi[c(1:5),c(1:5)]

dd <- total_arpi[c(1:5),c(2:5)]
colnames(dd) <- NULLs
dd

total_arpi_2 <- total_arpi[,c(2:dim(total_arpi)[2])] # from 2nd colum to last column 
# dd <- total_arpi[,c((dim(total_arpi)[2]-95):dim(total_arpi)[2]-90)]  # testing the columns w/ NAs 
# dd <- total_arpi[c(1:5),c(2:5)]
colnames(total_arpi_2) <- NULL
total_arpi_2[c(1:5),c(1:5)] %>% head()
total_arpi_2 %>% dim()

# create list to store N day average ARPU 
total_arpi_3 <- numeric() 
for(i in seq_along(total_arpi_2)) {
  # print(i)
  # print(dd[,i])
  # print(class(dd[,i]))
  # print(mean(total_arpi_2[,i], na.rm=TRUE))
  total_arpi_3[i] <- mean(total_arpi_2[,i], na.rm=TRUE)
}

total_arpi_2 %>% length() #check size 

total_arpi_2


for (i in 2:ncol(total_arpi_transposed)) {
  print(i)
}  

#### models the data for each cohort
for (i in 2:ncol(total_arpi_transposed[c(1:5),c(1:5)])) {
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



