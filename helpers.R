
# to remove the query list if it exists (mainly for cleaning while testing)
# will reuse for other lists below as well 
rm_if_exists <- function(obj, env = globalenv()) {
  obj <- deparse(substitute(obj))
  if(exists(obj, envir = env)) {
    rm(list = obj, envir = env) 
  }
}