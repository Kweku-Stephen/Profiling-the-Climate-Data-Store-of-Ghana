
################# Exclusive to Priority stations Script ###################

# Path to output
path <- "For Windows Systems/outputs"

# Missing Days ####
MissingDays_prio <- function(vec = "", list, cores, rgex = "") {
  
  # condition for execution
  if (cores >= parallelly::availableCores()) {
    stop(sprintf(
      "nCores must be > 1 and less than %i", parallelly::availableCores()
    ))
  }
  # Creating a cluster of size, nCores
  sprintf("Creating a Cluster of Size %i", cores)
  cl_Prior <- parallel::makePSOCKcluster(names = cores)
  
  # Main Body
  (parallel::clusterApplyLB(
    cl_Prior,
    regions,
    \(vec, list) {
      
      # reg <- list[[vec]] |> . =>
      #   split(., as.factor(.[ ,"Distr"])) |> 
      reg <- list[[vec]] |> . =>
        split(., as.factor(.[ ,"Distr"])) |> 
        lapply(
          \(data) {
            dt <- within(
              data, 
              {
                StationName = rownames(data) |> 
                  strsplit("\\.") |> 
                  sapply(\(vec) vec[2]) #|> . =>
                  #do.call("c", .) 
              }
            )
            return(dt)
          }
        )
      
      return(reg)
      
    },
    list = list
  ) |> . =>
    # binding into one contiguous list
    do.call("c", .)) |> . =>
    do.call("rbind", .) -> Dist
  
  Dist1 <- split(Dist, as.factor(Dist[ ,"Distr"]))
  
  # Priority Districts
  return(Dist1[grep(rgex, names(Dist1), value = TRUE)])
  
  # stop Cluster
  parallel::stopCluster(cl_Prior)

}


