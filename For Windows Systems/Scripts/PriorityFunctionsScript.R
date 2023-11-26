
################# Exclusive to Priority stations Script ###################
# 

# Missing Days ####
MissingDays_prio <- function(vec = "", list, cluster, rgex = "") {
  
  # condition for execution
  if (length(cluster) >= parallelly::availableCores()) {
    stop(sprintf(
      "cluster size must be > 1 and less than %i", parallelly::availableCores()
    ))
  }
  
  # Main Body
  (parallel::clusterApplyLB(
    cluster,
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
  
  return(split(Dist, as.factor(Dist[ ,"Distr"])))


}



nCores <- function() {
  
  availCores <- parallelly::availableCores()
  nc <- as.integer(readline(
    prompt = sprintf("Enter no. of compute nodes (must be > 1 and < %i) :", availCores)
  ))
  
  output <- ifelse(
    nc > availCores | nc >= 0,
    stop(sprintf("Number of cores must be > 1 and < %i", availCores)),
    nc
  )
  
  
  return(output)
  
}

