
################# Exclusive to Priority stations Script ###################


# Missing Days ####
MissingDays_prio <- function(vec = "", list, cluster, rgex = "") {
    
  # condition for execution
  if (length(cluster) >= parallelly::availableCores() & length(cluster) <= 1) {
    stop(sprintf(
      "cluster size must be > 1 and less than %i", parallelly::availableCores()
    ))
  }
  
  # Main Body
  (parallel::mclapply(
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
    list = list,
    mc.cores = cluster
  ) |> . =>
    # binding into one contiguous list
    do.call("c", .)) |> . =>
    do.call("rbind", .) -> Dist
  
  # Return
  return(
    split(Dist, as.factor(Dist[ ,"Distr"])) |> . =>
      .[grep(rgex, names(.), value = TRUE)]
  )

}


