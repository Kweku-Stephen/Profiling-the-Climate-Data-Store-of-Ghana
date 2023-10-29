
################# Exclusive to Priority stations Script ###################

# Enabling the Pipebind Operator
Sys.setenv("_R_USE_PIPEBIND_" = "true")

# Path to output
path <- "For Linux_Unix Systems/outputs"

# Missing Days ####
MissingDays_prio <- function(vec = "", list, nCores, rgex = "") {
    
  # condition for execution
  if (nCores >= parallelly::availableCores()) {
    stop(paste("nCores must be <=", parallelly::availableCores()))
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
    mc.cores = nCores
  ) |> . =>
    # binding into one contiguous list
    do.call("c", .)) |> . =>
    do.call("rbind", .) -> Dist
  
  Dist1 <- split(Dist, as.factor(Dist[ ,"Distr"]))
  
  # Priority Districts
  return(Dist1[grep(rgex, names(Dist1), value = TRUE)])

}


