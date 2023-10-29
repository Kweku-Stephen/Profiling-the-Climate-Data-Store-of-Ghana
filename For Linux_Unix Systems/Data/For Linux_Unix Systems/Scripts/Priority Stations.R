# Import of Priority Districts into a vector
dir("For Linux_Unix Systems/Data", pattern = "priority", full.names = TRUE) |> 
  readLines() -> pDistricts

# creating a regex for extracting Priority Districts
ind <- 1 
res <- pDistricts[1]

for(i in 1:(length(pDistricts) - 1)) {
  res <- paste(res[1], pDistricts[ind], sep = "|")
  
  ind <- ind + 1
}

# Rainfall
RR_split |>
  lapply(
    \(datatable) {
      data.frame(
        start = range(datatable[ ,Year])[1],
        End = range(datatable[ ,Year])[2],
        Dist = unique(datatable[ ,Dist])[!is.na(unique(datatable[ ,Dist]))],
        Type = unique(datatable[ ,`Station Type`])[!is.na(unique(datatable[ ,`Station Type`]))]
      )
    }
  ) |> . =>
  do.call("rbind", .) |> . =>
  data.table::data.table(
    Station = rownames(.),
    Start = .[ ,1],
    End = .[ ,2],
    Dist = .[ ,3],
    Type = .[ ,4]
  ) |> . =>
  .[order(.[ ,Station]), ] -> rr_pr

subset(rr_pr, grepl(res, rr_pr$Dist)) |> 
  write.csv(file = "For Linux_Unix Systems/outputs/RR/priority_RR_profile.csv", row.names = FALSE)


# Max Temp ####
TX_split |>
  lapply(
    \(datatable) {
      data.frame(
        start = range(datatable[ ,Year])[1],
        End = range(datatable[ ,Year])[2],
        Dist = unique(datatable[ ,Dist])[!is.na(unique(datatable[ ,Dist]))],
        Type = unique(datatable[ ,`Station Type`])[!is.na(unique(datatable[ ,`Station Type`]))]
      )
    }
  ) |> . =>
  do.call("rbind", .) |> . =>
  data.table::data.table(
    Station = rownames(.),
    Start = .[ ,1],
    End = .[ ,2],
    Dist = .[ ,3],
    Type = .[ ,4]
  ) |> . =>
  .[order(.[ ,Station]), ] -> tx_pr

subset(tx_pr, grepl(res, tx_pr$Dist)) |> 
  write.csv(file = "For Linux_Unix Systems/outputs/TMax/priority_TX_profile.csv", row.names = FALSE)



# Min Temp ####
TN_split |>
  lapply(
    \(datatable) {
      data.frame(
        start = range(datatable[ ,Year])[1],
        End = range(datatable[ ,Year])[2],
        Dist = unique(datatable[ ,Dist])[!is.na(unique(datatable[ ,Dist]))],
        Type = unique(datatable[ ,`Station Type`])[!is.na(unique(datatable[ ,`Station Type`]))]
      )
    }
  ) |> . =>
  do.call("rbind", .) |> . =>
  data.table::data.table(
    Station = rownames(.),
    Start = .[ ,1],
    End = .[ ,2],
    Dist = .[ ,3],
    Type = .[ ,4]
  ) |> . =>
  .[order(.[ ,Station]), ] -> tn_pr

subset(tn_pr, grepl(res, tn_pr$Dist)) |> 
  write.csv(file = "For Linux_Unix Systems/outputs/TMin/priority_Tn_profile.csv", row.names = FALSE)



# Missing Stations Days ####
# Rainfall
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
                lapply(\(vec) vec[2]) |> . =>
                do.call("c", .) 
            }
          )
          return(dt)
        }
      )
    
    return(reg)
    
  },
  list = profMssReg,
  mc.cores = 16
) |> . =>
  # binding into one contiguous list
  do.call("c", .)) -> Dist_rr

# Priority Districts - Rainfall
Dist_rr[grep(res, names(Dist_rr), value = TRUE)] -> Priority_Rainfall


# Maximum Temperature 
parallel::mclapply(
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
                lapply(\(vec) vec[2]) |> . =>
                do.call("c", .) 
            }
          )
          return(dt)
        }
      )
    
    return(reg)
    
  },
  list = profMssRegTX,
  mc.cores = 16
) |> . =>
    # binding into one contiguous list
    do.call("c", .) -> Dist_tx

# Priority Districts - Rainfall
Dist_rr[grep(res, names(Dist_tx), value = TRUE)] -> Priority_TMax



# Minimum Temperature
# Maximum Temperature 
parallel::mclapply(
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
                lapply(\(vec) vec[2]) |> . =>
                do.call("c", .) 
            }
          )
          return(dt)
        }
      )
    
    return(reg)
    
  },
  list = profMssRegTN,
  mc.cores = 16
) |> . =>
  # binding into one contiguous list
  do.call("c", .) -> Dist_tn

# Priority Districts - Rainfall
Dist_rr[grep(res, names(Dist_tn), value = TRUE)] -> Priority_TMin



# Duplicates ####
# Rainfall
Duplicates_RR_prio <- list(
  
  # Station ID Duplicates
  RR_ID_Duplicates = {Profile_StationsRR |> . =>
      .[duplicated(.[ ,"ID"]), ][ ,"ID"] |> 
      unlist() -> dupsID_RR
    
    Profile_StationsRR |>
      subset(ID %in% dupsID_RR) },
  
  
  # Multiple Stations at the same town but different IDs
  OneTown_DiffIDs = {Profile_StationsRR |> . =>
      .[duplicated(.[ ,"StationName"]), ][ ,StationName] -> dupsName_RR
    
    Profile_StationsRR |> 
      subset(StationName %in% dupsName_RR) |> . =>
      .[!duplicated(.[ ,ID]), ]},
  
  
  # Same Stations different/Same IDs but with Same Station Type
  OneTown_SameORdiffID_diffType = {Profile_StationsRR |> . =>
      .[duplicated(.[ ,"StationName"]), ][ ,StationName] -> dupsName_RR
    
    Profile_StationsRR |> 
      subset(StationName %in% dupsName_RR) |> . =>
      .[!duplicated(.[ ,ID]), ] -> a
    
    split(a, as.factor(a$StationName)) -> aa
    
    dp <- function(data) {
      subset(data, duplicated(Type))[ ,Type] -> name
      subset(data, Type %in% name)
    }
    
    lapply(aa, dp) |> . => do.call("rbind", .)}
  
) |> 
  # Extracting Priority Stations
  lapply(\(datatable) subset(datatable, grepl(res, datatable[ ,District])))

# Writing to a workbook ####
rio::export(
  Duplicates_RR, 
  file = "For Linux_Unix Systems/outputs/RR/Duplicates_RR_Priority.xlsx", 
  sheetNames = c("Duplicated IDs", "oneTownDiffIDs", "oneTowndif/sameid_diffType"),
)



# Max Temp
Duplicates_TX_prio <- list(
  
  # Station ID Duplicates
  TX_ID_Duplicates = {Profile_StationsTX |> . =>
      .[duplicated(.[ ,"ID"]), ][ ,"ID"] |> 
      unlist() -> dupsID_TX
    
    Profile_StationsTX |>
      subset(ID %in% dupsID_TX) },
  
  
  # Multiple Stations at the same town but different IDs
  OneTown_DiffIDs = {Profile_StationsTX |> . =>
      .[duplicated(.[ ,"StationName"]), ][ ,StationName] -> dupsName_TX
    
    Profile_StationsTX |> 
      subset(StationName %in% dupsName_TX) |> . =>
      .[!duplicated(.[ ,ID]), ]},
  
  
  # Same Stations different/Same IDs but with Same Station Type
  OneTown_SameORdiffID_diffType = {Profile_StationsTX |> . =>
      .[duplicated(.[ ,"StationName"]), ][ ,StationName] -> dupsName_TX
    
    Profile_StationsTX |> 
      subset(StationName %in% dupsName_TX) |> . =>
      .[!duplicated(.[ ,ID]), ] -> a
    
    split(a, as.factor(a$StationName)) -> aa
    
    dp <- function(data) {
      subset(data, duplicated(Type))[ ,Type] -> name
      subset(data, Type %in% name)
    }
    
    lapply(aa, dp) |> . => do.call("rbind", .)}
  
) |> 
  # Extracting Priority Stations
  lapply(\(datatable) subset(datatable, grepl(res, datatable[ ,District])))

# Writing to a workbook ####
rio::export(
  Duplicates_TX_prio, 
  file = "For Linux_Unix Systems/outputs/TMin/Duplicates_TN_Priority.xlsx", 
  sheetNames = c("Duplicated IDs", "oneTownDiffIDs", "oneTowndif/sameid_diffType"),
)


# Min Temp
Duplicates_TN_prio <- list(
  
  # Station ID Duplicates
  TX_ID_Duplicates = {Profile_StationsTN |> . =>
      .[duplicated(.[ ,"ID"]), ][ ,"ID"] |> 
      unlist() -> dupsID_TN
    
    Profile_StationsTN |>
      subset(ID %in% dupsID_TN) },
  
  
  # Multiple Stations at the same town but different IDs
  OneTown_DiffIDs = {Profile_StationsTN |> . =>
      .[duplicated(.[ ,"StationName"]), ][ ,StationName] -> dupsName_TN
    
    Profile_StationsTN |> 
      subset(StationName %in% dupsName_TN) |> . =>
      .[!duplicated(.[ ,ID]), ]},
  
  
  # Same Stations different/Same IDs but with Same Station Type
  OneTown_SameORdiffID_diffType = {Profile_StationsTX |> . =>
      .[duplicated(.[ ,"StationName"]), ][ ,StationName] -> dupsName_TX
    
    Profile_StationsTX |> 
      subset(StationName %in% dupsName_TX) |> . =>
      .[!duplicated(.[ ,ID]), ] -> a
    
    split(a, as.factor(a$StationName)) -> aa
    
    dp <- function(data) {
      subset(data, duplicated(Type))[ ,Type] -> name
      subset(data, Type %in% name)
    }
    
    lapply(aa, dp) |> . => do.call("rbind", .)}
  
) |> 
  # Extracting Priority Stations
  lapply(\(datatable) subset(datatable, grepl(res, datatable[ ,District])))

# Writing to a workbook ####
rio::export(
  Duplicates_TN_prio, 
  file = "For Linux_Unix Systems/outputs/TMax/Duplicates_TX_Priority.xlsx", 
  sheetNames = c("Duplicated IDs", "oneTownDiffIDs", "oneTowndif/sameid_diffType"),
)



