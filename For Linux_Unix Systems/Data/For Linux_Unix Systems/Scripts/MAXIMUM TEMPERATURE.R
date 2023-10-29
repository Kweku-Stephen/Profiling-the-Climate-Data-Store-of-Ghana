########################################## MAXIMUM TEMPERATURE ########################################
# Invoking the pipebind operator ####
Sys.setenv("_R_USE_PIPEBIND_" = "true")

# sourcing Functions Script
source("For Linux_Unix Systems/Scripts/FunctionsScript.R")

# Output Directory 
dir.create("For Linux_Unix Systems/outputs/TMax")

# Computing nodes
ncoresTemp <- parallelly::availableCores() - 25

# Maximum Temperature ####
# Extracting the Tmax data.table from the list data(containing all 3 datatables of rr, tx and tn)
TX <- data[[grep("Tx", names(data), value = TRUE)]]

# Extracting split Tmax data from the datasplit list
TX_split <- dataSplit[[grep("Tx", names(dataSplit), value = TRUE)]]

# Start and End Year for Each Station
TX_split |>
  lapply(
    \(datatable) range(datatable[ ,"Year"])
  ) |> . =>
  do.call("rbind", .) |> . =>
  data.table::data.table(
    Station = rownames(.),
    Start = .[ ,1],
    End = .[ ,2]
  ) |> . =>
  .[order(.[ ,Station]), ] -> startEndYears_TX

# Writing to Disk
write.csv(
  startEndYears_TX, 
  file = "For Linux_Unix Systems/outputs/TMax/StartandYears of .csv", 
  row.names = FALSE
)

# spliting Tmax Stations IDs "Eg Gh Id" into 15 elements, as a function of cluster size
idsTX <- parallel::splitIndices(length(unique(TX[ ,StationName_ID])), ncoresTemp) |> 
  lapply(\(vec) unique(TX[ ,StationName_ID])[vec])

# Data Reshaping for all dataTables of the list "TX_Split" from "dataSplit" ####
dataReshapedTX <- parallel::mclapply(
  idsTX,
  lD_list,
  mc.cores = ncoresTemp,
  list = TX_split
) |> . =>
  do.call("c", .)

# Populating missing dates for each Station
dataReshapedTXDone <- parallel::mclapply(
  idsTX,
  \(vec) {
    dataReshapedTX[vec] |>
      lapply(PopMisnDate, "Year")
  },
  mc.cores = ncoresTemp
)  |> . =>
  do.call("c", .)

# # Data Reshaping for all dataTables of the list "TX_Split" from "dataSplit" ####
# dataReshapedTX <- parallel::mclapply(
#   idsTX, 
#   lD_list, 
#   mc.cores = ncoresTemp,
#   list = TX_split
# ) |> . =>
#   do.call("c", .) |> . =>
#   # Populating missing dates for each Station
#   parallel::mclapply(
#     idsTX,
#     \(vec) {
#       .[vec] |> 
#         lapply(PopMisnDate, "Year")
#     },
#     mc.cores = ncoresTemp
#   )  |> . =>
#   do.call("c", .)



# Profiling Maximum Temperature Data ####
# Computing data availability for each Station
Profile_StationsTX <- parallel::mclapply(
  idsTX,
  # Anaymous Function which calls "Profile" for every node
  \(vec, list) {
    list[vec] |> 
      lapply(
        Profile
      )
  },
  list = dataReshapedTXDone,
  mc.cores = ncoresTemp
) |> . =>
  do.call("c", .) |> . =>
  do.call("rbind", .)

# Writing to disk
write.csv(
  Profile_StationsTX, 
  file = "For Linux_Unix Systems/outputs/TMax/Profiled StationsTX.csv", 
  row.names = FALSE
)



# Missing days for each Station ####
# Extracting missing days for each Station
parallel::mclapply(
  idsTX,
  \(vec, list){
    list[vec] |> 
      lapply(\(data) data[is.na(value), ])
  },
  list = dataReshapedTXDone,
  mc.cores = ncoresTemp
) |> . =>
  do.call("c", .) -> profileMissingTX

profileMissingTX <- (parallel::mclapply(
  idsTX,
  \(vec, list){
    list[vec] |> 
      lapply(missingDays)
  },
  list = profileMissingTX,
  mc.cores = ncores
) |> . =>
  do.call("c", .)) 

# Writing to disk as excel workbooks as a function of Regions and respective Districts ####
regions <- c(
  "Upper West", "Upper East", "Northern Region", "Savannah", "Ahafo", "Bono", "Bono East", 
  "Ashanti", "Eastern", "Central", "North East", "Oti", "Western North", "Volta", "Greater Accra", "Western"
)

profileMissingTX |> 
  lapply(as.data.frame) |> . => 
  do.call("rbind", .) |> . =>
  split(., as.factor(.[ ,"Reg"])) |> . =>
  lapply(
    c("[Uu]pper [Ww]est", "[Uu]pper [Ee]ast", "[Nn]orthern", "[Ss]avannah", "[Aa]hafo", 
      "[Bb]ono", "[Bb]ono [Ee]ast", "[Aa]shanti","[Ee]astern", "[Cc]entral", 
      "[Nn]orth [Ee]ast", "[Oo]ti", "[Ww]estern [Nn]orth", "[Vv]olta", "[Gg]reater [Aa]ccra", "[Ww]estern"),
    \(vec, list){
      list[grep(vec, names(list), value = TRUE)] |> . =>
        do.call("rbind", .)
    },
    list = .
  ) |> 
  setNames(regions) -> profMssRegTX

# Saving to workbooks
parallel::mclapply(
  regions,
  \(vec, list) {
    
    # reg <- list[[vec]] |> . =>
    #   split(., as.factor(.[ ,"Distr"]))
    
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
    
    rio::export(
      reg, 
      file = file.path(getwd(), "For Linux_Unix Systems/outputs/TMax", paste(vec, "xlsx", sep = ".")), 
      sheetName = names(reg), 
      rowNames = FALSE
    )
    
  },
  list = profMssRegTX,
  mc.cores = 16
)



# Duplicates ####
Duplicates_TX <- list(
  
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
  OneTown_SameORdiffID_DiffType = {Profile_StationsTX |> . =>
      .[duplicated(.[ ,"StationName"]), ][ ,StationName] -> dupsName_TX
    
    Profile_StationsTX |> 
      subset(StationName %in% dupsName_TX) |> . =>
      .[!duplicated(.[ ,ID]), ] -> a
    
    split(a, as.factor(a$StationName)) -> ab
    
    dp <- function(data) {
      subset(data, duplicated(Type))[ ,Type] -> name
      subset(data, Type %in% name)
    }
    
    lapply(ab, dp) |> . => do.call("rbind", .)}
  
)

# Writing to a workbook ####
rio::export(
  Duplicates_TX, 
  file = "For Linux_Unix Systems/outputs/TMax/Duplicates_TX.xlsx", 
  sheetNames = c("Duplicated IDs", "oneTownDiffIDs", "oneTownSD-IDsSameType"),
)
