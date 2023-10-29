###################################### Rainfall ##############################################
# Invoking the pipebind operator ####
Sys.setenv("_R_USE_PIPEBIND_" = "true")

# sourcing Functions Script
source("For Linux_Unix Systems/Scripts/FunctionsScript.R")

# Output Directory 
dir.create("For Linux_Unix Systems/outputs/RR")

# Computing nodes
ncores <- parallelly::availableCores() - 17

# Rainfall
RR <- data[[grep("RR", names(data), value = TRUE)]]
RR_split <- dataSplit[[grep("RR", names(dataSplit), value = TRUE)]]

# Start and End Years for each Rainfall Stations ####
RR_split |>
  lapply(
    \(datatable) range(datatable[ ,"Year"])
  ) |> . =>
  do.call("rbind", .) |> . =>
  data.table::data.table(
    Station = rownames(.),
    Start = .[ ,1],
    End = .[ ,2]
  ) |> . =>
  .[order(.[ ,Station]), ] -> startEndYears_RR

# Writing to Disk
write.csv(startEndYears_RR, file = "startEndYears of Rainfall Stations.csv", row.names = FALSE)

# spliting Stations IDs "Eg Gh Id" into 15 elements, as a function of cluster size
idsRR <- parallel::splitIndices(length(unique(RR[ ,StationName_ID])), ncores) |> 
  lapply(\(vec) unique(RR[ ,StationName_ID])[vec])

# Data Reshaping for all dataTables of the list "RR_Split" from "dataSplit" ####
dataReshapedRR <- parallel::mclapply(
  idsRR, 
  lD_list, 
  mc.cores = ncores,
  list = RR_split
) |> . =>
  do.call("c", .) 

# Populating missing dates in each dataTable/Station of the list dataReshapedRR
dataReshapedRRDone <- parallel::mclapply(
  idsRR,
  \(vec) {
    dataReshapedRR[vec] |> 
      lapply(PopMisnDate, "Year")
  },
  mc.cores = ncores
)  |> . =>
  do.call("c", .)



# Profiling Rainfall Data ####
# Computing data availability for each Station
Profile_StationsRR <- parallel::mclapply(
  idsRR,
  # Anaymous Function which calls "Profile" for every node
  \(vec, list) {
    list[vec] |> 
      lapply(
        Profile
      )
  },
  list = dataReshapedRRDone,
  mc.cores = ncores
) |> . =>
  do.call("c", .) |> . =>
  do.call("rbind", .)

# Writing to disk
write.csv(
  Profile_StationsRR, 
  file = "For Linux_Unix Systems/outputs/RR/Profiled StationsRR.csv", 
  row.names = FALSE
)


# Missing days for each Station ####
# Extracting missing days for each Station
parallel::mclapply(
  idsRR,
  \(vec, list){
    list[vec] |> 
      lapply(\(data) data[is.na(value), ])
  },
  list = dataReshapedRRDone,
  mc.cores = ncores
) |> . =>
  do.call("c", .) -> profileMissing

profileMissing <- (parallel::mclapply(
  idsRR,
  \(vec, list){
    list[vec] |> 
      lapply(missingDays)
  },
  list = profileMissing,
  mc.cores = ncores
) |> . =>
  do.call("c", .)) 

# Writing to disk as excel workbooks as a function of Regions and respective Districts ####
# Vector of all 16 Regions in Ghana
regions <- c(
  "Upper West", "Upper East", "Northern Region", "Savannah", 
  "Ahafo", "Bono", "Bono East", "Ashanti", "Eastern", "Central", 
  "North East", "Oti", "Western North", "Volta", "Greater Accra", "Western"
)

profileMissing |> 
  lapply(as.data.frame) |> . => # coverting from data.table to data.frame to preserve rownames
  do.call("rbind", .) |> . =>
  split(., as.factor(.[ ,"Reg"])) |> . =>
  lapply(
    c("[Uu]pper [Ww]est", "[Uu]pper [Ee]ast", "[Nn]orthern", "[Ss]avannah", 
      "[Aa]hafo", "[Bb]ono", "[Bb]ono [Ee]ast", "[Aa]shanti","[Ee]astern", 
      "[Cc]entral", "[Nn]orth [Ee]ast", "[Oo]ti", "[Ww]estern [Nn]orth", 
      "[Vv]olta", "[Gg]reater [Aa]ccra", "[Ww]estern"),
    \(vec, list){
      list[grep(vec, names(list), value = TRUE)] |> . =>
        do.call("rbind", .)
    },
    list = .
  ) |> 
  setNames(regions) -> profMssReg

# Saving to workbooks
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
    
    rio::export(
      reg, 
      file = file.path(getwd(), "For Linux_Unix Systems/outputs/RR", paste(vec, "xlsx", sep = ".")), 
      sheetName = names(reg), 
      rowNames = FALSE
    )
    
  },
  list = profMssReg,
  mc.cores = 16
)



# Duplicates ####
Duplicates_RR <- list(
  
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
  OneTown_SameORdiffID_DiffType = {Profile_StationsRR |> . =>
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
  
)

# Writing to a workbook ####
rio::export(
  Duplicates_RR, 
  file = "For Linux_Unix Systems/outputs/RR/Duplicates_RR.xlsx", 
  sheetNames = c("Duplicated IDs", "oneTownDiffIDs", "oneTownSD-IDsSameType"),
)



  


