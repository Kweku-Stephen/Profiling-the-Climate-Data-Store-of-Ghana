#=====================================================================================================================
#                      GSSTI - GMET -  
# Digitizing Climatological Paper Archives in Ghana: 
# 
# Project: Digitizing Climatological Paper Archives in Ghana: Consultancy Service to support the Ghana Meteorololigcal Agency in Digitizing Climatological Paper Archives in Ghana
# Code: Profiling entire Rainfall Data of Ghana
# Variable: Rainfall
# Written by: Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Edited by:  Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Objective: This code profiles the Rainfall data Store of Ghana as a function of their respective districts.
#           it returns the start and end dates, missing  days, days with no recorded rainfall, duplicated stations names, duplicated stations IDs amongst
#           other duplicates.               



###################################### Rainfall ##############################################
# Invoking the pipebind operator ####
Sys.setenv("_R_USE_PIPEBIND_" = "true")

# sourcing Functions Script
source("For Linux_Unix Systems/Scripts/FunctionsScript.R")

# Output Directory 
dir.create("For Linux_Unix Systems/outputs/RR")

# path
path <- "For Linux_Unix Systems/outputs"

# Rainfall
RR <- data[[grep("RR", names(data), value = TRUE)]]
RR_split <- dataSplit[[grep("RR", names(dataSplit), value = TRUE)]]

# Start and End Years for each Rainfall Stations ####
startEndYear(list = RR_split)  |> 
  # Writing to Disk
  write.csv(
    file = paste(path, "RR/startEndYears of Rainfall Stations.csv", sep = "/"), 
    row.names = FALSE
  )


# Compute nodes
ncores <- nCores()


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
  file = paste(path, "RR/Profiled StationsRR.csv", sep = "/"), 
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
  setNames(regions) -> profMssRegRR

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
                sapply(\(vec) vec[2]) #|> . =>
                #do.call("c", .) 
            }
          )
          return(dt)
        }
      )
    
    # Writing out to an excel workbook
    rio::export(
      reg, 
      file = file.path(path, "RR", paste(vec, "xlsx", sep = ".")), 
      sheetName = names(reg), 
      rowNames = FALSE
    )
    
  },
  list = profMssRegRR,
  mc.cores = ncores
)



#==============================Extracting Duplicates===================================
#==============================Extracting Duplicates===================================
# Duplicates ####
Duplicates(Profile_StationsRR) |> 
  # Writing to a workbook ####
rio::export(
  file = paste(path, "RR/Duplicates_RR.xlsx", sep = "/"), 
  sheetNames = c("Duplicated IDs", "oneTownDiffIDs", "oneTownSD-IDsSameType")
)


# Same Observation Duplicates
subset(
  data[[grep("RR|Rr|rr", names(data), value = TRUE)]],
  duplicated(
    with(
      data[[grep("RR|Rr|rr", names(data), value = TRUE)]],
      paste(Name, `Eg Gh Id`, `Station Type`, Year, Month, sep = "-")
    )
  )
) |> 
  rio::export(file = paste(path, "RR/sameObservationRR.xlsx",sep = "/"))


  





  


