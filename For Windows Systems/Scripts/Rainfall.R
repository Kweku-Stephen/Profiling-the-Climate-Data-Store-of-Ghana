#=====================================================================================================================
#                      GSSTI - GMET - GNAPP 
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



#===================================Rainfall =================================
# Invoking the pipebind operator ####
Sys.setenv("_R_USE_PIPEBIND_" = "true")
source("For Windows Systems/Scripts/FunctionsScript_W.R")

# sourcing Functions Script
#source("For Linux_Unix Systems/Scripts/FunctionsScript.R")

# Output Directory 
dir.create("For Windows Systems/outputs/RR")

# Rainfall
RR <- data[[grep("RR", names(data), value = TRUE)]]
RR_split <- dataSplit[[grep("Rainfall", names(dataSplit), value = TRUE)]]

# Start and End Years for each Rainfall Stations ####
startEndYear(list = RR_split) |> 
  # Writing to Disk
  write.csv(
    file = paste(path, "RR/startEndYears of Rainfall Stations.csv", sep = "/"), 
    row.names = FALSE
  )




#=======================Creating a cluster==================================#
# Compute nodes
ncores <- nCores()

# cluster of size, ncores
# Creating a Cluster of ncore nodes
cl_RR <- parallel::makeCluster(
  spec = ncores,
  type = "PSOCK"
)

# spliting Stations IDs "Eg Gh Id" into "ncores" elements (cluster size)
idsRR <- parallel::splitIndices(length(unique(RR[ ,StationName_ID])), ncores) |> 
  lapply(\(vec) unique(RR[ ,StationName_ID])[vec])


# #Exporting datasets to all nodes of cl_RR
#parallel::clusterExport(cl_RR, c("RR_split", "idsRR"))

#Evaluating Functions on all nodes of the cluster "cl_RR"
parallel::clusterEvalQ(
  # cluster of size cl_RR
  cl_RR,
  
  # Evaluating user defined Functions all nodes of the cluster cl_RR
  {
    
    #Invoking the pipebind operator
    Sys.setenv("_R_USE_PIPEBIND_" = "true")
    
    # Data Reshaping for a single datatable
    longData <- function(dataTable) {
      
      lng <- data.table::melt(
        dataTable,
        id = grep("^[^Val]", names(dataTable), value = TRUE),
        measure = grep("^[Val]", names(dataTable), value = TRUE)
      ) |> . => 
        split(., as.factor(.[ ,Year])) |>
        lapply(\(dataTable) dataTable[order(dataTable[ ,Month]), ]) |> . =>
        do.call("rbind", .)
      
      return(lng)
    }
    
    
    # Data Reshaping for a list of datatables / Reshaping dataTables in a list
    lD_list <- function(vec, list) {
      
      ls_data <- list[vec]
      
      out <- lapply(
        ls_data,
        \(dataTable) {
          
          # Wide to long format
          res <- longData(dataTable) |> . =>
            # # Adding Date column using Year, Month and Day columns of each dataframe/list element
            .[ ,Date := as.Date(with(., paste(Year, Month, substr(variable, 4, 5), sep = "-")), format = "%Y-%m-%d")]
          
          return(subset(res, !is.na(Date)))
          
        }
      )
      
      return(out)
    }
    
    
    # Setting all Trace to 0 if present
    setTrace0 <- function(dataTable, var = "") {
      dataTable[dataTable[ ,var] %in% c("Trace", "trace", "TRACE", "TR", "tr"), var]
      return(data)
    }    
    
    # Function to populate missing years for each Station
    PopMisnDate <- function(dataTable, Time = "") {
      stopifnot(!is.character(var) | !is.character(var))
      Year = cat(Time)
      
      t <- data.table::data.table(
        Date = range(dataTable[ ,Year]) |> . =>
          seq.Date(
            as.Date(paste(.[1], "01", "01", sep = "-")), 
            as.Date(paste(.[2], "12", "31", sep = "-")), 
            by = "day"
          ),
        Distr = unique(dataTable[ ,Dist])[!is.na(unique(dataTable[ ,Dist]))],
        Reg = unique(dataTable[ ,Region])[!is.na(unique(dataTable[ ,Region]))]
      )
      
      
      filled <- dplyr::full_join(dataTable, t, by = c("Date" = "Date")) |> .=>
        .[order(.[ ,Date]), ]
      
      return(filled)
      
    }
    
    # PopMisnDate <- function(dataTable, Time = "") {
    #   stopifnot(!is.character(var) | !is.character(var))
    #   Year = cat(Time)
    #   
    #   t <- data.table::data.table(
    #     Date = range(dataTable[ ,Year]) |> . =>
    #       seq.Date(
    #         as.Date(paste(.[1], "01", "01", sep = "-")), 
    #         as.Date(paste(.[2], "12", "31", sep = "-")), 
    #         by = "day"
    #       )
    #   )
    #   
    #   
    #   filled <- dplyr::full_join(dataTable, t, by = c("Date" = "Date")) |> .=>
    #     .[order(.[ ,Date]), ]
    #   
    #   return(filled)
    #   
    # }
    
    
    # Function to profile each Stations Data Store ####
    Profile <- function(data){
      
      # Tibble of 9 variables
      dt <- data.table::data.table(
        
        StationName = unique(data[ ,Name])[!is.na(unique(data[ ,Name]))],
        ID = unique(data[ ,`Eg Gh Id`])[!is.na(unique(data[ ,`Eg Gh Id`]))],
        District = unique(data[ ,Distr])[!is.na(unique(data[ ,Distr]))],
        lon = unique(data[ ,Geogr1])[!is.na(unique(data[ ,Geogr1]))],
        lat = unique(data[ ,Geogr2])[!is.na(unique(data[ ,Geogr2]))], 
        StartYear = min(data[ ,Year], na.rm = TRUE),
        EndYear = max(data[ ,Year], na.rm = TRUE),
        Type = unique(data[ ,`Station Type`])[!is.na(unique(data[ ,`Station Type`]))],
        `% Available` = (length(data[ ,value][!is.na(data[ ,value])]) / length(data[ ,value])) * 100,
        `% missing` = 100 - (length(data[ ,value][!is.na(data[ ,value])]) / length(data[ ,value])) * 100,
        `Number of Years` = length(unique(data[ ,Year])),
        
        `% Available 2022` = {
          joined <- data.table::merge.data.table(
            data[ ,c("Date", "value")],
            data.table::data.table(Date = seq.Date(
              as.Date(paste(range(data[ ,Year], na.rm = T)[1], "01", "01", sep = "-")),
              as.Date("2022-12-31"),
              by = "day"
            )),
            all = TRUE
          )
          (length(joined[ ,value][!is.na(joined[ ,value])]) / length(joined[ ,value])) * 100
        },
        
        `% missing 2022` = 100 - {
          joined <- data.table::merge.data.table(
            data[ ,c("Date", "value")],
            data.table::data.table(Date = seq.Date(
              as.Date(paste(range(data[ ,Year], na.rm = T)[1], "01", "01", sep = "-")),
              as.Date("2022-12-31"),
              by = "day"
            )),
            all = TRUE
          )
          (length(joined[ ,value][!is.na(joined[ ,value])]) / length(joined[ ,value])) * 100
        },
        
        Remarks = ""
        
      )
      
      return(dt)
      
    }
    
    
    # Function to extract days with missing data of month of each year
    missingDays <- function(data) {
      split(
        data, 
        as.factor(format(data[ ,Date], format = "%Y-%m"))
      ) |> 
        lapply(\(data) data[ ,c("Date", "Distr", "Reg", "Geogr1", "Geogr2")]) |> . =>
        do.call("rbind", .)
    }    
    
    
    # Path for output files
    path <- "For Windows Systems/outputs"
    
    writeToWorkbook <- function(vec, list, var) {
      
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
        file = file.path(path, var, paste(vec, "xlsx", sep = ".")), 
        sheetName = names(reg), 
        rowNames = FALSE
      )
      
    }
    
    
    
  }
  
)


#===========================Extracting Missing Days============================


# Data Reshaping to long format ####
# Reshaping Prcp
dataReshapedRR <- parallel::clusterApply(
  cl_RR,
  idsRR, 
  \(vec, list) lD_list(vec, list), 
  list = RR_split
) |> . =>
  do.call("c", .)

# #Exporting dataset "dataReshapedRR" to all nodes
#parallel::clusterExport(cl_RR, c("dataReshapedRR"))

# Populating missing dates in each dataTable/Station from the output above
dataReshapedRRDone <- parallel::clusterApply(
  cl_RR,
  idsRR,
  \(vec, data) {
    data[vec] |> 
      lapply(\(dataTable, Time) PopMisnDate(dataTable, Time), "Year")
  },
  data = dataReshapedRR
)  |> . =>
  do.call("c", .)

# #EXporting dataset to all nodes of cl_RR
#parallel::clusterExport(cl_RR, c("dataReshapedRRDone"))

# Profiling Rainfall Data ####
# Computing data availability for each Station
Profile_StationsRR <- parallel::clusterApply(
  cl_RR,
  idsRR,
  # Anaymous Function which calls "Profile" for every node
  \(vec, list) {
    list[vec] |> 
      lapply(
        \(data) Profile(data)
      )
  },
  list = dataReshapedRRDone
) |> . =>
  do.call("c", .) |> . =>
  do.call("rbind", .)

# Writing to disk
write.csv(
  Profile_StationsRR, 
  file =  paste(path, "RR/ProfiledStations_RR.csv", sep = "/"), 
  row.names = FALSE
)

# Days with missing data for each Station ####
# Extracting missing days for each Station
parallel::clusterApply(
  cl_RR,
  idsRR,
  \(vec, list){
    list[vec] |> 
      lapply(\(data) data[is.na(value), ])
  },
  list = dataReshapedRRDone
) |> . =>
  do.call("c", .) -> profileMissingRR
#
profileMissingRR <- (parallel::clusterApply(
  cl_RR,
  idsRR,
  \(vec, list){
    list[vec] |> 
      lapply(\(data) missingDays(data))
  },
  list = profileMissingRR
) |> . =>
  do.call("c", .)) 

# Writing to disk as excel workbooks as a function of Regions and respective Districts ####
# Vector of all 16 Regions in Ghana
regions <- c(
  "Upper West", "Upper East", "Northern Region", "Savannah", 
  "Ahafo", "Bono", "Bono East", "Ashanti", "Eastern", "Central", 
  "North East", "Oti", "Western North", "Volta", "Greater Accra", "Western"
)

profileMissingRR |> 
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
parallel::clusterApplyLB(
  cl_RR,
  regions,
  \(vec, list, var) writeToWorkbook(vec, list, var),
  list = profMssRegRR,
  var = "RR"
)


# Stop Cluster
parallel::stopCluster(cl_RR)



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







