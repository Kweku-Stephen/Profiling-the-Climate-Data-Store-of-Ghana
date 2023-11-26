#=====================================================================================================================
#                           GSSTI - GMET - 
# Digitizing Climatological Paper Archives of Ghana: 
# 
# Project: Digitizing Climatological Paper Archives in Ghana: Consultancy Service to support the Ghana Meteorololigcal Agency in Digitizing Climatological Paper Archives in Ghana
# Code: Data Import, cleaning and Preparation for Profiling
# Code Written by: Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Edited by:  Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Objective:  This code imports all Rainfall and Temperature datasets accross the country, cleans
#             the data (removing duplicates and adding each station's respective district) to make Rainfall,
#             and Temperature ready for Profiling.


# Software Version
# R Version::4.2.3


#========================Reading in Datasets into Memory========================####
# Invoking pipebind operator
Sys.setenv("_R_USE_PIPEBIND_" = "true")

# Dependencies
pkgs <- c("magrittr", "dplyr", "parallelly", "ggplot2", "data.table", "rio")

if(length(setdiff(pkgs, installed.packages())) > 0) {
  install.packages(pkgs, dependencies = TRUE)
  sapply(pkgs, require, character.only = TRUE)
} else {
  sapply(pkgs, require, character.only = TRUE)
}


# Reading in Datasets into Memory ####
data <- dir(path = "For Windows Systems/Data/", pattern = ".txt",full.names = TRUE) |>
  lapply(
    data.table::fread,
    header = TRUE,
    sep = "\t",
    na.strings = c("9999", "-9999", "9988", "-9988", "-99.9", "99.9", "99", "-99")
  ) |>
  setNames(dir(path = "For Windows Systems/Data/", pattern = ".txt")) 


# Importing Districts and Regions Data
regionDistricts <- dir(
  path = "For Windows Systems/Data", 
  pattern = "regionmerge1.csv", 
  full.names = TRUE
) |> 
  data.table::fread(header = TRUE) |> 
  subset(!duplicated(StationName)) |> 
  dplyr::select(-c("lon", "lat"))

# # Adding Station's Region and District Data
data %<>% lapply(
  .,
  \(dataTable) {
    
    res <- dplyr::left_join(
      dataTable,
      regionDistricts,
      by = c("Name" = "StationName")
    )
    return(res)
  }
) 


#============================================================================

# checking for Duplicates in the data ####
# Rainfall ####
# Removing of duplicated rows (which is a months data)

# Parallelizing computation
# Calling Duplicated on the entire dataframe wont return the duplicates as expected
# because one of the duplicates may have a space or metacharacter between characters


# Creating a cluster of size ncores
source("For Windows Systems/Scripts/FunctionsScript_W.R")
# Compute nodes
ncores <- nCores()

# cluster of size, ncores
cl <- parallel::makeCluster(
  spec = ncores,
  type = "PSOCK"
)

# # Indexes / main arguments to the foreach/ clusterApply function
# chuncks <- parallel::splitIndices(nrow(data$`Daily-RR-All-Stations Sheet 1.txt`), 4)
# 
#


# #========================Removing duplicates=================================####


# 
# #
# pste <- function(vec, var = "") {
#   data[[grep(var, names(data), value = TRUE)]][vec, ] |>
#     apply(
#       1,
#       FUN = \(vec) {
#         as.character(vec) |>
#           paste(collapse = "") |> . =>
#           gsub("[[:punct:]]|[ \t\n\r\f\v]", "", .)
#       }
#     )
# }
# 
# # Rainfall ##
# # concise and compact but slower
# doParallel::registerDoParallel(cl) # Registering the parallel backend
# # Rainfall 
# data$`Daily-RR-All-Stations Sheet 1.txt` <- foreach::foreach(
#   i = chuncks, .combine = "c"
# ) %dopar% 
#   pste(vec = i, var = "RR") |> . =>
#   subset(data$`Daily-RR-All-Stations Sheet 1.txt`, !(duplicated(.)))
# 
# # Maximum Temperature
# # slow but compact and concise
# data$`Daily-Tx-All-Stations Sheet 1.txt` <- foreach(
#   i = 1:nrow(data$`Daily-Tx-All-Stations Sheet 1.txt`), .combine = "c"
# ) %do%
#   pste(vec = i, var = "Tx") 
# 
# # Minimum Temperature
# # slow but compact and concise
# data$`Daily-Tn-All-Stations Sheet 1.txt` <- foreach(
#   i = 1:nrow(data$`Daily-Tx-All-Stations Sheet 1.txt`), .combine = "c"
# ) %do%
#   pste(vec = i, var = "Tn") 



# #Rainfall ####

dups <- parallel::clusterApply(
  # cluster
  cl,
  # chunking on worker side
  parallel::splitIndices(nrow(data[[grep("RR|Rr|rr", names(data), value = TRUE)]]), 4),
  # Ananymous function for pasting elements of all rows together as one string
  fun = \(vec, data) {
    data[vec, ] |>
      apply(
        1,
        FUN = \(vec) {
          as.character(vec) |>
            paste(collapse = "") |> . =>
            gsub("[[:punct:]]|[ \t\n\r\f\v]", "", .)
        }
      )
  },
  # second argument "data" to the anonymous function
  data = data[[grep("RR|Rr|rr", names(data), value = TRUE)]]
) |> . =>
  do.call("c", .) 

# Stopping Cluster
parallel::stopCluster(cl)

#
data[[grep("RR|Rr|rr", names(data), value = TRUE)]] |> . =>
  if(nrow(.[duplicated(dups), ]) == 0) {
  "0 duplicates found for Rainfall"
} else {
  sprintf(
    "%i duplicates identified for Rainfall", nrow(.[duplicated(dups), ])
  )
}

#
data[[grep("RR|Rr|rr", names(data), value = TRUE)]] <-  data[[grep("RR|Rr|rr", names(data), value = TRUE)]][!duplicated(dups), ]




# Maximum Temperature ####

dupsMX <- apply(
  data[[grep("TX|Tx", names(data), value = TRUE)]],
  1,
  FUN = \(vec) {
    as.character(vec) |>
      paste(collapse = "") |> . =>
      gsub("[[:punct:]]|[ \t\n\r\f\v]", "", .)
  }
) 
#
data[[grep("TX|Tx", names(data), value = TRUE)]] |> . =>
  if(nrow(.[duplicated(dupsMX), ]) == 0) {
    "0 duplicates found for Max Temp"
  } else {
    sprintf(
      "%i duplicates identified for Max Temp", nrow(.[duplicated(dupsMX), ])
    )
  }

#  
data[[grep("TX|Tx", names(data), value = TRUE)]] <- data[[grep("TX|Tx", names(data), value = TRUE)]][!duplicated(dupsMX), ]



# Minimum Temperature ####

dupsMN <- apply(
  data[[grep("TN|Tn|tn", names(data), value = TRUE)]],
  1,
  FUN = \(vec) {
    as.character(vec) |>
      paste(collapse = "") |> . =>
      gsub("[[:punct:]]|[ \t\n\r\f\v]", "", .)
  }
)
#
data[[grep("TN|Tn|tn", names(data), value = TRUE)]] |> . =>
  if(nrow(.[duplicated(dupsMN), ]) == 0) {
    "0 duplicates found for Min Temp"
  } else {
    sprintf(
      "%i duplicates identified for Min Temp", nrow(.[duplicated(dupsMN), ])
    )
  }
#
data[[grep("TN|Tn|tn", names(data), value = TRUE)]] <- data[[grep("TN|Tn|tn", names(data), value = TRUE)]][!duplicated(dupsMN), ]



#====================================================================================




# Adding a new column comprising Station name and Station ID ####
# This code is subject to review and improvement ####
# Adding a new column comprising Station name and Station ID to all 3 datasets ####
suppressWarnings(
  data %<>% lapply(
    .,
    \(dataTable){
      dataTable[ ,StationName_ID := paste(dataTable[ ,Name], dataTable[ ,`Eg Gh Id`], sep = "_")]
    }
  ), 
  classes = "warning"
)

# data %<>% lapply(
#   \(data) data[ ,StationName_ID := with(data, paste(Name, `Eg Gh Id`, sep = "_"))][]
# )


# Splitting each element of the list "data" by the variable "StationName_ID"
dataSplit <- list(
  
  Rainfall = data[[grep("RR|Rr|rr", names(data), names(data), value = TRUE)]] |> . =>
    split(., as.factor(.[ ,StationName_ID])),
  
  TMax = data[[grep("TX|Tx", names(data), names(data), value = TRUE)]] |> . =>
    split(., as.factor(.[ ,StationName_ID])),
  
  TMin = data[[grep("TN|Tn|tn", names(data), names(data), value = TRUE)]] |> . =>
    split(., as.factor(.[ ,StationName_ID]))
  
)


#======================================================================================
#======================================================================================
#
# Bencmarking sequential splitting and parallel splitting using foreach
# require(foreach)
# registerDoParallel(cores = 3)
# 
# microbenchmark::microbenchmark(
#   foreach_parallel = foreach::foreach(
#     dt = data,
#     fct = sapply(data, \(dtb) grep("StationName_ID", colnames(dtb), value = TRUE))
#   ) %dopar%
#     split(
#       dt,
#       as.factor(dt[ ,fct])
#     ),
#   
#   Base_Sequential = list(
#     
#     Rainfall = data$`Daily-RR-All-Stations Sheet 1.txt` |> . =>
#       split(., as.factor(.[ ,StationName_ID])),
#     
#     TMax = data$`Daily-Tx-All-Stations Sheet 1.txt` |> . =>
#       split(., as.factor(.[ ,StationName_ID])),
#     
#     TMin = data$`Daily-Tn-All-Stations Sheet 1.txt` |> . =>
#       split(., as.factor(.[ ,StationName_ID]))
#     
#   ),
#   
#   times = 5
# )


