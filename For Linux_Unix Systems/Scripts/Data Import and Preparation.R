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





# Invoking pipebind operator
Sys.setenv("_R_USE_PIPEBIND_" = "true")

# Dependencies
library(magrittr)

# Reading in Datasets into Memory ####
dir(path = "For Linux_Unix Systems/Data/", pattern = ".txt",full.names = TRUE) |>
  lapply(
    data.table::fread,
    header = TRUE,
    sep = "\t",
    na.strings = c("9999", "-9999", "9988", "-9988", "-99.9", "99.9", "99", "-99")
  ) |>
  setNames(dir(path = "For Linux_Unix Systems/Data/", pattern = ".txt")) -> data


# Importing Districts and Regions Data
regionDistricts <- dir(
  path = "For Linux_Unix Systems/Data", 
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


#########################################################################

# checking for Duplicates in the data ####
# Rainfall ####
# Removing of duplicated rows (which is a months data)

# Parallelizing computation
# Calling Duplicated on the entire dataframe wont return the duplicates as expected
# because one of the duplicates may have a space or metacharacter between characters
parallel::mclapply(
  parallel::splitIndices(nrow(data$`Daily-RR-All-Stations Sheet 1.txt`), 4),
  fun = \(vec) {
    data$`Daily-RR-All-Stations Sheet 1.txt`[vec, ] |> 
      apply(
        1,
        FUN = \(vec) {
          as.character(vec) |> 
            paste(collapse = "") |> . =>
            gsub("[[:punct:]]|[ \t\n\r\f\v]", "", .)
        }
      )
  },
  mc.cores = 6
) |> . =>
  do.call("c", .) |> . =>
  subset(
    data$`Daily-RR-All-Stations Sheet 1.txt`,
    !duplicated(.)
  ) -> data$`Daily-RR-All-Stations Sheet 1.txt`


# Maximum Temperature
apply(
  data$`Daily-Tx-All-Stations Sheet 1.txt`, 
  1, 
  FUN = \(vec) {
    as.character(vec) |>
      paste(collapse = "") |> . =>
      gsub("[[:punct:]]|[ \t\n\r\f\v]", "", .)
  }
) |> . =>
  subset(
    data$`Daily-Tx-All-Stations Sheet 1.txt`,
    !duplicated(.)
  ) -> data$`Daily-Tx-All-Stations Sheet 1.txt`


# Minimum Temperature
apply(
  data$`Daily-Tn-All-Stations Sheet 1.txt`, 
  1, 
  FUN = \(vec) {
    as.character(vec) |>
      paste(collapse = "") |> . =>
      gsub("[[:punct:]]|[ \t\n\r\f\v]", "", .)
  }
) |> . =>
  subset(
    data$`Daily-Tn-All-Stations Sheet 1.txt`,
    !duplicated(.)
  ) -> data$`Daily-Tn-All-Stations Sheet 1.txt`


################################################################################################################

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


# Splitting each dataset of the list "data" by "StationName_ID" ####
dataSplit <- parallel::mclapply(
  data,
  \(data) split(data, as.factor(data[ ,StationName_ID])),
  mc.cores = length(data)
)



# Holding on For Trace ####
# Checking and Setting all "Trace" to 0
# parallel::clusterApply(
#   cl2,
#   ids,
#   # Anonymous Function calling setTrace0
#   \(vec = "") {
#     dt <- dataReshaped[vec]
#     lapply(
#       dt,
#       setTrace0,
#       var = "Prcp"
#     )
#   }
# )
