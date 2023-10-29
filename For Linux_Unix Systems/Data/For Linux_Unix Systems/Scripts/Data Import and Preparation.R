# Invoking pipebind operator
Sys.setenv("_R_USE_PIPEBIND_" = "true")

# Dependencies
library(magrittr)

# Directory for data outputs
dir.create("outputs")

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
  data.table::fread(header = TRUE)

# # Adding Station's Region and District Data
data %<>% lapply(
  .,
  \(dataTable) {

    res <- dplyr::left_join(
      dataTable,
      regionDistricts,
      by = c("Name" = "StationName"),
      multiple = "all"
    )
    return(res)
  }
) 

# Adding a new column comprising Station name and Station ID to all 3 datasets ####
data %<>% lapply(
  .,
  \(dataTable){
    dataTable[ ,StationName_ID := paste(dataTable[ ,Name], dataTable[ ,`Eg Gh Id`], sep = "_")]
  }
)


# Splitting each dataset of the list "data" by "StationName_ID" ####
dataSplit <- parallel::mclapply(
  data,
  \(data) split(data, as.factor(data[ ,StationName_ID])),
  mc.cores = length(data)
)







# Holding on For Trace
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
