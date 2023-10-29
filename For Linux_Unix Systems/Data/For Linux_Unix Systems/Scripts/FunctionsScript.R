################################################ FUNCTIONS ################################################################3
# # Testing for sufficient cores for computation
# nCores <- function() {
#   
#   nc <- as.integer(readline(prompt = "number of Cores for Computation: "))
#   avCores <- parallelly::availableCores()
#   
#   output <- ifelse(
#     nc > avCores,
#     stop("Number of Cores must be less than or eaual to  ", avCores),
#     nc
#   )
#   
#   
#   return(output)
#   
# }



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

