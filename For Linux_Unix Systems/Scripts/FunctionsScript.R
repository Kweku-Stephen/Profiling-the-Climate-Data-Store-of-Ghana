#=====================================================================================================================
#              GSSTI - GMET - GNAPP
# Digitizing Climatological Paper Archives in Ghana: 
# 
# Project: Digitizing Climatological Paper Archives in Ghana: Consultancy Service to support the Ghana Meteorololigcal Agency in Digitizing Climatological Paper Archives in Ghana
# Code: Profiling entire Rainfall Data of Ghana
# Variable: Minimum Temperature
# Written by: Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Edited by:  Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Objective: This code profiles minimum temperature data Store of Ghana as a function of their respective districts.
#           it returns the start and end dates, missing  days, days with no recorded min temperature, duplicated stations names, duplicated stations IDs amongst
#           other duplicates.               



################################################ FUNCTIONS ################################################################3
# Enabling the Pipebind Operator and other dependencies
Sys.setenv("_R_USE_PIPEBIND_" = "true")
require(magrittr)




# Computation Cores
# cores for computation
nCores <- function() {
  
  availCores <- parallelly::availableCores()
  nc <- as.integer(readline(
    prompt = sprintf("Enter no. of compute nodes (must be > 1 and < %i) :", availCores)
  ))
  
  output <- ifelse(
    nc > availCores | nc <= 0,
    stop(sprintf("Number of cores must be > 1 and < %i", availCores)),
    nc
  )
  
  
  return(output)
  
}



# Start and End Year of Station
startEndYear <- function(list) {
  
  list |>
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
    .[order(.[ ,Station]), ] -> res
  
  return(res)
  
}



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
    `Available Years` = length(unique(data$Year)[!is.na(unique(data$Year))]),
    Actual_no_of_Years = length(min(data[ ,Year], na.rm = TRUE):max(data[ ,Year], na.rm = TRUE)),
    
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



# # Duplicates ####
Duplicates <- function(data) {
  
  Duplicates <- list(
    
    # Station ID Duplicates
    ID_Duplicates = {data |> . =>
        .[duplicated(.[ ,"ID"]), ][ ,"ID"] |> 
        unlist() -> dupsID
      
      data |>
        subset(ID %in% dupsID) },
    
    
    # Multiple Stations at the same town but different IDs
    OneTown_DiffIDs = {data |> . =>
        .[duplicated(.[ ,"StationName"]), ][ ,StationName] -> dupsName
      
      data |> 
        subset(StationName %in% dupsName) |> . =>
        .[!duplicated(.[ ,ID]), ]},
    
    
    # Same Stations different/Same IDs but with Same Station Type
    OneTown_ID_cords_Years = {data |> . =>
        .[duplicated(.[ ,"StationName"]), ][ ,StationName] -> dupsName
      
      data |> 
        subset(StationName %in% dupsName) |> . =>
        .[!duplicated(.[ ,ID]), ] -> a
      
      split(a, as.factor(a$StationName)) -> aa
      
      dp <- function(data) {
        subset(data, duplicated(Type))[ ,Type] -> name
        subset(data, Type %in% name)
      }
      
      lapply(aa, dp) |> . => do.call("rbind", .)}
    
  )
  
  return(Duplicates)
  
}




# Data Integrity visualization ####
RRdataIntegVis <- function(reshapedData, StationName_ID = "") {
  
  dat <- subset(reshapedData[[StationName_ID]], Date <= format(Sys.time(), "%Y-%m-%d")) |> . =>
    data.table::merge.data.table(
      data.table::data.table(
        Date = seq.Date(
          as.Date(range(.[ ,Date])[1]), as.Date(format(Sys.time(), "%Y-%m-%d")),
          by = "day"
        )
      ),
      .,
      by = "Date",
      all = TRUE
    )
  
  # Categorizing values
  dat$Status <- sapply(
    dat$value, 
    \(x) {
      if (is.na(x)) {
        "Missing"
      } else if (x == 0) {
        "Dry"
      } else {
        "Wet"
      }
    }
  )
  
  # Adding nth Day Dates as a column
  dat <- within(
    dat,
    {Day = as.numeric(strftime(dat$Date, format = "%j"))}
  )
  
  # Adding Status columns for each element of the status variable
  dat %<>% base::within(
    {
      Dry = ifelse(dat$Status == "Dry", "Dry", NA)
      Missing = ifelse(dat$Status == "Missing", "Missing", NA)
      Available = ifelse(dat$Status == "Wet", "Wet", NA)
    }
  ) 
  
  # Plotting
  rr <- ggplot(data = dat, aes(x = as.numeric(format(dat$Date, "%Y")))) +
    geom_point(
      data = data.table::merge.data.table(
        data.table::data.table(Date = seq(as.Date(range(dat$Date)[1]), as.Date(range(dat$Date)[2]), by = "day")),
        subset(dat, Status == "Dry"),
        by = "Date",
        all = TRUE
      ) |> data.table::setorderv(cols = "Date"), 
      aes(y = Day, col = "Dry")
    ) +
    geom_point(
      data = data.table::merge.data.table(
        data.table::data.table(Date = seq(as.Date(range(dat$Date)[1]), as.Date(range(dat$Date)[2]), by = "day")),
        subset(dat, Status == "Missing"),
        by = "Date",
        all = TRUE
      ) |> data.table::setorderv(cols = "Date"), 
      aes(y = Day, col = "Missing")
    ) +
    geom_point(
      data = data.table::merge.data.table(
        data.table::data.table(Date = seq(as.Date(range(dat$Date)[1]), as.Date(range(dat$Date)[2]), by = "day")),
        subset(dat, Status == "Wet"),
        by = "Date",
        all = TRUE
      ) |> data.table::setorderv(cols = "Date"), 
      aes(y = Day, col = "Wet")
    ) +
    scale_color_manual(
      "Legend",
      values = c("Dry" = "brown", "Missing" = "grey", "Wet" = "darkblue")
    ) +
    labs(
      title = paste("Rainfall", StationName_ID, sep = " ") ,
      x = "Year", 
      y = "Day of the Year"
    ) +
    scale_x_continuous(
      breaks = seq(
        min(as.numeric(format(dat$Date, "%Y"))), 
        max(as.numeric(format(dat$Date, "%Y"))), 
        by = 10
      )
    ) +
    scale_y_continuous(breaks = seq(0, 366, 50)) +
    theme_classic() +
    theme(legend.text = element_text(size = 14),
          legend.title = element_text(size = 16, face = "bold"),
          plot.title = element_text(size = 20, face = "bold", hjust = 0),
          axis.text = element_text(size = 16),
          axis.title = element_text(size = 18, face = "italic")) +
    guides(colour = guide_legend(override.aes = list(size=3)))
  
  # return value
  return(rr)
}


# Temperature ####
TMdataIntegVis <- function(reshapedData, StationName_ID = "", var = "") {
  
  dat <- subset(reshapedData[[StationName_ID]], Date <= format(Sys.time(), "%Y-%m-%d")) |> . =>
    data.table::merge.data.table(
      data.table::data.table(
        Date = seq.Date(
          as.Date(range(.[ ,Date])[1]), as.Date(format(Sys.time(), "%Y-%m-%d")),
          by = "day"
        )
      ),
      .,
      by = "Date",
      all = TRUE
    )
  
  # Categorizing values
  dat$Status <- sapply(
    dat$value, 
    \(x) {
      if (is.na(x)) {
        "Missing"
      }  else {
        "Available"
      }
    }
  )
  
  # Adding nth Day Dates as a column
  dat <- within(
    dat,
    {Day = as.numeric(strftime(dat$Date, format = "%j"))}
  )
  
  # Adding Status columns for each element of the status variable
  dat %<>% base::within(
    {
      Missing = ifelse(dat$Status == "Missing", "Missing", NA)
      Available = ifelse(dat$Status == "Available", "Available", NA)
    }
  )
  
  
  # Plotting
  tm <- ggplot(data = dat, aes(x = as.numeric(format(dat$Date, "%Y")))) +
    geom_point(
      data = data.table::merge.data.table(
        data.table::data.table(Date = seq(as.Date(range(dat$Date)[1]), as.Date(range(dat$Date)[2]), by = "day")),
        subset(dat, Status == "Missing"),
        by = "Date",
        all = TRUE
      ) |> data.table::setorderv(cols = "Date"), 
      aes(y = Day, col = "Missing")
    ) +
    geom_point(
      data = data.table::merge.data.table(
        data.table::data.table(Date = seq(as.Date(range(dat$Date)[1]), as.Date(range(dat$Date)[2]), by = "day")),
        subset(dat, Status == "Available"),
        by = "Date",
        all = TRUE
      ) |> data.table::setorderv(cols = "Date"), 
      aes(y = Day, col = "Available")
    ) +
    scale_color_manual(
      "Legend",
      values = c("Available" = "brown", "Missing" = "grey")
    ) +
    labs(
      title = paste(var, StationName_ID, sep = " ") ,
      x = "Year", 
      y = "Day of the Year"
    ) +
    scale_x_continuous(
      breaks = seq(
        min(as.numeric(format(dat$Date, "%Y"))), 
        max(as.numeric(format(dat$Date, "%Y"))), 
        by = 10
      )
    ) +
    scale_y_continuous(breaks = seq(0, 366, 50)) +
    theme_classic() +
    theme(legend.text = element_text(size = 14),
          legend.title = element_text(size = 16, face = "bold"),
          plot.title = element_text(size = 20, face = "bold", hjust = 0),
          axis.text = element_text(size = 16),
          axis.title = element_text(size = 18, face = "italic"))  +
    guides(colour = guide_legend(override.aes = list(size=3)))
  
  # return value
  return(tm)
}
