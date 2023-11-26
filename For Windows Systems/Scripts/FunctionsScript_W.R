############################################ FUNCTIONS ################################################################3
# Enabling the Pipebind Operator and other dependencies
#Sys.setenv("_R_USE_PIPEBIND_" = "true")
require(magrittr)


# cores for computation
nCores <- function() {
  
  availCores <- parallelly::availableCores()
  nc <- as.integer(readline(
    prompt = sprintf("Enter no. of compute nodes (must be > 1 and < %i) :", availCores)
  ))
  
  output <- ifelse(
    nc >= availCores | nc <= 1,
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
      Dry = ifelse(dat$Status == "Dry", "Dry", NA)
      Missing = ifelse(dat$Status == "Missing", "Missing", NA)
      Available = ifelse(dat$Status == "Available", "Available", NA)
    }
  ) 
  
  # # Plotting ##
  rr <- ggplot(data = dat, aes(x = as.numeric(format(dat$Date, "%Y")))) +
    geom_point(
      data = data.table::merge.data.table(
        data.table::data.table(Date = seq(as.Date(range(dat$Date)[1]), as.Date(range(dat$Date)[2]), by = "day")),
        subset(dat, Status == "Dry"),
        by = "Date",
        all = TRUE
      ) |> dplyr::arrange(Date),
      aes(y = Day, col = "Dry")
    ) +
    geom_point(
      data = data.table::merge.data.table(
        data.table::data.table(Date = seq(as.Date(range(dat$Date)[1]), as.Date(range(dat$Date)[2]), by = "day")),
        subset(dat, Status == "Missing"),
        by = "Date",
        all = TRUE
      ) |> dplyr::arrange(Date),
      aes(y = Day, col = "Missing")
    ) +
    geom_point(
      data = data.table::merge.data.table(
        data.table::data.table(Date = seq(as.Date(range(dat$Date)[1]), as.Date(range(dat$Date)[2]), by = "day")),
        subset(dat, Status == "Available"),
        by = "Date",
        all = TRUE
      ) |> dplyr::arrange(Date),
      aes(y = Day, col = "Available")
    ) +
    scale_color_manual(
      "Legend",
      values = c("Dry" = "brown", "Missing" = "grey", "Available" = "darkblue")
    ) +
    labs(
      title = paste("Rainfall", StationName_ID, sep = " ") ,
      x = "Day of Year",
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
    theme(legend.text = element_text(size = 11),
          axis.text = element_text(size = 11),
          axis.title = element_text(size = 14, face = "italic")) +
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
      data = dplyr::full_join(
        data.table::data.table(Date = seq(as.Date(range(dat$Date)[1]), as.Date(range(dat$Date)[2]), by = "day")),
        subset(dat, Status == "Available"),
        by = c("Date" = "Date")
      ) |> dplyr::arrange(Date), 
      aes(y = Day, col = "Available")
    ) +
    geom_point(
      data = dplyr::full_join(
        data.table::data.table(Date = seq(as.Date(range(dat$Date)[1]), as.Date(range(dat$Date)[2]), by = "day")),
        subset(dat, Status == "Missing"),
        by = c("Date" = "Date")
      ) |> dplyr::arrange(Date), 
      aes(y = Day, col = "Missing")
    ) +
    scale_color_manual(
      "Legend",
      values = c("Missing" = "grey", "Available" = "firebrick")
    ) +
    labs(title = paste(var, StationName_ID, sep = " "), 
         x = "Year", 
         y = "Day of the year") +
    scale_x_continuous(
      breaks = seq(
        min(as.numeric(format(dat$Date, "%Y"))), 
        max(as.numeric(format(dat$Date, "%Y"))), 
        by = 10
      )
    ) +
    scale_y_continuous(breaks = seq(0, 366, 50)) +
    theme_classic() +
    theme(legend.text = element_text(size = 11),
          axis.text = element_text(size = 11),
          axis.title = element_text(size = 14, face = "italic")) +
    guides(colour = guide_legend(override.aes = list(size=3)))
  
  # return value
  return(tm)
  
}
