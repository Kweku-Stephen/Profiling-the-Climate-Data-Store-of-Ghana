######################################################################################################
source("For Linux_Unix Systems/Scripts/FunctionsScript.R")



lapply(
	names(a),
	\(x) {
		if (identical(a[[x]], a[[1]])) {
			
			sink(file = "Dist_Stations.txt", append = FALSE)
			a[[x]]
			sink(file = NULL)
			
		} else {
			
			sink(file = "Dist_Staions.txt", append = TRUE)
			a[[x]]
			sink(file = NULL)
			
		}
	}
)


# Using For Lopps with if statements
sta <- character(length = length(b$value))

for (i in 1:length(b$value)) {
	
	if (is.na(b$value[i])) {
		sta[i] <- "missing"
	} else if (b$value[i] == 0) {
		sta[i] <- "Dry"
	} else {
		sta[i] <- "Available"
	}
	
}



# Visualizing missing, and Available data ####
b = subset(dataReshapedRRDone$`Asante Krom (Dodi)_07075ASA`, Date <= format(Sys.time(), "%Y-%m-%d")) |> . =>
	dplyr::full_join(
		data.table::data.table(Date = seq(as.Date(range(.[ ,Date])[1]), as.Date(format(Sys.time(), "%Y-%m-%d")), by = "day")),
		.,
		by = c("Date" = "Date")
	)

# categorizing values
b$status <- sapply(
	b$value,
	\(x) {
		if(is.na(x)) {
			"missing"
		} else if (x == 0) {
			 "Dry"
		} else {
			"Available"
		}
	}
)

b = within(b, {D = as.numeric(strftime(b$Date, format = "%j"))})


# cl = sapply(
# 	b$value,
# 	\(x) {
# 		if(is.na(x)) {
# 			"black"
# 		} else if (x == 0) {
# 			"brown"
# 		} else {
# 			"blue"
# 		}
# 	}
# )

b %<>% base::within(
	{
		Dry = ifelse(b$status == "Dry", "Dry", NA)
		missing = ifelse(b$status == "missing", "missing", NA)
		Available = ifelse(b$status == "Available", "Available", NA)
	}
)


# b$Available = ifelse(b$status == "Available", "Available", NA)
# b$missing = ifelse(b$status == "missing", "missing", NA)
# b$Dry = ifelse(b$status == "Dry", "Dry", NA)

rr <- ggplot(data = b, aes(x = as.numeric(format(b$Date, "%Y")))) +
	geom_point(
		data = dplyr::full_join(
			data.table::data.table(Date = seq(as.Date(range(b$Date)[1]), as.Date(range(b$Date)[2]), by = "day")),
			subset(b, status == "Dry"),
			by = c("Date" = "Date")
		) |> dplyr::arrange(Date), 
		aes(y = D, col = "Dry")
	) +
	geom_point(
		data = dplyr::full_join(
			data.table::data.table(Date = seq(as.Date(range(b$Date)[1]), as.Date(range(b$Date)[2]), by = "day")),
			subset(b, status == "missing"),
			by = c("Date" = "Date")
		) |> dplyr::arrange(Date), 
		aes(y = D, col = "missing")
	) +
	geom_point(
		data = dplyr::full_join(
			data.table::data.table(Date = seq(as.Date(range(b$Date)[1]), as.Date(range(b$Date)[2]), by = "day")),
			subset(b, status == "Available"),
			by = c("Date" = "Date")
		) |> dplyr::arrange(Date), 
		aes(y = D, col = "Available")
	) +
	scale_color_manual(
		"Legend",
		values = c("Dry" = "brown", "missing" = "grey", "Available" = "darkblue")
	) +
	labs(title = "Rainfall Asante Krom (Dodi)_07075ASA" ,x = "Day of Year", y = "Day of the Year") +
	scale_x_continuous(
		breaks = seq(
			min(as.numeric(format(b$Date, "%Y"))), 
			max(as.numeric(format(b$Date, "%Y"))), 
			by = 10
		)
	) +
  scale_y_continuous(breaks = seq(0, 366, 50)) +
	theme_classic() +
	theme(legend.text = element_text(size = 11),
			axis.text = element_text(size = 11),
			axis.title = element_text(size = 14, face = "italic")) +
	guides(colour = guide_legend(override.aes = list(size=3)))




Rcpp::cppFunction("

    IntegerVector YearDay (DateVector date) {
        // Declaring an empty integer vector
        IntegerVector res;

        // iterating over the elements of date extract the nth day of the year
        for (int i = 0; i < date.size(); i++) {
   			Date d = date[i]
            res[i] = d.getYearday();
        }

        return res;
    }

")
