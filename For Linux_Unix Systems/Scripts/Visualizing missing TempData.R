


# TMax ###############################################################################################
# Visualizing missing, and Available data ####
c = subset(dataReshapedTXDone$`Asante Krom (Dodi)_07075ASA`, Date <= format(Sys.time(), "%Y-%m-%d")) |> . =>
	dplyr::full_join(
		data.table::data.table(Date = seq(as.Date(range(.[ ,Date])[1]), as.Date(format(Sys.time(), "%Y-%m-%d")), by = "day")),
		.,
		by = c("Date" = "Date")
	)

# categorizing values
c$status <- sapply(
	c$value,
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

c = within(c, {D = as.numeric(strftime(c$Date, format = "%j"))})


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


c %<>% base::within(
	{
		#Dry = ifelse(b$status == "Dry", "Dry", NA)
		missing = ifelse(c$status == "missing", "missing", NA)
		Available = ifelse(c$status == "Available", "Available", NA)
	}
)

tx <- ggplot(data = c, aes(x = as.numeric(format(c$Date, "%Y")))) +
	geom_point(
		data = dplyr::full_join(
			data.table::data.table(Date = seq(as.Date(range(c$Date)[1]), as.Date(range(c$Date)[2]), by = "day")),
			subset(c, status == "Available"),
			by = c("Date" = "Date")
		) |> dplyr::arrange(Date), 
		aes(y = D, col = "Available")
	) +
	geom_point(
		data = dplyr::full_join(
			data.table::data.table(Date = seq(as.Date(range(c$Date)[1]), as.Date(range(c$Date)[2]), by = "day")),
			subset(c, status == "missing"),
			by = c("Date" = "Date")
		) |> dplyr::arrange(Date), 
		aes(y = D, col = "missing")
	) +
	scale_color_manual(
		"Legend",
		values = c("missing" = "grey", "Available" = "firebrick")
	) +
	labs(title = "TMax Asante Krom (Dodi)_07075ASA", x = "Year", y = "Day of the year") +
	scale_x_continuous(
		breaks = seq(
			min(as.numeric(format(c$Date, "%Y"))), 
			max(as.numeric(format(c$Date, "%Y"))), 
			by = 10
		)
	) +
  scale_y_continuous(breaks = seq(0, 366, 50)) +
	theme_classic() +
	theme(legend.text = element_text(size = 11),
			axis.text = element_text(size = 11),
			axis.title = element_text(size = 14, face = "italic")) +
	guides(colour = guide_legend(override.aes = list(size=3)))







# TMin ####################################################################
# Visualizing missing, and Available data ####
d = subset(dataReshapedTNDone$`Asante Krom (Dodi)_07075ASA`, Date <= format(Sys.time(), "%Y-%m-%d")) |> . =>
  dplyr::full_join(
    data.table::data.table(Date = seq(as.Date(range(.[ ,Date])[1]), as.Date(format(Sys.time(), "%Y-%m-%d")), by = "day")),
    .,
    by = c("Date" = "Date")
  )

# categorizing values
d$status <- sapply(
  d$value,
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

d = within(d, {D = as.numeric(strftime(d$Date, format = "%j"))})


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

d %<>% base::within(
  {
    #Dry = ifelse(b$status == "Dry", "Dry", NA)
    missing = ifelse(d$status == "missing", "missing", NA)
    Available = ifelse(d$status == "Available", "Available", NA)
  }
)

tn <- ggplot(data = d, aes(x = as.numeric(format(d$Date, "%Y")))) +
  geom_point(
    data = dplyr::full_join(
      data.table::data.table(Date = seq(as.Date(range(d$Date)[1]), as.Date(range(d$Date)[2]), by = "day")),
      subset(d, status == "Available"),
      by = c("Date" = "Date")
    ) |> dplyr::arrange(Date), 
    aes(y = D, col = "Available")
  ) +
  geom_point(
    data = dplyr::full_join(
      data.table::data.table(Date = seq(as.Date(range(d$Date)[1]), as.Date(range(d$Date)[2]), by = "day")),
      subset(d, status == "missing"),
      by = c("Date" = "Date")
    ) |> dplyr::arrange(Date), 
    aes(y = D, col = "missing")
  ) +
  scale_color_manual(
    "Legend",
    values = c("missing" = "grey", "Available" = "firebrick")
  ) +
  labs(title = "TMin Asante Krom (Dodi)_07075ASA", x = "Year", y = "Day of the year") +
  scale_x_continuous(
    breaks = seq(
      min(as.numeric(format(d$Date, "%Y"))), 
      max(as.numeric(format(d$Date, "%Y"))), 
      by = 10
    )
  ) +
  scale_y_continuous(breaks = seq(0, 366, 50)) +
  theme_classic() +
  theme(legend.text = element_text(size = 11),
        axis.text = element_text(size = 11),
        axis.title = element_text(size = 14, face = "italic")) +
  guides(colour = guide_legend(override.aes = list(size=3)))


rr + tx + tn + plot_layout(nrow = 2, byrow = TRUE)

intersect(intersect(Profile_StationsTX_Prior$StationName, Profile_StationsRR_Prior$StationName), Profile_StationsTN_Prior$StationName)
