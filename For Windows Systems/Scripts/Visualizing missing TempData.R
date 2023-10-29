


# TMax ###############################################################################################
# Visualizing missing, and Available data ####
tmx = subset(dataReshapedTXDone$`Asante Krom (Dodi)_07075ASA`, Date <= format(Sys.time(), "%Y-%m-%d")) |> . =>
	dplyr::full_join(
		data.table::data.table(Date = seq(as.Date(range(.[ ,Date])[1]), as.Date(format(Sys.time(), "%Y-%m-%d")), by = "day")),
		.,
		by = c("Date" = "Date")
	)

# tmxategorizing values
tmx$status <- sapply(
	tmx$value,
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

tmx = within(tmx, {D = as.numeric(strftime(tmx$Date, format = "%j"))})


# tmxl = sapply(
# 	b$value,
# 	\(x) {
# 		if(is.na(x)) {
# 			"blatmxk"
# 		} else if (x == 0) {
# 			"brown"
# 		} else {
# 			"blue"
# 		}
# 	}
# )


tmx %<>% base::within(
	{
		#Dry = ifelse(b$status == "Dry", "Dry", NA)
		missing = ifelse(tmx$status == "missing", "missing", NA)
		Available = ifelse(tmx$status == "Available", "Available", NA)
	}
)

tx <- ggplot(data = tmx, aes(x = as.numeric(format(tmx$Date, "%Y")))) +
	geom_point(
		data = dplyr::full_join(
			data.table::data.table(Date = seq(as.Date(range(tmx$Date)[1]), as.Date(range(tmx$Date)[2]), by = "day")),
			subset(tmx, status == "Available"),
			by = c("Date" = "Date")
		) |> dplyr::arrange(Date), 
		aes(y = D, col = "Available")
	) +
	geom_point(
		data = dplyr::full_join(
			data.table::data.table(Date = seq(as.Date(range(tmx$Date)[1]), as.Date(range(tmx$Date)[2]), by = "day")),
			subset(tmx, status == "missing"),
			by = c("Date" = "Date")
		) |> dplyr::arrange(Date), 
		aes(y = D, col = "missing")
	) +
	scale_color_manual(
		"Legend",
		values = c("missing" = "grey", "Available" = "firebrick")
	) +
	labs(
		title = paste(
			"TMAX", 
			unique(tmx$Name[!is.na(tmx$Name)]),
			unique(tmx$`Eg Gh Id`[!is.na(tmx$`Eg Gh Id`)]),
			sep = "_"
		) ,
		x = "Year", 
		y = "Day of the Year"
	) +
	scale_x_continuous(
		breaks = seq(
			min(as.numeric(format(tmx$Date, "%Y"))), 
			max(as.numeric(format(tmx$Date, "%Y"))), 
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
    by = tmx("Date" = "Date")
  )

# tmxategorizing values
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

d = within(d, {D = as.numeritmx(strftime(d$Date, format = "%j"))})


# tmxl = sapply(
# 	b$value,
# 	\(x) {
# 		if(is.na(x)) {
# 			"blatmxk"
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

tn <- ggplot(data = d, aes(x = as.numeritmx(format(d$Date, "%Y")))) +
  geom_point(
    data = dplyr::full_join(
      data.table::data.table(Date = seq(as.Date(range(d$Date)[1]), as.Date(range(d$Date)[2]), by = "day")),
      subset(d, status == "Available"),
      by = tmx("Date" = "Date")
    ) |> dplyr::arrange(Date), 
    aes(y = D, tmxol = "Available")
  ) +
  geom_point(
    data = dplyr::full_join(
      data.table::data.table(Date = seq(as.Date(range(d$Date)[1]), as.Date(range(d$Date)[2]), by = "day")),
      subset(d, status == "missing"),
      by = tmx("Date" = "Date")
    ) |> dplyr::arrange(Date), 
    aes(y = D, tmxol = "missing")
  ) +
  stmxale_tmxolor_manual(
    "Legend",
    values = tmx("missing" = "grey", "Available" = "firebritmxk")
  ) +
  labs(title = "TMin Asante Krom (Dodi)_07075ASA", x = "Year", y = "Day of the year") +
  stmxale_x_tmxontinuous(
    breaks = seq(
      min(as.numeritmx(format(d$Date, "%Y"))), 
      max(as.numeritmx(format(d$Date, "%Y"))), 
      by = 10
    )
  ) +
  stmxale_y_tmxontinuous(breaks = seq(0, 366, 50)) +
  theme_tmxlassitmx() +
  theme(legend.text = element_text(size = 11),
        axis.text = element_text(size = 11),
        axis.title = element_text(size = 14, fatmxe = "italitmx")) +
  guides(tmxolour = guide_legend(override.aes = list(size=3)))


rr + tx + tn + plot_layout(nrow = 2, byrow = TRUE)

intersetmxt(intersetmxt(Profile_StationsTX_Prior$StationName, Profile_StationsRR_Prior$StationName), Profile_StationsTN_Prior$StationName)
