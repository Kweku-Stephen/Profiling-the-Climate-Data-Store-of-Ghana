# Saving to workbooks
classes = parallel::clusterApplyLB(
	cl_RR,
	regions,
	\(vec, list) {
		
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
		
		return(reg)
		
		# rio::export(
		# 	reg, 
		# 	file = file.path(path, "RR", paste(vec, "xlsx", sep = ".")), 
		# 	sheetName = names(reg), 
		# 	rowNames = FALSE
		# )
		
	},
	list = profMssReg
)

# Naming list(classes) elements
names(classes) <- regions

# Explicitly writing out
parallel::clusterApply(
	cl_RR,
	regions,
	fun = \(vec, list) {
		
		rio::export(
			list[[vec]],
			file = file.path(getwd(), path, paste(vec, "xlsx", sep = ".")),
			rowNames = FALSE,
			sheetName = names(list[vec])
		)
		
	},
	list = classes
)
