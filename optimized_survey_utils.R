# optimized_survey_utils.R
# Utility helpers to speed up common blocks in the survey pipeline.
#
# Usage:
#   source("optimized_survey_utils.R")
#   SurveyData <- filter_quarter(SurveyData, FQ)
#   meta_preparedPOG <- build_meta_preparedPOG(SurveyData, meta_prepared, AttQTR, FQ)
#   cantgeton <- compute_cantgeton(SurveyData, metadata)

filter_quarter <- function(SurveyData, FQ, quarter_col = "fiscal_quarter") {
  idx <- SurveyData[[quarter_col]] == FQ
  SurveyData[idx, , drop = FALSE]
}

# Vectorized replacement for the while(k...) POG loop.
# Notes:
# - Mirrors your original denominator: nrow(SurveyData[SurveyData$park == pahk,])
# - Mirrors your original numerator: sum(SurveyData[, var] > 0) (not park-filtered)
build_meta_preparedPOG <- function(SurveyData, meta_prepared, AttQTR, FQ, park_col = "park") {
  # Quarter filter once (safe if already filtered)
  SurveyData <- filter_quarter(SurveyData, FQ)

  vars <- as.character(meta_prepared$Variable)
  prefix <- sub("(charexp_|entexp_|ridesexp_).*$", "", vars)

  name <- sub(".*_", "", prefix)
  park_code <- sub("_.*", "", prefix)

  park_map <- c(mk = 1L, ec = 2L, dhs = 3L, dak = 4L)
  Park <- unname(park_map[park_code])

  keep <- !is.na(Park) & vars %in% names(SurveyData)
  if (!any(keep)) {
    return(data.frame(name = character(0), Park = integer(0), POG = numeric(0), expd = numeric(0)))
  }

  denom_by_park <- tabulate(match(SurveyData[[park_col]], 1:4), nbins = 4)
  num_pos <- colSums(SurveyData[, vars[keep], drop = FALSE] > 0, na.rm = TRUE)

  expd <- num_pos / denom_by_park[Park[keep]]

  meta_preparedPOG <- data.frame(
    name = name[keep],
    Park = Park[keep],
    POG  = meta_prepared$POG[keep],
    expd = expd
  )

  AttQTR <- AttQTR[AttQTR$FQ == FQ, , drop = FALSE]
  meta_preparedPOG <- merge(meta_preparedPOG, AttQTR, by = "Park")
  meta_preparedPOG$NEWPOG <- meta_preparedPOG$expd * meta_preparedPOG$Factor
  meta_preparedPOG$NEWGC  <- meta_preparedPOG$Att  * meta_preparedPOG$NEWPOG

  meta_preparedPOG
}

# Faster pattern: preselect columns once and compute 1..5 counts.
count_ratings_1to5 <- function(SurveyData, cols, values = 1:5) {
  cols <- intersect(tolower(cols), tolower(names(SurveyData)))
  if (!length(cols)) {
    out <- as.data.frame(setNames(replicate(length(values), 0, simplify = FALSE), paste0(c("one","two","three","four","five")[values])))
    return(out)
  }
  x <- SurveyData[, cols, drop = FALSE]
  out <- lapply(values, function(v) rowSums(x == v, na.rm = TRUE))
  names(out) <- c("one", "two", "three", "four", "five")[values]
  as.data.frame(out)
}

# Vectorized replacement for the cantgeton accumulator loop.
# Expects metadata[,18] = CouldntRide column name, metadata[,2] = Experience column name (as in your script).
compute_cantgeton <- function(SurveyData, metadata, couldnt_col_idx = 18, exp_col_idx = 2, ov_col = "ovpropex") {
  nms <- tolower(names(SurveyData))

  ix <- which(!is.na(metadata[, couldnt_col_idx]))
  if (!length(ix)) return(rep(0, nrow(SurveyData)))

  CouldntRide <- tolower(metadata[ix, couldnt_col_idx])
  Experience  <- tolower(metadata[ix, exp_col_idx])

  # Keep only columns that exist in SurveyData (and keep pairs aligned)
  ok_pair <- (CouldntRide %in% nms) & (Experience %in% nms)
  CouldntRide <- CouldntRide[ok_pair]
  Experience  <- Experience[ok_pair]
  if (!length(Experience)) return(rep(0, nrow(SurveyData)))

  # Pull matrices
  Xexp  <- as.matrix(SurveyData[, match(Experience, nms), drop = FALSE])
  Xcant <- as.matrix(SurveyData[, match(CouldntRide, nms), drop = FALSE])

  ok <- matrix(SurveyData[[ov_col]] < 6, nrow = nrow(SurveyData), ncol = ncol(Xexp))
  rowSums((Xexp == 0) & (Xcant == 1) & ok, na.rm = TRUE)
}
