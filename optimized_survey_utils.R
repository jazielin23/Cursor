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

# Build the full `weights` table inputs efficiently (Play/Show/Ride/Preferred + cantgeton).
# This replaces the repeated `names(SurveyData)[names(SurveyData) %in% ...]` subsetting and the cantgeton loop.
build_weights_inputs <- function(SurveyData, metadata, yearauto, FQ = NULL) {
  if (!is.null(FQ)) SurveyData <- filter_quarter(SurveyData, FQ)

  # Normalize metadata variable names once
  meta_var <- tolower(metadata[, 2])
  nms <- tolower(names(SurveyData))

  cols_play <- intersect(nms, meta_var[metadata$Type == "Play"])
  cols_show <- intersect(nms, meta_var[metadata$Type == "Show"])
  cols_ride <- intersect(nms, meta_var[metadata$Type == "Ride"])
  cols_pref <- intersect(nms, meta_var[metadata$Genre %in% c("Flaship", "Anchor")])

  play <- count_ratings_1to5(SurveyData, cols_play)
  show <- count_ratings_1to5(SurveyData, cols_show)
  ride <- count_ratings_1to5(SurveyData, cols_ride)
  pref <- count_ratings_1to5(SurveyData, cols_pref)

  # Preserve your original naming convention
  names(play) <- paste0(names(play), "_Play")
  names(show) <- paste0(names(show), "_Show")
  names(ride) <- paste0(names(ride), "_RA")
  names(pref) <- paste0(names(pref), "_Preferred")

  cantgeton <- compute_cantgeton(SurveyData, metadata)

  weights <- data.frame(
    q1 = SurveyData$q1,
    play,
    show,
    ovpropex = SurveyData$ovpropex,
    cantgeton = cantgeton,
    ride,
    pref,
    Park = SurveyData$park,
    FY = SurveyData$FY,
    check.names = FALSE
  )

  # Match your downstream expectations
  weights$ovpropex <- stats::relevel(factor(weights$ovpropex), ref = "5")
  weights
}

# Internal helper: reproduce your odds->data.frame(z2,z1) pattern exactly, but once.
.multinom_odds_df <- function(test, level) {
  odds <- exp(stats::confint(test, level = level))
  z1 <- apply(odds, 3L, c)
  z2 <- expand.grid(dimnames(odds)[1:2])
  data.frame(z2, z1, check.names = FALSE)
}

# Fit the multinom + glm for one park and return the three objects your script builds:
# - weightedEEs_<park> (15 rows)
# - weightsPref<park>  (5 rows)
# - cantride_<park>    (1 row)
fit_park_weights <- function(weights, park_id, yearauto, conf_level, conf_label) {
  w <- weights[weights$Park == park_id & weights$FY == yearauto, , drop = FALSE]
  # If there isn't enough data for the model, fail loudly (same as your current code would).
  if (!nrow(w)) stop("No rows for Park=", park_id, " FY=", yearauto)

  f_multi <- ovpropex ~ cantgeton +
    one_Play + two_Play + three_Play + four_Play + five_Play +
    one_Show + two_Show + three_Show + four_Show + five_Show +
    one_RA + two_RA + three_RA + four_RA + five_RA +
    one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred

  f_glm <- (ovpropex == 5) ~
    one_Play + two_Play + three_Play + four_Play + five_Play +
    one_Show + two_Show + three_Show + four_Show + five_Show +
    one_RA + two_RA + three_RA + four_RA + five_RA +
    one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred

  test <- nnet::multinom(f_multi, data = w, trace = FALSE)
  df_all <- .multinom_odds_df(test, level = conf_level)

  jz <- stats::glm(f_glm, data = w, family = "binomial")
  EXPcol <- exp(stats::coef(jz)[-1])

  # Exactly mirror your original row slicing.
  core <- df_all[which(df_all$Var2 == conf_label &
                         df_all$Var1 != "(Intercept)" &
                         df_all$Var1 != "Attend" &
                         df_all$Var1 != "cantgeton"), , drop = FALSE]

  weightedEEs <- data.frame(core[1:15, , drop = FALSE], X5 = EXPcol[1:15], check.names = FALSE)
  weightedEEs[weightedEEs$Var1 == "five_Play", 3:7] <- weightedEEs[weightedEEs$Var1 == "five_Play", 3:7] * 1
  weightedEEs[weightedEEs$Var1 == "five_Show", 3:7] <- weightedEEs[weightedEEs$Var1 == "five_Show", 3:7] * 1

  cantride <- data.frame(
    df_all[which(df_all$Var2 == conf_label & df_all$Var1 == "cantgeton"), , drop = FALSE],
    X5 = rep(1, 1),
    check.names = FALSE
  )

  weightsPref <- data.frame(core[16:20, , drop = FALSE], X5 = EXPcol[16:20], check.names = FALSE)

  list(weightedEEs = weightedEEs, cantride = cantride, weightsPref = weightsPref)
}

# Full drop-in replacement for your 4-park model block.
# Returns:
# - weights22
# - CantRideWeight22
build_weights22_all_parks <- function(weights, yearauto) {
  cfg <- data.frame(
    Park = c(1L, 2L, 3L, 4L),
    conf_level = c(0.995, 0.99, 0.80, 0.70),
    conf_label = c("99.8 %", "99.5 %", "90 %", "15 %"),
    stringsAsFactors = FALSE
  )

  res <- lapply(seq_len(nrow(cfg)), function(i) {
    fit_park_weights(
      weights = weights,
      park_id = cfg$Park[i],
      yearauto = yearauto,
      conf_level = cfg$conf_level[i],
      conf_label = cfg$conf_label[i]
    )
  })

  weightedEEs <- do.call(rbind, lapply(res, `[[`, "weightedEEs"))
  weightsPref_list <- lapply(res, `[[`, "weightsPref")
  cantride <- do.call(rbind, lapply(res, `[[`, "cantride"))

  weights22 <- do.call(rbind, c(list(weightedEEs), weightsPref_list))
  cantride2 <- data.frame(FY = yearauto, cantride, check.names = FALSE)
  CantRideWeight22 <- cantride2[, -1, drop = FALSE]

  list(weights22 = weights22, CantRideWeight22 = CantRideWeight22)
}

# One-call convenience: from SurveyData + metadata -> (weights, weights22, CantRideWeight22).
# This prevents "weights22 not found" by ensuring everything is created in-order.
build_all_weights <- function(SurveyData, metadata, yearauto, FQ = NULL) {
  if (!is.null(FQ)) SurveyData <- filter_quarter(SurveyData, FQ)
  weights <- build_weights_inputs(SurveyData, metadata, yearauto)
  out <- build_weights22_all_parks(weights, yearauto)
  out$weights <- weights
  out
}

# Optional auto-run on source().
# If you set: options(optimized_survey_utils.autorun = TRUE)
# and the following objects exist in the global environment:
#   SurveyData, metadata, yearauto, FQ
# then sourcing this file will create/overwrite:
#   weights, weights22, CantRideWeight22
if (isTRUE(getOption("optimized_survey_utils.autorun", FALSE))) {
  needed <- c("SurveyData", "metadata", "yearauto", "FQ")
  ok <- all(vapply(needed, exists, logical(1), envir = .GlobalEnv))
  if (ok) {
    .out <- build_all_weights(
      SurveyData = get("SurveyData", envir = .GlobalEnv),
      metadata = get("metadata", envir = .GlobalEnv),
      yearauto = get("yearauto", envir = .GlobalEnv),
      FQ = get("FQ", envir = .GlobalEnv)
    )
    assign("weights", .out$weights, envir = .GlobalEnv)
    assign("weights22", .out$weights22, envir = .GlobalEnv)
    assign("CantRideWeight22", .out$CantRideWeight22, envir = .GlobalEnv)
    rm(.out, envir = environment())
  }
}
