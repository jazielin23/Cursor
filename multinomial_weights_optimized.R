## Optimized multinomial + weights feature engineering
## Drop-in replacement for the "multinomial model and weights section" in your script.
##
## What this file changes (performance only):
## - Replaces 5x repeated rowSums(df == k) scans per group with a single pass tabulation.
## - Vectorizes cantgeton computation (no per-experience loop, no giant broadcast matrix).
## - Keeps the multinom()/confint() and glm() logic / formulas the same.
##
## Usage (inside your existing flow, after SurveyData is quarter-filtered and NA->0):
##   source("multinomial_weights_optimized.R")
##   out <- build_weights22_fast(SurveyData = SurveyData, metadata = metadata, yearauto = yearauto)
##   weights22 <- out$weights22
##   CantRideWeight22 <- out$CantRideWeight22
##
## Notes:
## - Expects SurveyData column names already lowercased (like your script does).
## - Expects metadata column 2 to contain the experience variable names (same as your code).
## - Expects metadata columns: Type, Genre, Park, and the "couldn't ride" column at index 18.

suppressPackageStartupMessages({
  if (!requireNamespace("nnet", quietly = TRUE)) stop("Package 'nnet' is required.")
})

.as_int_ov <- function(x) {
  if (is.factor(x)) suppressWarnings(as.integer(as.character(x))) else suppressWarnings(as.integer(x))
}

# Fast per-row counts of values 1..5 across many columns.
.row_counts_1to5 <- function(df, cols) {
  cols <- intersect(tolower(cols), names(df))
  n <- nrow(df)
  if (!length(cols) || n == 0) {
    out <- matrix(0L, nrow = n, ncol = 5)
    colnames(out) <- c("one", "two", "three", "four", "five")
    return(out)
  }

  # data.matrix preserves numeric columns (avoids as.matrix() coercion to character).
  m <- data.matrix(df[, cols, drop = FALSE])
  v <- as.integer(m)
  ok <- !is.na(v) & v >= 1L & v <= 5L
  if (!any(ok)) {
    out <- matrix(0L, nrow = n, ncol = 5)
    colnames(out) <- c("one", "two", "three", "four", "five")
    return(out)
  }

  row_id <- rep.int(seq_len(n), times = ncol(m))
  idx <- row_id[ok] + n * (v[ok] - 1L) # bins: [lvl1 rows][lvl2 rows]...[lvl5 rows]
  counts <- tabulate(idx, nbins = n * 5L)
  out <- matrix(as.integer(counts), nrow = n, ncol = 5L)
  colnames(out) <- c("one", "two", "three", "four", "five")
  out
}

build_weights22_fast <- function(SurveyData, metadata, yearauto = 2024L) {
  SurveyData <- as.data.frame(SurveyData)
  names(SurveyData) <- tolower(names(SurveyData))
  SurveyData[is.na(SurveyData)] <- 0

  metadata <- as.data.frame(metadata)
  # Your script lowercases these before later steps; do it here for consistency.
  metadata[, 2] <- tolower(metadata[, 2])
  if ("Type" %in% names(metadata)) metadata$Type <- as.character(metadata$Type)
  if ("Genre" %in% names(metadata)) metadata$Genre <- as.character(metadata$Genre)

  n <- nrow(SurveyData)
  park_col <- if ("park" %in% names(SurveyData)) as.integer(SurveyData$park) else rep(NA_integer_, n)
  ov_col <- if ("ovpropex" %in% names(SurveyData)) SurveyData$ovpropex else rep(5L, n)
  FY_col <- if ("FY" %in% names(SurveyData)) as.integer(SurveyData$FY) else rep(as.integer(yearauto), n)

  # ---- Play ----
  cols_play <- metadata[metadata$Type == "Play", 2]
  cnt_play <- .row_counts_1to5(SurveyData, cols_play)
  weights_Play <- data.frame(
    one_Play = cnt_play[, "one"],
    two_Play = cnt_play[, "two"],
    three_Play = cnt_play[, "three"],
    four_Play = cnt_play[, "four"],
    five_Play = cnt_play[, "five"],
    Park = park_col,
    FY = FY_col
  )

  # ---- Show ----
  cols_show <- metadata[metadata$Type == "Show", 2]
  cnt_show <- .row_counts_1to5(SurveyData, cols_show)
  weights_Show <- data.frame(
    one_Show = cnt_show[, "one"],
    two_Show = cnt_show[, "two"],
    three_Show = cnt_show[, "three"],
    four_Show = cnt_show[, "four"],
    five_Show = cnt_show[, "five"],
    Park = park_col,
    FY = FY_col
  )

  # ---- Preferred (Flaship/Anchor) ----
  cols_pref <- metadata[metadata$Genre == "Flaship" | metadata$Genre == "Anchor", 2]
  cnt_pref <- .row_counts_1to5(SurveyData, cols_pref)
  weights_Preferred <- data.frame(
    one_Preferred = cnt_pref[, "one"],
    two_Preferred = cnt_pref[, "two"],
    three_Preferred = cnt_pref[, "three"],
    four_Preferred = cnt_pref[, "four"],
    five_Preferred = cnt_pref[, "five"],
    Park = park_col,
    FY = FY_col
  )

  # ---- Rides / Att ----
  cols_ride <- metadata[metadata$Type == "Ride", 2]
  cnt_ride <- .row_counts_1to5(SurveyData, cols_ride)
  one_RA <- cnt_ride[, "one"]
  two_RA <- cnt_ride[, "two"]
  three_RA <- cnt_ride[, "three"]
  four_RA <- cnt_ride[, "four"]
  five_RA <- cnt_ride[, "five"]

  # ---- can't-get-on (vectorized) ----
  idx_cr <- which(!is.na(metadata[, 18]) & metadata[, 18] != "")
  CouldntRide <- tolower(metadata[idx_cr, 18])
  Experience <- tolower(metadata[idx_cr, 2])
  CouldntRide <- intersect(CouldntRide, names(SurveyData))
  Experience <- intersect(Experience, names(SurveyData))

  cantgeton <- rep(0L, n)
  if (length(Experience) && length(CouldntRide) && length(Experience) == length(CouldntRide)) {
    Xexp <- data.matrix(SurveyData[, Experience, drop = FALSE])
    Xcant <- data.matrix(SurveyData[, CouldntRide, drop = FALSE])
    cantgeton <- rowSums((Xexp == 0) & (Xcant == 1), na.rm = TRUE)
    ov_int <- .as_int_ov(ov_col)
    cantgeton[!is.na(ov_int) & ov_int >= 6L] <- 0L
  }

  cant <- data.frame(
    ovpropex = ov_col,
    cantgeton = cantgeton,
    Park = park_col,
    FY = FY_col
  )

  weights_RA <- data.frame(
    ovpropex = ov_col,
    one_RA = one_RA, two_RA = two_RA, three_RA = three_RA, four_RA = four_RA, five_RA = five_RA,
    Park = park_col,
    FY = FY_col
  )

  weights <- cbind(weights_Play, weights_Show, weights_RA, cant, weights_Preferred)
  weights$ovpropex <- relevel(factor(weights$ovpropex), ref = "5")

  # ---- Fit per-park models (same formulas as your script) ----
  build_park_weights <- function(park_id, conf_level, conf_label) {
    w <- weights[weights$Park == park_id & weights$FY == yearauto, ]

    test <- nnet::multinom(
      ovpropex ~ cantgeton +
        one_Play + two_Play + three_Play + four_Play + five_Play +
        one_Show + two_Show + three_Show + four_Show + five_Show +
        one_RA + two_RA + three_RA + four_RA + five_RA +
        one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred,
      data = w
    )

    odds <- exp(confint(test, level = conf_level))
    z1 <- apply(odds, 3L, c)
    z2 <- expand.grid(dimnames(odds)[1:2])
    df_all <- data.frame(z2, z1)

    jz <- glm(
      (ovpropex == 5) ~
        one_Play + two_Play + three_Play + four_Play + five_Play +
        one_Show + two_Show + three_Show + four_Show + five_Show +
        one_RA + two_RA + three_RA + four_RA + five_RA +
        one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred,
      data = w, family = "binomial"
    )
    EXPcol <- exp(jz$coefficients[-1])

    core <- df_all[
      df_all$Var2 == conf_label &
        df_all$Var1 != "(Intercept)" &
        df_all$Var1 != "Attend" &
        df_all$Var1 != "cantgeton",
      ,
      drop = FALSE
    ]

    weightedEEs_part <- data.frame(core[1:15, , drop = FALSE], X5 = EXPcol[1:15])
    weightedEEs_part[weightedEEs_part$Var1 == "five_Play", 3:7] <- weightedEEs_part[weightedEEs_part$Var1 == "five_Play", 3:7] * 1
    weightedEEs_part[weightedEEs_part$Var1 == "five_Show", 3:7] <- weightedEEs_part[weightedEEs_part$Var1 == "five_Show", 3:7] * 1

    cantride_part <- data.frame(
      df_all[df_all$Var2 == conf_label & df_all$Var1 == "cantgeton", , drop = FALSE],
      X5 = rep(1, 1)
    )

    pref_part <- data.frame(core[16:20, , drop = FALSE], X5 = EXPcol[16:20])

    list(weightedEEs = weightedEEs_part, cantride = cantride_part, pref = pref_part)
  }

  mk <- build_park_weights(1, 0.995, "99.8 %")
  ep <- build_park_weights(2, 0.99, "99.5 %")
  st <- build_park_weights(3, 0.80, "90 %")
  ak <- build_park_weights(4, 0.70, "15 %")

  weightedEEs <- rbind(mk$weightedEEs, ep$weightedEEs, st$weightedEEs, ak$weightedEEs)
  weightsPref1 <- mk$pref
  weightsPref2 <- ep$pref
  weightsPref3 <- st$pref
  weightsPref4 <- ak$pref

  cantride2 <- data.frame(FY = yearauto, rbind(mk$cantride, ep$cantride, st$cantride, ak$cantride))
  CantRideWeight22 <- cantride2[, -1, drop = FALSE]
  weights22 <- rbind(weightedEEs, weightsPref1, weightsPref2, weightsPref3, weightsPref4)

  list(
    weights22 = weights22,
    CantRideWeight22 = CantRideWeight22,
    # optional diagnostics
    weights = weights,
    weights_Play = weights_Play,
    weights_Show = weights_Show,
    weights_RA = weights_RA,
    weights_Preferred = weights_Preferred,
    cant = cant
  )
}

