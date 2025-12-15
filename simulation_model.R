# simulation_model.R
#
# Modeling-only version of the EARS simulation (no Shiny).
# Drop this into a Dataiku R recipe (or source it) and call `run_simulation_dataiku()`.

suppressPackageStartupMessages({
  library(dataiku)
  library(foreach)
  library(doParallel)
  library(data.table)
  library(dplyr)
  library(nnet)
  library(sqldf)
  library(reshape2)
})

# ---------------------------
# Helpers (fast + NA-safe)
# ---------------------------

.as_int_ov <- function(x) {
  if (is.factor(x)) suppressWarnings(as.integer(as.character(x))) else suppressWarnings(as.integer(x))
}

.base_name <- function(col) {
  sub("^[^_]+_[^_]+_", "", tolower(col))
}

.normalize_ears_join_keys <- function(df) {
  df <- as.data.frame(df)
  if ("NAME" %in% names(df)) df$NAME <- tolower(as.character(df$NAME))
  if ("Genre" %in% names(df)) df$Genre <- as.character(df$Genre)
  if ("Park" %in% names(df)) df$Park <- suppressWarnings(as.integer(df$Park))
  if ("QTR" %in% names(df)) df$QTR <- suppressWarnings(as.integer(df$QTR))
  if ("LifeStage" %in% names(df)) df$LifeStage <- suppressWarnings(as.integer(df$LifeStage))
  df
}

# Critical: prevents “valid experiences” becoming NA due to NAME mismatches in joins
rename_ride_measures_to_taxonomy <- function(dt, metadata, id_cols = c("Park", "LifeStage", "QTR")) {
  dt <- as.data.table(dt)
  m <- as.data.table(metadata)

  if (!all(c("Variable", "name", "Type") %in% names(m))) return(dt)
  m <- m[Type == "Ride" & !is.na(Variable) & nzchar(Variable) & !is.na(name) & nzchar(name)]
  if (!nrow(m)) return(dt)

  m[, from := .base_name(Variable)]
  m[, to := tolower(as.character(name))]
  map <- unique(m[, .(from, to)])
  map <- map[!is.na(from) & nzchar(from) & !is.na(to) & nzchar(to)]
  map <- map[, .SD[1], by = from]

  meas <- setdiff(names(dt), id_cols)
  from_present <- intersect(map$from, meas)
  if (!length(from_present)) return(dt)

  for (f in from_present) {
    t <- map$to[match(f, map$from)]
    if (is.na(t) || !nzchar(t)) next
    if (t %in% names(dt)) next
    setnames(dt, f, t)
  }
  dt
}

.score_vectors <- function(x, ov_col_idx, w_mat, cant_vec) {
  out2 <- numeric(length(x))
  out3 <- numeric(length(x))

  idx2 <- which(x == 5L & ov_col_idx > 0L)
  if (length(idx2)) out2[idx2] <- w_mat[5L, ov_col_idx[idx2]]

  idx3 <- which(x >= 1L & x <= 4L & ov_col_idx > 0L)
  if (length(idx3)) out3[idx3] <- w_mat[cbind(x[idx3], ov_col_idx[idx3])]

  # can't ride sentinel -1
  idx_cr <- which(x == -1L & ov_col_idx > 0L)
  if (length(idx_cr)) {
    # matches your original:
    # ov=5 => keep x (-1) so becomes -cant
    # ov!=5 => flip sign so becomes +cant
    sign_term <- ifelse(ov_col_idx[idx_cr] == 5L, x[idx_cr], -x[idx_cr])
    out3[idx_cr] <- cant_vec[ov_col_idx[idx_cr]] * sign_term
  }

  list(score2 = out2, score3 = out3)
}

apply_weights_fast <- function(dt, metadata, weights22, CantRideWeight22, FQ) {
  dt <- as.data.table(dt)
  dt <- dt[fiscal_quarter == FQ]

  ov <- .as_int_ov(dt$ovpropex)
  ov_col_idx <- match(ov, 1:5)
  ov_col_idx[is.na(ov_col_idx)] <- 0L

  W <- as.matrix(weights22[, 3:7, drop = FALSE])

  get_block15 <- function(park) {
    start <- (park - 1L) * 15L
    list(
      play = matrix(W[start + 1:5, ], nrow = 5),
      show = matrix(W[start + 6:10, ], nrow = 5),
      ride = matrix(W[start + 11:15, ], nrow = 5)
    )
  }

  get_pref <- function(park) {
    start <- 60L + (park - 1L) * 5L
    matrix(W[start + 1:5, ], nrow = 5)
  }

  CR <- as.matrix(CantRideWeight22[, 3:7, drop = FALSE])

  m <- as.data.table(metadata)
  m[, Variable := tolower(Variable)]
  m[, Type := as.character(Type)]
  m[, Genre := as.character(Genre)]
  m[, Park := as.integer(Park)]

  vars_present <- intersect(m$Variable, names(dt))
  if (!length(vars_present)) return(dt)

  m2 <- unique(m[Variable %in% vars_present, .(Variable, Type, Genre, Park)])
  m2[, base := .base_name(Variable)]
  m2[, col2 := paste0(base, "2")]
  m2[, col3 := paste0(base, "3")]

  for (cn in unique(c(m2$col2, m2$col3))) if (!cn %in% names(dt)) set(dt, j = cn, value = 0)

  for (park in 1:4) {
    rows_p <- which(dt$park == park)
    if (!length(rows_p)) next

    blocks <- get_block15(park)
    pref_mat <- get_pref(park)
    cant_vec <- CR[park, ]

    mp <- m2[Park == park]
    if (!nrow(mp)) next

    for (r in seq_len(nrow(mp))) {
      var <- mp$Variable[r]
      x <- dt[[var]][rows_p]
      if (is.factor(x)) x <- suppressWarnings(as.integer(as.character(x)))
      x[is.na(x)] <- 0L
      x <- as.integer(x)

      if (!is.na(mp$Genre[r]) && (mp$Genre[r] == "Flaship" || mp$Genre[r] == "Anchor")) {
        w_mat <- pref_mat
      } else if (!is.na(mp$Type[r]) && mp$Type[r] == "Ride") {
        w_mat <- blocks$ride
      } else if (!is.na(mp$Type[r]) && mp$Type[r] == "Show" && !(mp$Genre[r] %in% "Anchor")) {
        w_mat <- blocks$show
      } else if (!is.na(mp$Type[r]) && mp$Type[r] == "Play") {
        w_mat <- blocks$play
      } else if (!is.na(mp$Type[r]) && mp$Type[r] == "Show") {
        w_mat <- blocks$show
      } else {
        next
      }

      sc <- .score_vectors(x, ov_col_idx[rows_p], w_mat, cant_vec)
      set(dt, i = rows_p, j = mp$col2[r], value = sc$score2)
      set(dt, i = rows_p, j = mp$col3[r], value = sc$score3)
    }
  }

  dt
}

apply_counts_fast <- function(dt, metadata, FQ) {
  dt <- as.data.table(dt)
  dt <- dt[fiscal_quarter == FQ]

  ov <- .as_int_ov(dt$ovpropex)
  ov_ok <- !is.na(match(ov, 1:5))

  m <- as.data.table(metadata)
  m[, Variable := tolower(Variable)]
  m[, Park := as.integer(Park)]

  vars_present <- intersect(m$Variable, names(dt))
  if (!length(vars_present)) return(dt)

  m2 <- unique(m[Variable %in% vars_present, .(Variable, Park)])
  m2[, base := .base_name(Variable)]
  m2[, col2 := paste0(base, "2")]
  m2[, col3 := paste0(base, "3")]

  for (cn in unique(c(m2$col2, m2$col3))) if (!cn %in% names(dt)) set(dt, j = cn, value = 0)

  for (park in 1:4) {
    rows_p <- which(dt$park == park)
    if (!length(rows_p)) next

    mp <- m2[Park == park]
    if (!nrow(mp)) next

    for (r in seq_len(nrow(mp))) {
      var <- mp$Variable[r]
      x <- dt[[var]][rows_p]
      if (is.factor(x)) x <- suppressWarnings(as.integer(as.character(x)))
      x[is.na(x)] <- 0L
      x <- as.integer(x)

      out2 <- as.numeric(x == 5L & ov_ok[rows_p])
      out3 <- as.numeric(x >= 1L & x <= 4L & ov_ok[rows_p])

      idx_cr <- which(x == -1L & ov_ok[rows_p])
      if (length(idx_cr)) {
        ov_sub <- ov[rows_p][idx_cr]
        out3[idx_cr] <- ifelse(ov_sub == 5L, 1L * x[idx_cr], -1L * x[idx_cr])
      }

      set(dt, i = rows_p, j = mp$col2[r], value = out2)
      set(dt, i = rows_p, j = mp$col3[r], value = out3)
    }
  }

  dt
}

summarize_wide_by_group <- function(dt, bases, suffix, group_cols = c("park", "newgroup", "fiscal_quarter")) {
  dt <- as.data.table(dt)
  cols <- intersect(paste0(bases, suffix), names(dt))
  if (!length(cols)) return(unique(dt[, ..group_cols]))
  dt[, lapply(.SD, sum, na.rm = TRUE), by = group_cols, .SDcols = cols]
}

summarize_category_wide_by_group <- function(dt, metadata, type = c("Show", "Play"), suffix,
                                            group_cols = c("park", "newgroup", "fiscal_quarter"),
                                            exclude_anchor_genre_for_show = TRUE) {
  type <- match.arg(type)
  dt <- as.data.table(dt)
  m <- as.data.table(metadata)

  m[, Variable := tolower(Variable)]
  m[, Type := as.character(Type)]
  if ("Genre" %in% names(m)) m[, Genre := as.character(Genre)]
  m[, Park := suppressWarnings(as.integer(Park))]

  m <- m[Type == type & !is.na(Category1) & Category1 != ""]
  if (!nrow(m)) return(unique(dt[, ..group_cols]))

  if (type == "Show" && exclude_anchor_genre_for_show && "Genre" %in% names(m)) {
    m <- m[is.na(Genre) | Genre != "Anchor"]
  }

  m[, base := .base_name(Variable)]
  map <- m[!is.na(Park) & Park %in% 1:4, .(park = Park, base, Category1)]
  map <- map[!is.na(base) & nzchar(base) & !is.na(Category1) & nzchar(Category1)]
  map <- map[, .SD[1], by = .(park, base)]

  cols <- intersect(paste0(unique(map$base), suffix), names(dt))
  if (!length(cols)) return(unique(dt[, ..group_cols]))

  # Be explicit: in Dataiku + foreach workers, `melt()` can resolve to reshape2::melt()
  # which returns a data.frame. We need a data.table for `:=`.
  long <- data.table::melt(
    dt[, c(group_cols, cols), with = FALSE],
    id.vars = group_cols,
    variable.name = "var",
    value.name = "val",
    variable.factor = FALSE
  )
  long <- as.data.table(long)
  long[, base := sub(paste0(suffix, "$"), "", var)]
  long <- merge(long, map, by = c("park", "base"), all = FALSE)

  agg <- long[, .(val = sum(val, na.rm = TRUE)), by = c(group_cols, "Category1")]
  data.table::dcast(
    agg,
    as.formula(paste(paste(group_cols, collapse = " + "), "~ Category1")),
    value.var = "val",
    fill = 0
  )
}

as_Park_LifeStage_QTR <- function(dt, park_col = "park", life_col = "newgroup", qtr_col = "fiscal_quarter") {
  dt <- as.data.table(dt)
  setnames(dt, c(park_col, life_col, qtr_col), c("Park", "LifeStage", "QTR"), skip_absent = TRUE)
  dt[]
}

strip_measure_suffix <- function(dt, suffix, id_cols = c("Park", "LifeStage", "QTR")) {
  dt <- as.data.table(dt)
  meas <- setdiff(names(dt), id_cols)
  if (!length(meas)) return(dt)
  setnames(dt, meas, sub(paste0(suffix, "$"), "", meas))
  dt
}

# ---------------------------
# Main modeling runner
# ---------------------------

run_simulation_dataiku <- function(
  n_runs = 10,
  num_cores = 5,
  yearauto = 2024,
  park_for_sim = 1,
  exp_name = c("tron"),
  exp_date_ranges = list(tron = c("2023-10-11", "2025-09-02")),
  maxFQ = 4
) {
  meta_prepared_new <- dkuReadDataset("MetaDataFinalTaxonomy")
  AttQTR_new <- dkuReadDataset("Attendance")
  QTRLY_GC_new <- dkuReadDataset("GuestCarriedFinal")
  SurveyData_new <- dkuReadDataset("FY24_prepared")
  DT <- dkuReadDataset("DT")
  EARS <- dkuReadDataset("EARS_Taxonomy")
  EARS <- .normalize_ears_join_keys(EARS)

  cl <- makeCluster(as.integer(num_cores))
  on.exit({
    try(stopCluster(cl), silent = TRUE)
  }, add = TRUE)
  registerDoParallel(cl)

  # Ensure worker processes can see helper functions defined in this file.
  # Without this, %dopar% tasks can fail with "could not find function ...".
  helper_exports <- c(
    ".as_int_ov",
    ".base_name",
    ".normalize_ears_join_keys",
    "rename_ride_measures_to_taxonomy",
    ".score_vectors",
    "apply_weights_fast",
    "apply_counts_fast",
    "summarize_wide_by_group",
    "summarize_category_wide_by_group",
    "as_Park_LifeStage_QTR",
    "strip_measure_suffix"
  )

  # Best-effort explicit export (in addition to foreach's .export).
  # This makes behavior consistent across Dataiku / RStudio / CLI.
  try(parallel::clusterExport(cl, varlist = helper_exports, envir = .GlobalEnv), silent = TRUE)

  EARSTotal_list <- foreach(
    run = 1:as.integer(n_runs),
    .combine = rbind,
    .packages = c("dataiku", "nnet", "sqldf", "data.table", "dplyr", "reshape2"),
    .export = helper_exports
  ) %dopar% {
    stage <- "init"
    tryCatch({
      SurveyData <- SurveyData_new
      SurveyData <- as.data.frame(SurveyData)
      names(SurveyData) <- tolower(names(SurveyData))

      stage <- "newgroup_recode"
      SurveyData$newgroup <- SurveyData$newgroup1
      SurveyData$newgroup[SurveyData$newgroup == 4] <- 3
      SurveyData$newgroup[SurveyData$newgroup == 5] <- 4
      SurveyData$newgroup[SurveyData$newgroup == 6] <- 5
      SurveyData$newgroup[SurveyData$newgroup == 7] <- 5

      stage <- "mc_swap"
      meta_prepared <- meta_prepared_new
      park <- as.integer(park_for_sim)

      for (name in exp_name) {
        matched_row <- meta_prepared[meta_prepared$name == name & meta_prepared$Park == park, ]
        if (!nrow(matched_row)) next
        exp_ride_col <- as.character(matched_row$Variable[1])
        exp_group_col <- as.character(matched_row$SPEC[1])
        if (is.na(exp_ride_col) || !nzchar(exp_ride_col)) next

        ride_exists <- exp_ride_col %in% colnames(SurveyData)
        group_exists <- exp_group_col %in% colnames(SurveyData)

        if (ride_exists) {
          segments <- unique(SurveyData$newgroup[SurveyData$park == park])
          date_range <- exp_date_ranges[[name]]
          for (seg in segments) {
            seg_idx <- which(
              SurveyData$park == park &
                SurveyData$newgroup == seg &
                SurveyData$visdate_parsed >= as.Date(date_range[1]) &
                SurveyData$visdate_parsed <= as.Date(date_range[2])
            )
            if (!length(seg_idx)) next
            seg_data <- SurveyData[seg_idx, , drop = FALSE]

            if (group_exists) {
              for (wanted in c(1, 0)) {
                to_replace_idx <- which(!is.na(seg_data[[exp_ride_col]]) & seg_data[[exp_group_col]] == wanted)
                pool_idx <- which(is.na(seg_data[[exp_ride_col]]) & seg_data[[exp_group_col]] == wanted)
                if (length(to_replace_idx) > 0 && length(pool_idx) > 0) {
                  sampled_local_idx <- sample(pool_idx, size = length(to_replace_idx), replace = TRUE)
                  replace_rows <- seg_idx[to_replace_idx]
                  pool_rows <- seg_idx[sampled_local_idx]
                  if (length(replace_rows) == length(pool_rows) && length(replace_rows) > 0) {
                    SurveyData[replace_rows, ] <- SurveyData[pool_rows, , drop = FALSE]
                  }
                }
              }
            } else {
              to_replace_idx <- which(!is.na(seg_data[[exp_ride_col]]))
              pool_idx <- which(is.na(seg_data[[exp_ride_col]]))
              if (length(to_replace_idx) > 0 && length(pool_idx) > 0) {
                sampled_local_idx <- sample(pool_idx, size = length(to_replace_idx), replace = TRUE)
                replace_rows <- seg_idx[to_replace_idx]
                pool_rows <- seg_idx[sampled_local_idx]
                if (length(replace_rows) == length(pool_rows) && length(replace_rows) > 0) {
                  SurveyData[replace_rows, ] <- SurveyData[pool_rows, , drop = FALSE]
                }
              }
            }
          }
        }
      }

      SurveyDataSim <- SurveyData
      EARSTotal <- NULL

      stage <- "quarter_loop"
      FQ <- 1L
      while (FQ < as.integer(maxFQ) + 1L) {
      SurveyData <- SurveyDataSim
      SurveyData <- SurveyData[SurveyData$fiscal_quarter == FQ, ]
      if (!nrow(SurveyData)) {
        FQ <- FQ + 1L
        next
      }

      meta_prepared <- meta_prepared_new
      AttQTR <- AttQTR_new[AttQTR_new$FQ == FQ, ]
      QTRLY_GC <- QTRLY_GC_new[QTRLY_GC_new$FQ == FQ, ]

      # --- POG backfill ---
      k <- 1L
      name_vec <- character(0)
      park_vec <- integer(0)
      expd_vec <- numeric(0)
      pog_vec <- numeric(0)

      while (k < length(meta_prepared$Variable) + 1L) {
        var_k_raw <- as.character(meta_prepared$Variable[k])
        if (is.na(var_k_raw) || !nzchar(var_k_raw)) {
          k <- k + 1L
          next
        }

        tmp1 <- strsplit(var_k_raw, "charexp_", fixed = TRUE)[[1]][1]
        tmp1 <- strsplit(tmp1, "entexp_", fixed = TRUE)[[1]][1]
        tmp1 <- strsplit(tmp1, "ridesexp_", fixed = TRUE)[[1]][1]

        eddie <- sub(".*_", "", tmp1)
        pahk <- gsub("_.*", "", tmp1)

        park_map <- c(mk = 1L, ec = 2L, dhs = 3L, dak = 4L)
        if (pahk %in% names(park_map)) pahk <- park_map[[pahk]]

        var_col <- tolower(var_k_raw)
        if (var_col %in% names(SurveyData) && !is.na(pahk) && pahk %in% 1:4) {
          denom <- sum(SurveyData$park == pahk, na.rm = TRUE)
          expd1 <- if (denom > 0) sum(SurveyData[[var_col]] > 0, na.rm = TRUE) / denom else 0
          name_vec <- c(name_vec, eddie)
          park_vec <- c(park_vec, pahk)
          expd_vec <- c(expd_vec, expd1)
          pog_vec <- c(pog_vec, meta_prepared$POG[k])
        }
        k <- k + 1L
      }

      metaPOG <- NULL
      if (length(name_vec)) {
        meta_preparedPOG <- data.frame(name = name_vec, Park = park_vec, POG = pog_vec, expd = expd_vec)
        meta_preparedPOG <- merge(meta_preparedPOG, AttQTR, by = "Park")
        meta_preparedPOG$NEWPOG <- meta_preparedPOG$expd * meta_preparedPOG$Factor
        meta_preparedPOG$NEWGC <- meta_preparedPOG$Att * meta_preparedPOG$NEWPOG
        metaPOG <- merge(meta_prepared, meta_preparedPOG[, c("Park", "name", "NEWGC")], by = c("name", "Park"))
      }

      meta_prepared2 <- sqldf("select a.*,b.GC as QuarterlyGuestCarried from meta_prepared a left join QTRLY_GC b on a.name=b.name and a.Park = b.Park")
      meta_prepared2 <- as.data.frame(meta_prepared2)

      stage <- "pog_backfill"
      if (!is.null(metaPOG) && nrow(metaPOG)) {
        meta_prepared2 <- merge(meta_prepared2, metaPOG[, c("name", "Park", "NEWGC")], by = c("name", "Park"), all.x = TRUE)
        # Defensive: Dataiku schemas/merges can drop/rename columns; only backfill when both exist.
        if ("QuarterlyGuestCarried" %in% names(meta_prepared2) && "NEWGC" %in% names(meta_prepared2)) {
          idx_fill <- is.na(meta_prepared2$QuarterlyGuestCarried) & !is.na(meta_prepared2$NEWGC)
          if (length(idx_fill) == nrow(meta_prepared2) && any(idx_fill, na.rm = TRUE)) {
            meta_prepared2$QuarterlyGuestCarried[idx_fill] <- meta_prepared2$NEWGC[idx_fill]
          }
          meta_prepared2$NEWGC <- NULL
        }
      }

      stage <- "weights_and_ears"
      metadata <- data.frame(meta_prepared2)
      names(SurveyData) <- tolower(names(SurveyData))
      SurveyData[is.na(SurveyData)] <- 0
      SurveyData$FY <- as.integer(yearauto)

      # ---------------------------
      # Weight building
      # ---------------------------
      .safe_rowSums_eq <- function(df, cols, value) {
        cols <- intersect(tolower(cols), names(df))
        if (!length(cols)) return(rep(0, nrow(df)))
        rowSums(df[, cols, drop = FALSE] == value, na.rm = TRUE)
      }

      park_col <- SurveyData$park
      ov_col <- SurveyData$ovpropex

      cols_play <- metadata[metadata$Type == "Play", 2]
      five_Play <- .safe_rowSums_eq(SurveyData, cols_play, 5)
      four_Play <- .safe_rowSums_eq(SurveyData, cols_play, 4)
      three_Play <- .safe_rowSums_eq(SurveyData, cols_play, 3)
      two_Play <- .safe_rowSums_eq(SurveyData, cols_play, 2)
      one_Play <- .safe_rowSums_eq(SurveyData, cols_play, 1)
      weights_Play <- data.frame(one_Play, two_Play, three_Play, four_Play, five_Play, Park = as.integer(park_col), FY = as.integer(SurveyData$FY))

      cols_show <- metadata[metadata$Type == "Show", 2]
      five_Show <- .safe_rowSums_eq(SurveyData, cols_show, 5)
      four_Show <- .safe_rowSums_eq(SurveyData, cols_show, 4)
      three_Show <- .safe_rowSums_eq(SurveyData, cols_show, 3)
      two_Show <- .safe_rowSums_eq(SurveyData, cols_show, 2)
      one_Show <- .safe_rowSums_eq(SurveyData, cols_show, 1)
      weights_Show <- data.frame(one_Show, two_Show, three_Show, four_Show, five_Show, Park = as.integer(park_col), FY = as.integer(SurveyData$FY))

      cols_pref <- metadata[metadata$Genre == "Flaship" | metadata$Genre == "Anchor", 2]
      five_Preferred <- .safe_rowSums_eq(SurveyData, cols_pref, 5)
      four_Preferred <- .safe_rowSums_eq(SurveyData, cols_pref, 4)
      three_Preferred <- .safe_rowSums_eq(SurveyData, cols_pref, 3)
      two_Preferred <- .safe_rowSums_eq(SurveyData, cols_pref, 2)
      one_Preferred <- .safe_rowSums_eq(SurveyData, cols_pref, 1)
      weights_Preferred <- data.frame(one_Preferred, two_Preferred, three_Preferred, four_Preferred, five_Preferred, Park = as.integer(park_col), FY = as.integer(SurveyData$FY))

      cols_ride <- metadata[metadata$Type == "Ride", 2]
      five_RA <- .safe_rowSums_eq(SurveyData, cols_ride, 5)
      four_RA <- .safe_rowSums_eq(SurveyData, cols_ride, 4)
      three_RA <- .safe_rowSums_eq(SurveyData, cols_ride, 3)
      two_RA <- .safe_rowSums_eq(SurveyData, cols_ride, 2)
      one_RA <- .safe_rowSums_eq(SurveyData, cols_ride, 1)

      idx_cr <- which(!is.na(metadata[, 18]) & metadata[, 18] != "")
      CouldntRide <- tolower(metadata[idx_cr, 18])
      Experience <- tolower(metadata[idx_cr, 2])
      CouldntRide <- intersect(CouldntRide, names(SurveyData))
      Experience <- intersect(Experience, names(SurveyData))

      cantgeton <- rep(0, nrow(SurveyData))
      if (length(Experience) && length(CouldntRide) && length(Experience) == length(CouldntRide)) {
        Xexp <- as.matrix(SurveyData[, Experience, drop = FALSE])
        Xcant <- as.matrix(SurveyData[, CouldntRide, drop = FALSE])
        ov_ok <- suppressWarnings(as.integer(ov_col) < 6)
        cantgeton <- rowSums((Xexp == 0) & (Xcant == 1) & matrix(ov_ok, nrow = nrow(SurveyData), ncol = ncol(Xexp)), na.rm = TRUE)
      }

      cant <- data.frame(ovpropex = ov_col, cantgeton = cantgeton, Park = as.integer(park_col), FY = as.integer(SurveyData$FY))
      weights_RA <- data.frame(ovpropex = ov_col, one_RA, two_RA, three_RA, four_RA, five_RA, Park = as.integer(park_col), FY = as.integer(SurveyData$FY))
      weights <- cbind(weights_Play, weights_Show, weights_RA, cant, weights_Preferred)

      build_park_weights <- function(park_id, conf_level, conf_label) {
        .defaults <- function() {
          make_df <- function(var1) {
            data.frame(
              Var1 = var1,
              Var2 = rep(conf_label, length(var1)),
              X1 = rep(1, length(var1)),
              X2 = rep(1, length(var1)),
              X3 = rep(1, length(var1)),
              X4 = rep(1, length(var1)),
              X5 = rep(1, length(var1)),
              stringsAsFactors = FALSE
            )
          }
          list(
            weightedEEs = make_df(c(
              "one_Play","two_Play","three_Play","four_Play","five_Play",
              "one_Show","two_Show","three_Show","four_Show","five_Show",
              "one_RA","two_RA","three_RA","four_RA","five_RA"
            )),
            cantride = make_df("cantgeton"),
            pref = make_df(c("one_Preferred","two_Preferred","three_Preferred","four_Preferred","five_Preferred"))
          )
        }

        w <- weights
        w$ovpropex <- relevel(factor(w$ovpropex), ref = "5")
        w <- w[w$Park == park_id & w$FY == yearauto, ]
        n_classes <- length(unique(w$ovpropex[!is.na(w$ovpropex)]))
        if (nrow(w) < 10 || n_classes < 2) return(.defaults())

        test <- tryCatch(
          multinom(
            ovpropex ~ cantgeton +
              one_Play + two_Play + three_Play + four_Play + five_Play +
              one_Show + two_Show + three_Show + four_Show + five_Show +
              one_RA + two_RA + three_RA + four_RA + five_RA +
              one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred,
            data = w
          ),
          error = function(e) NULL
        )
        if (is.null(test)) return(.defaults())

        odds <- tryCatch(exp(confint(test, level = conf_level)), error = function(e) NULL)
        if (is.null(odds)) return(.defaults())
        z1 <- apply(odds, 3L, c)
        z2 <- expand.grid(dimnames(odds)[1:2])

        jz <- tryCatch(
          glm(
            (ovpropex == 5) ~
              one_Play + two_Play + three_Play + four_Play + five_Play +
              one_Show + two_Show + three_Show + four_Show + five_Show +
              one_RA + two_RA + three_RA + four_RA + five_RA +
              one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred,
            data = w, family = "binomial"
          ),
          error = function(e) NULL
        )
        if (is.null(jz)) return(.defaults())
        EXPcol <- exp(jz$coefficients[-1])

        df_all <- data.frame(z2, z1)
        keep <- df_all$Var2 == conf_label & df_all$Var1 != "(Intercept)" & df_all$Var1 != "Attend" & df_all$Var1 != "cantgeton"
        if (!any(keep)) return(.defaults())
        core <- df_all[keep, ]
        if (nrow(core) < 20) return(.defaults())

        weightedEEs_part <- data.frame(core[1:15, , drop = FALSE], X5 = EXPcol[1:15])
        can_core <- df_all[df_all$Var2 == conf_label & df_all$Var1 == "cantgeton", ]
        if (!nrow(can_core)) return(.defaults())
        cantride_part <- data.frame(can_core[1, , drop = FALSE], X5 = 1)
        pref_part <- data.frame(core[16:20, , drop = FALSE], X5 = EXPcol[16:20])

        list(weightedEEs = weightedEEs_part, cantride = cantride_part, pref = pref_part)
      }

      mk <- build_park_weights(1, 0.995, "99.8 %")
      ep <- build_park_weights(2, 0.99,  "99.5 %")
      st <- build_park_weights(3, 0.80,  "90 %")
      ak <- build_park_weights(4, 0.70,  "15 %")

      weightedEEs <- rbind(mk$weightedEEs, ep$weightedEEs, st$weightedEEs, ak$weightedEEs)
      cantride2 <- data.frame(FY = yearauto, rbind(mk$cantride, ep$cantride, st$cantride, ak$cantride))

      weights22 <- rbind(weightedEEs, mk$pref, ep$pref, st$pref, ak$pref)
      CantRideWeight22 <- cantride2[, -1, drop = FALSE]

      # ---------------------------
      # Apply weights + counts (fast)
      # ---------------------------
      metadata$POG[(metadata$Type == "Show" & is.na(metadata$POG) | metadata$Type == "Play" & is.na(metadata$POG))] <- 0
      metadata <- metadata[!is.na(metadata$Category1) | metadata$Type == "Ride", ]

      SurveyData22 <- SurveyData
      CountData22 <- SurveyData22

      rideagainx <- tolower(metadata[, 18])
      RIDEX <- tolower(metadata[which(!is.na(metadata[, 18])), 2])
      rideagainx <- rideagainx[!is.na(rideagainx)]
      RIDEX <- RIDEX[!is.na(RIDEX)]
      if (length(RIDEX) == length(rideagainx)) {
        for (i in seq_along(RIDEX)) {
          if (RIDEX[i] %in% names(SurveyData22) && rideagainx[i] %in% names(SurveyData22)) {
            idx <- which(SurveyData22[, RIDEX[i]] == 0 & SurveyData22[, rideagainx[i]] == 1 & SurveyData22$ovpropex < 6)
            if (length(idx)) SurveyData22[idx, RIDEX[i]] <- -1
          }
          if (RIDEX[i] %in% names(CountData22) && rideagainx[i] %in% names(CountData22)) {
            idx2 <- which(CountData22[, RIDEX[i]] == 0 & CountData22[, rideagainx[i]] == 1 & CountData22$ovpropex < 6)
            if (length(idx2)) CountData22[idx2, RIDEX[i]] <- -1
          }
        }
      }

      SurveyData22 <- apply_weights_fast(SurveyData22, metadata, weights22, CantRideWeight22, FQ)
      CountData22 <- apply_counts_fast(CountData22, metadata, FQ)

      # ---------------------------
      # Summaries (fast)
      # ---------------------------
      ride_bases <- unique(.base_name(tolower(metadata$Variable[metadata$Type == "Ride"])))
      ride_bases <- ride_bases[!is.na(ride_bases) & ride_bases != ""]

      Ride_Runs20 <- as_Park_LifeStage_QTR(summarize_wide_by_group(SurveyData22, ride_bases, "2"))
      Ride_Against20 <- as_Park_LifeStage_QTR(summarize_wide_by_group(SurveyData22, ride_bases, "3"))
      Ride_OriginalRuns20 <- as_Park_LifeStage_QTR(summarize_wide_by_group(CountData22, ride_bases, "2"))
      Ride_OriginalAgainst20 <- as_Park_LifeStage_QTR(summarize_wide_by_group(CountData22, ride_bases, "3"))

      Ride_Runs20 <- strip_measure_suffix(Ride_Runs20, "2")
      Ride_Against20 <- strip_measure_suffix(Ride_Against20, "3")
      Ride_OriginalRuns20 <- strip_measure_suffix(Ride_OriginalRuns20, "2")
      Ride_OriginalAgainst20 <- strip_measure_suffix(Ride_OriginalAgainst20, "3")

      # Critical: align ride names to taxonomy
      Ride_Runs20 <- rename_ride_measures_to_taxonomy(Ride_Runs20, metadata)
      Ride_Against20 <- rename_ride_measures_to_taxonomy(Ride_Against20, metadata)
      Ride_OriginalRuns20 <- rename_ride_measures_to_taxonomy(Ride_OriginalRuns20, metadata)
      Ride_OriginalAgainst20 <- rename_ride_measures_to_taxonomy(Ride_OriginalAgainst20, metadata)

      Show_Runs20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(SurveyData22, metadata, type = "Show", suffix = "2"))
      Show_Against20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(SurveyData22, metadata, type = "Show", suffix = "3"))
      Show_OriginalRuns20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(CountData22, metadata, type = "Show", suffix = "2"))
      Show_OriginalAgainst20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(CountData22, metadata, type = "Show", suffix = "3"))

      Play_Runs20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(SurveyData22, metadata, type = "Play", suffix = "2"))
      Play_Against20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(SurveyData22, metadata, type = "Play", suffix = "3"))
      Play_OriginalRuns20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(CountData22, metadata, type = "Play", suffix = "2"))
      Play_OriginalAgainst20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(CountData22, metadata, type = "Play", suffix = "3"))

      # ---------------------------
      # Build wRAA_Table + EARS
      # ---------------------------
      build_long_block <- function(orig_rs, orig_ra, w_rs, w_ra, genre) {
        ids <- c("Park", "LifeStage", "QTR")
        if (ncol(orig_rs) <= length(ids) || ncol(orig_ra) <= length(ids) || ncol(w_rs) <= length(ids) || ncol(w_ra) <= length(ids)) {
          return(data.frame(Park = integer(0), LifeStage = integer(0), QTR = integer(0), NAME = character(0),
                            Original_RS = numeric(0), Original_RA = numeric(0), wEE = numeric(0), wEEx = numeric(0),
                            Genre = character(0)))
        }
        m1 <- melt(orig_rs, id = ids); names(m1)[4:5] <- c("NAME", "Original_RS")
        m2 <- melt(orig_ra, id = ids); names(m2)[4:5] <- c("NAME", "Original_RA")
        m3 <- melt(w_rs,   id = ids); names(m3)[4:5] <- c("NAME", "wEE")
        m4 <- melt(w_ra,   id = ids); names(m4)[4:5] <- c("NAME", "wEEx")

        key <- c(ids, "NAME")
        out <- merge(m1, m2[, c(key, "Original_RA")], by = key, all = TRUE)
        out <- merge(out, m3[, c(key, "wEE")],        by = key, all = TRUE)
        out <- merge(out, m4[, c(key, "wEEx")],       by = key, all = TRUE)

        out$Original_RS[is.na(out$Original_RS)] <- 0
        out$Original_RA[is.na(out$Original_RA)] <- 0
        out$wEE[is.na(out$wEE)] <- 0
        out$wEEx[is.na(out$wEEx)] <- 0
        out$Genre <- genre

        out <- out[rowSums(out[, c("Original_RS", "Original_RA", "wEE", "wEEx")] != 0, na.rm = TRUE) > 0, ]
        out
      }

      Ride <- build_long_block(Ride_OriginalRuns20, Ride_OriginalAgainst20, Ride_Runs20, Ride_Against20, "Ride")
      Show <- build_long_block(Show_OriginalRuns20, Show_OriginalAgainst20, Show_Runs20, Show_Against20, "Show")
      Play <- build_long_block(Play_OriginalRuns20, Play_OriginalAgainst20, Play_Runs20, Play_Against20, "Play")
      if (nrow(Play)) Play <- Play[Play$NAME != "Park.1" & Play$NAME != "LifeStage.1" & Play$NAME != "QTR.1", ]

      wRAA_Table <- rbind(Ride, Show, Play)
      wRAA_Table <- .normalize_ears_join_keys(wRAA_Table)

      twox <- wRAA_Table %>%
        group_by(NAME, Park, Genre, QTR, LifeStage) %>%
        mutate(Original_RS = sum(Original_RS), Original_RA = sum(Original_RA), wEE = sum(wEE), wEEx = sum(wEEx)) %>%
        distinct(.keep_all = FALSE)

      onex <- twox %>%
        group_by(NAME, Park, QTR) %>%
        mutate(sum = sum(Original_RS + Original_RA))
      onex$Percent <- (onex$Original_RS + onex$Original_RA) / onex$sum

      wRAA_Table <- sqldf('select a.*,b.Percent from twox a left join onex b on a.Park = b.Park and a.QTR=b.QTR and a.LifeStage = b.LifeStage and a.Name = b.Name')
      wRAA_Table <- wRAA_Table[!is.na(wRAA_Table$Park), ]

      wRAA_Table <- cbind(wRAA_Table, wOBA = wRAA_Table$wEE / (wRAA_Table$Original_RS + wRAA_Table$Original_RA))

      wRAA_Table <- merge(x = wRAA_Table, y = aggregate(wRAA_Table$Original_RS, by = list(QTR = wRAA_Table$QTR, Park = wRAA_Table$Park), FUN = sum),
                          by = c("QTR", "Park"), all.x = TRUE)
      names(wRAA_Table)[length(names(wRAA_Table))] <- "OBP1"
      wRAA_Table <- merge(x = wRAA_Table, y = aggregate(wRAA_Table$Original_RA, by = list(QTR = wRAA_Table$QTR, Park = wRAA_Table$Park), FUN = sum),
                          by = c("QTR", "Park"), all.x = TRUE)
      names(wRAA_Table)[length(names(wRAA_Table))] <- "OBP2"
      wRAA_Table$OBP <- wRAA_Table$OBP1 / (wRAA_Table$OBP1 + wRAA_Table$OBP2)

      wRAA_Table <- merge(x = wRAA_Table, y = aggregate(wRAA_Table$wOBA, by = list(QTR = wRAA_Table$QTR, Park = wRAA_Table$Park), FUN = median),
                          by = c("QTR", "Park"), all.x = TRUE)
      names(wRAA_Table)[length(names(wRAA_Table))] <- "wOBA_Park"
      wRAA_Table <- data.frame(wRAA_Table, wOBA_Scale = wRAA_Table$wOBA_Park / wRAA_Table$OBP)

      join_keys <- c("NAME", "Park", "Genre", "QTR", "LifeStage")
      table2_nodup <- EARS[, setdiff(names(EARS), setdiff(names(wRAA_Table), join_keys))]
      EARSFinal_Final <- merge(wRAA_Table, table2_nodup, by = join_keys, all = FALSE)

      EARSx <- EARSFinal_Final
      EARSx$wRAA <- ((EARSx$wOBA - EARSx$wOBA_Park) / EARSx$wOBA_Scale) * (EARSx$wRAA_Table.AnnualGuestsCarried)
      EARSx$EARS <- EARSx$wRAA / EARSx$RPW
      EARSx$EARS <- (EARSx$EARS + EARSx$replacement) * EARSx$p

      EARSTotal <- rbind(EARSTotal, EARSx)

      FQ <- FQ + 1L
    }

    EARS$Actual_EARS <- EARS$EARS
    result <- full_join(EARSTotal, EARS[, c("NAME", "Park", "Genre", "QTR", "LifeStage", "Actual_EARS")],
                        by = c("NAME", "Park", "Genre", "QTR", "LifeStage"))
    result$EARS[is.na(result$EARS)] <- 0
    colnames(result)[colnames(result) == "EARS"] <- "Simulation_EARS"
    result$Incremental_EARS <- result$Simulation_EARS - result$Actual_EARS
    result$sim_run <- run
    result
    }, error = function(e) {
      # Re-throw with high-signal context so Dataiku shows *where* it failed.
      msg <- paste0(
        "sim_run=", run,
        " stage=", stage,
        " call=", paste(deparse(conditionCall(e)), collapse = " "),
        " msg=", conditionMessage(e)
      )
      stop(msg, call. = FALSE)
    })
  }

  EARSTotal_list
}
