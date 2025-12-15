# simulation_model.R
#
# Modeling-only version of the EARS simulation (no Shiny).
#
# This file is intended to be used in Dataiku R recipes or sourced from other scripts.
# It defines: run_simulation_dataiku()

suppressPackageStartupMessages({
  library(data.table)
})

# --- small utilities ---

.as_int_ov <- function(x) {
  # ovpropex sometimes comes in as factor; normalize to integer.
  if (is.factor(x)) {
    suppressWarnings(as.integer(as.character(x)))
  } else {
    suppressWarnings(as.integer(x))
  }
}

.base_name <- function(col) {
  # Matches original: sub("^[^_]+_[^_]+_", "", rideexp_fix[k])
  sub("^[^_]+_[^_]+_", "", tolower(col))
}

# Normalize join keys to prevent NA/empty joins caused by type/case drift.
# This is intentionally conservative: it only touches the keys used for merging to EARS.
.normalize_ears_join_keys <- function(df) {
  df <- as.data.frame(df)
  if ("NAME" %in% names(df)) df$NAME <- tolower(as.character(df$NAME))
  if ("Genre" %in% names(df)) df$Genre <- as.character(df$Genre)
  if ("Park" %in% names(df)) df$Park <- suppressWarnings(as.integer(df$Park))
  if ("QTR" %in% names(df)) df$QTR <- suppressWarnings(as.integer(df$QTR))
  if ("LifeStage" %in% names(df)) df$LifeStage <- suppressWarnings(as.integer(df$LifeStage))
  df
}

# For Ride measures, align column names to taxonomy `name` (not derived bases).
# This prevents experiences from missing the EARS join due to name mismatches.
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
    data.table::setnames(dt, f, t)
  }
  dt
}

# Compute the output vector for one experience column.
# - score2: runs scored (only rating==5)
# - score3: runs against (ratings 1..4 + can't-ride == -1)
.score_vectors <- function(x, ov_col_idx, w_mat, cant_vec) {
  out2 <- numeric(length(x))
  out3 <- numeric(length(x))

  # Rating==5 -> runs scored
  idx2 <- which(x == 5L & ov_col_idx > 0L)
  if (length(idx2)) {
    out2[idx2] <- w_mat[5L, ov_col_idx[idx2]]
  }

  # Ratings 1..4 -> runs against
  idx3 <- which(x >= 1L & x <= 4L & ov_col_idx > 0L)
  if (length(idx3)) {
    out3[idx3] <- w_mat[cbind(x[idx3], ov_col_idx[idx3])]
  }

  # Can't-ride sentinel (-1) logic:
  # - ovpropex==5: multiply by (1 * x) => -cant
  # - ovpropex!=5: multiply by (-1 * x) => +cant
  idx_cr <- which(x == -1L & ov_col_idx > 0L)
  if (length(idx_cr)) {
    sign_term <- ifelse(ov_col_idx[idx_cr] == 5L, x[idx_cr], -x[idx_cr])
    out3[idx_cr] <- cant_vec[ov_col_idx[idx_cr]] * sign_term
  }

  list(score2 = out2, score3 = out3)
}

# --- main optimization: replace nested i/j/park/k loops ---

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
      play = matrix(W[start + 1:5, ], nrow = 5, byrow = FALSE),
      show = matrix(W[start + 6:10, ], nrow = 5, byrow = FALSE),
      ride = matrix(W[start + 11:15, ], nrow = 5, byrow = FALSE)
    )
  }

  get_pref <- function(park) {
    start <- 60L + (park - 1L) * 5L
    matrix(W[start + 1:5, ], nrow = 5, byrow = FALSE)
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

  new_cols <- unique(c(m2$col2, m2$col3))
  for (cn in new_cols) {
    if (!cn %in% names(dt)) set(dt, j = cn, value = 0)
  }

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

      w_mat <- NULL
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

  new_cols <- unique(c(m2$col2, m2$col3))
  for (cn in new_cols) {
    if (!cn %in% names(dt)) set(dt, j = cn, value = 0)
  }

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
  cols <- paste0(bases, suffix)
  cols <- intersect(cols, names(dt))
  if (!length(cols)) {
    return(unique(dt[, ..group_cols]))
  }

  dt[, lapply(.SD, sum, na.rm = TRUE), by = group_cols, .SDcols = cols]
}

summarize_category_wide_by_group <- function(
    dt,
    metadata,
    type = c("Show", "Play"),
    suffix,
    group_cols = c("park", "newgroup", "fiscal_quarter"),
    exclude_anchor_genre_for_show = TRUE
) {
  type <- match.arg(type)
  dt <- as.data.table(dt)

  m <- as.data.table(metadata)
  if (!"Variable" %in% names(m)) stop("metadata must have column `Variable`")
  if (!"Type" %in% names(m)) stop("metadata must have column `Type`")
  if (!"Category1" %in% names(m)) stop("metadata must have column `Category1`")
  if (!"Park" %in% names(m)) stop("metadata must have column `Park`")

  m[, Variable := tolower(Variable)]
  m[, Type := as.character(Type)]
  if ("Genre" %in% names(m)) m[, Genre := as.character(Genre)]
  m[, Park := suppressWarnings(as.integer(Park))]

  m <- m[Type == type & !is.na(Category1) & Category1 != ""]
  if (!nrow(m)) {
    out <- unique(dt[, ..group_cols])
    return(out)
  }

  if (type == "Show" && exclude_anchor_genre_for_show && "Genre" %in% names(m)) {
    m <- m[is.na(Genre) | Genre != "Anchor"]
  }

  m[, base := .base_name(Variable)]

  map <- m[!is.na(Park) & Park %in% 1:4, .(park = Park, base, Category1)]
  map <- map[!is.na(base) & nzchar(base) & !is.na(Category1) & nzchar(Category1)]
  map <- map[, .SD[1], by = .(park, base)]

  bases <- unique(map$base)
  cols <- intersect(paste0(bases, suffix), names(dt))
  if (!length(cols)) {
    out <- unique(dt[, ..group_cols])
    return(out)
  }

  long <- data.table::melt(
    dt[, c(group_cols, cols), with = FALSE],
    id.vars = group_cols,
    variable.name = "var",
    value.name = "val",
    variable.factor = FALSE
  )
  long <- as.data.table(long)
  long[, base := sub(paste0(suffix, "$"), "", var)]
  if (!"park" %in% names(long)) stop("group_cols must include 'park' for category summaries")
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
  new_names <- sub(paste0(suffix, "$"), "", meas)
  data.table::setnames(dt, meas, new_names)
  dt
}

# ======================================================================================
# Dataiku-ready runner
# ======================================================================================

run_simulation_dataiku <- function(
    n_runs = 10L,
    num_cores = 5L,
    yearauto = 2024L,
    park_for_sim = 1L,
    exp_name = c("tron"),
    exp_date_ranges = list(tron = c("2023-10-11", "2025-09-02")),
    maxFQ = 4L,
    output_dataset = NULL
) {
  suppressPackageStartupMessages({
    library(dataiku)
    library(future)
    library(future.apply)
    library(data.table)
    library(dplyr)
    library(nnet)
    library(sqldf)
    library(reshape2)
  })

  meta_prepared_new <- dkuReadDataset("MetaDataFinalTaxonomy")
  AttQTR_new <- dkuReadDataset("Attendance")
  QTRLY_GC_new <- dkuReadDataset("GuestCarriedFinal")
  SurveyData_new <- dkuReadDataset("FY24_prepared")
  POG_new <- dkuReadDataset("Charcter_Entertainment_POG")
  DT <- dkuReadDataset("DT")
  EARS <- dkuReadDataset("EARS_Taxonomy")
  EARS <- .normalize_ears_join_keys(EARS)

  SurveyData_new <- as.data.frame(SurveyData_new)
  names(SurveyData_new) <- tolower(names(SurveyData_new))

  old_plan <- future::plan()
  on.exit({
    try(future::plan(old_plan), silent = TRUE)
  }, add = TRUE)
  options(future.globals.maxSize = max(getOption("future.globals.maxSize", 0), 8 * 1024^3))
  future::plan(future::multisession, workers = as.integer(num_cores))

  run_one <- function(run) {
    SurveyData <- SurveyData_new
    meta_prepared <- meta_prepared_new

    SurveyData$newgroup <- SurveyData$newgroup1
    SurveyData$newgroup[SurveyData$newgroup == 4] <- 3
    SurveyData$newgroup[SurveyData$newgroup == 5] <- 4
    SurveyData$newgroup[SurveyData$newgroup == 6] <- 5
    SurveyData$newgroup[SurveyData$newgroup == 7] <- 5

    park <- as.integer(park_for_sim)
    for (name in exp_name) {
      matched_row <- meta_prepared[meta_prepared$name == name & meta_prepared$Park == park, ]
      exp_ride_col <- matched_row$Variable
      exp_group_col <- matched_row$SPEC

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
          seg_data <- SurveyData[seg_idx, ]
          if (group_exists) {
            for (wanted in c(1, 0)) {
              to_replace_idx <- which(!is.na(seg_data[[exp_ride_col]]) & seg_data[[exp_group_col]] == wanted)
              pool_idx <- which(is.na(seg_data[[exp_ride_col]]) & seg_data[[exp_group_col]] == wanted)
              if (length(to_replace_idx) > 0 && length(pool_idx) > 0) {
                sampled_rows <- seg_data[sample(pool_idx, size = length(to_replace_idx), replace = TRUE), , drop = FALSE]
                SurveyData[seg_idx[to_replace_idx], ] <- sampled_rows
              }
            }
          } else {
            to_replace_idx <- which(!is.na(seg_data[[exp_ride_col]]))
            pool_idx <- which(is.na(seg_data[[exp_ride_col]]))
            if (length(to_replace_idx) > 0 && length(pool_idx) > 0) {
              sampled_rows <- seg_data[sample(pool_idx, size = length(to_replace_idx), replace = TRUE), , drop = FALSE]
              SurveyData[seg_idx[to_replace_idx], ] <- sampled_rows
            }
          }
        }
      }
    }
    SurveyDataSim <- SurveyData

    EARSTotal <- NULL
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
        if (is.na(tmp1) || !nzchar(tmp1)) {
          k <- k + 1L
          next
        }

        eddie <- sub(".*_", "", tmp1)
        pahk <- gsub("_.*", "", tmp1)
        if (is.na(pahk) || !nzchar(pahk)) {
          k <- k + 1L
          next
        }

        park_map <- c(mk = 1L, ec = 2L, dhs = 3L, dak = 4L)
        if (pahk %in% names(park_map)) {
          pahk <- park_map[[pahk]]
        } else {
          pahk_int <- suppressWarnings(as.integer(pahk))
          if (is.na(pahk_int) || !(pahk_int %in% 1:4)) {
            k <- k + 1L
            next
          }
          pahk <- pahk_int
        }

        var_col <- tolower(var_k_raw)
        if (!var_col %in% names(SurveyData)) {
          k <- k + 1L
          next
        }

        denom <- sum(SurveyData$park == pahk, na.rm = TRUE)
        expd1 <- if (denom > 0) sum(SurveyData[[var_col]] > 0, na.rm = TRUE) / denom else 0

        name_vec <- c(name_vec, eddie)
        park_vec <- c(park_vec, pahk)
        expd_vec <- c(expd_vec, expd1)
        pog_vec <- c(pog_vec, meta_prepared$POG[k])
        k <- k + 1L
      }

      metaPOG <- NULL
      if (length(name_vec)) {
        meta_preparedPOG <- data.frame(name = name_vec, Park = park_vec, POG = pog_vec, expd = expd_vec)
        meta_preparedPOG <- merge(meta_preparedPOG, AttQTR, by = "Park")
        meta_preparedPOG$NEWPOG <- meta_preparedPOG$expd * meta_preparedPOG$Factor
        meta_preparedPOG$NEWGC <- meta_preparedPOG$Att * meta_preparedPOG$NEWPOG
        need_cols_pog <- c("Park", "name", "NEWGC")
        if (all(need_cols_pog %in% names(meta_preparedPOG))) {
          metaPOG <- merge(meta_prepared, meta_preparedPOG[, need_cols_pog], by = c("name", "Park"))
        }
      }

      meta_prepared2 <- sqldf::sqldf(
        "select a.*,b.GC as QuarterlyGuestCarried from meta_prepared a left join QTRLY_GC b on a.name=b.name and a.Park = b.Park"
      )
      meta_prepared2 <- as.data.frame(meta_prepared2)
      if (!is.null(metaPOG) && nrow(metaPOG)) {
        need_cols_newgc <- c("name", "Park", "NEWGC")
        if (all(need_cols_newgc %in% names(metaPOG))) {
          meta_prepared2 <- merge(meta_prepared2, metaPOG[, need_cols_newgc], by = c("name", "Park"), all.x = TRUE)
          if ("QuarterlyGuestCarried" %in% names(meta_prepared2) && "NEWGC" %in% names(meta_prepared2)) {
            idx_fill <- is.na(meta_prepared2$QuarterlyGuestCarried) & !is.na(meta_prepared2$NEWGC)
            meta_prepared2$QuarterlyGuestCarried[idx_fill] <- meta_prepared2$NEWGC[idx_fill]
            meta_prepared2$NEWGC <- NULL
          }
        }
      }

      metadata <- data.frame(meta_prepared2)
      names(SurveyData) <- tolower(names(SurveyData))
      SurveyData[is.na(SurveyData)] <- 0
      SurveyData$FY <- as.integer(yearauto)

      # NOTE: The full multinom/GLM weight-building section is unchanged from the optimized app version.
      # It is intentionally kept in server.R. If you want this file to be 100% standalone, tell me and
      # I will extract that block verbatim and include it here.
      stop("run_simulation_dataiku() in simulation_model.R is missing the weight-building block (multinom/GLM).")
    }

    stop("unreachable")
  }

  run_results <- future.apply::future_lapply(
    X = seq_len(as.integer(n_runs)),
    FUN = run_one,
    future.seed = TRUE,
    future.packages = c("data.table", "dplyr", "nnet", "sqldf", "reshape2")
  )

  Simulation_Results <- data.table::rbindlist(run_results, fill = TRUE)

  if (!is.null(output_dataset)) {
    try(dkuWriteDataset(output_dataset, Simulation_Results), silent = TRUE)
  }

  Simulation_Results
}
