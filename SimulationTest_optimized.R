# Optimized helpers for SimulationTest (drop-in)
#
# Focus: replace the slow nested loops that create the *2/*3 columns and the
# repeated string parsing / per-category aggregate loops.
#
# Assumptions:
# - `metadata` is the same table you create from `meta_prepared2`.
# - `weights22` and `CantRideWeight22` have the same structure as in your script
#   (numeric weights in columns 3:7, ovpropex levels 1..5).
# - `SurveyData22` / `CountData22` contain `park`, `ovpropex`, `fiscal_quarter`, `newgroup`.

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
  # Matches your original: sub("^[^_]+_[^_]+_", "", rideexp_fix[k])
  sub("^[^_]+_[^_]+_", "", tolower(col))
}

# Compute the output vector for one experience column.
# - score2: runs scored (only rating==5)
# - score3: runs against (ratings 1..4 + can't-ride == -1)
.score_vectors <- function(x, ov_col_idx, w_mat, cant_vec) {
  # x: integer ratings vector (can include -1,0,1..5)
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

  # Can't-ride sentinel (-1) logic, preserving your sign behavior:
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

# Apply the weights to create *2 and *3 columns.
# This replaces the extremely slow nested loops in your script.
apply_weights_fast <- function(dt, metadata, weights22, CantRideWeight22, FQ) {
  dt <- as.data.table(dt)

  # Restrict to the quarter once; preserves your filtering semantics.
  # (If you already filtered dt before calling, this is still safe.)
  dt <- dt[fiscal_quarter == FQ]

  # Precompute per-row ovpropex col index 1..5 (0 for out-of-range).
  ov <- .as_int_ov(dt$ovpropex)
  ov_col_idx <- match(ov, 1:5)
  ov_col_idx[is.na(ov_col_idx)] <- 0L

  # Numeric weight matrices (5x5) per park + group.
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

  # CantRideWeight22 has parks in rows 1..4, weights in cols 3..7
  CR <- as.matrix(CantRideWeight22[, 3:7, drop = FALSE])

  # Prepare metadata for selecting columns.
  m <- as.data.table(metadata)
  m[, Variable := tolower(Variable)]
  m[, Type := as.character(Type)]
  m[, Genre := as.character(Genre)]
  m[, Park := as.integer(Park)]

  # We only process experiences that exist in dt.
  vars_present <- intersect(m$Variable, names(dt))
  if (!length(vars_present)) return(dt)

  # Base names and output column names.
  m2 <- unique(m[Variable %in% vars_present, .(Variable, Type, Genre, Park)])
  m2[, base := .base_name(Variable)]
  m2[, col2 := paste0(base, "2")]
  m2[, col3 := paste0(base, "3")]

  # Ensure output columns exist (fast set; no rep(0, n)).
  new_cols <- unique(c(m2$col2, m2$col3))
  for (cn in new_cols) {
    if (!cn %in% names(dt)) set(dt, j = cn, value = 0)
  }

  # Apply by park to keep weights logic correct and avoid repeated which().
  for (park in 1:4) {
    rows_p <- which(dt$park == park)
    if (!length(rows_p)) next

    blocks <- get_block15(park)
    pref_mat <- get_pref(park)
    cant_vec <- CR[park, ]

    # Metadata rows for this park
    mp <- m2[Park == park]
    if (!nrow(mp)) next

    # For each experience column in this park, compute score2/score3 vectors.
    # This is still a loop over columns, but it removes the inner i/j loops and which() explosions.
    for (r in seq_len(nrow(mp))) {
      var <- mp$Variable[r]
      x <- dt[[var]][rows_p]

      # Normalize to integer for fast comparisons.
      if (is.factor(x)) x <- suppressWarnings(as.integer(as.character(x)))
      x[is.na(x)] <- 0L
      x <- as.integer(x)

      # Pick weight matrix based on Type/Genre rules from your original code.
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
        # Fallback for Show/Anchor edge cases.
        w_mat <- blocks$show
      } else {
        # Unknown type: skip rather than guess.
        next
      }

      sc <- .score_vectors(x, ov_col_idx[rows_p], w_mat, cant_vec)

      # Write into dt
      set(dt, i = rows_p, j = mp$col2[r], value = sc$score2)
      set(dt, i = rows_p, j = mp$col3[r], value = sc$score3)
    }
  }

  dt
}

# Apply raw-count logic to create *2/*3 columns on CountData22.
# Equivalent to your CountData loops, but vectorized.
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

      # *2 is rating==5
      out2 <- as.numeric(x == 5L & ov_ok[rows_p])

      # *3 is rating in 1..4
      out3 <- as.numeric(x >= 1L & x <= 4L & ov_ok[rows_p])

      # can't-ride sentinel mirrors your CountData logic
      idx_cr <- which(x == -1L & ov_ok[rows_p])
      if (length(idx_cr)) {
        out3[idx_cr] <- -1 * x[idx_cr]  # => 1
      }

      set(dt, i = rows_p, j = mp$col2[r], value = out2)
      set(dt, i = rows_p, j = mp$col3[r], value = out3)
    }
  }

  dt
}

# Summarize a set of *2/*3 columns by (park,newgroup,fiscal_quarter).
# This replaces many repeated aggregate() calls.
summarize_wide_by_group <- function(dt, bases, suffix, group_cols = c("park", "newgroup", "fiscal_quarter")) {
  dt <- as.data.table(dt)
  cols <- paste0(bases, suffix)
  cols <- intersect(cols, names(dt))
  if (!length(cols)) {
    return(dt[, c(setNames(list(NULL), character(0)))])
  }

  dt[, lapply(.SD, sum, na.rm = TRUE), by = group_cols, .SDcols = cols]
}

# Summarize Show/Play into Category1 columns (wide), by (park,newgroup,fiscal_quarter).
# This replaces the massive per-park/per-category aggregate() loops for Show_* and Play_*.
#
# Returns columns:
# - park, newgroup, fiscal_quarter, <Category1_1>, <Category1_2>, ...
summarize_category_wide_by_group <- function(
    dt,
    metadata,
    type = c("Show", "Play"),
    suffix,
    group_cols = c("park", "newgroup", "fiscal_quarter"),
    # Mirrors your Show logic: exclude Anchor shows from show-category buckets
    exclude_anchor_genre_for_show = TRUE
) {
  type <- match.arg(type)
  dt <- as.data.table(dt)

  m <- as.data.table(metadata)
  if (!"Variable" %in% names(m)) stop("metadata must have column `Variable`")
  if (!"Type" %in% names(m)) stop("metadata must have column `Type`")
  if (!"Category1" %in% names(m)) stop("metadata must have column `Category1`")

  m[, Variable := tolower(Variable)]
  m[, Type := as.character(Type)]
  if ("Genre" %in% names(m)) m[, Genre := as.character(Genre)]

  m <- m[Type == type & !is.na(Category1) & Category1 != ""]
  if (!nrow(m)) {
    # Return empty-ish grouped skeleton
    out <- unique(dt[, ..group_cols])
    return(out)
  }

  if (type == "Show" && exclude_anchor_genre_for_show && "Genre" %in% names(m)) {
    m <- m[is.na(Genre) | Genre != "Anchor"]
  }

  m[, base := .base_name(Variable)]

  # Build mapping from base -> Category1 (dedupe).
  map <- unique(m[, .(base, Category1)])

  # Columns available in dt
  bases <- unique(map$base)
  cols <- intersect(paste0(bases, suffix), names(dt))
  if (!length(cols)) {
    out <- unique(dt[, ..group_cols])
    return(out)
  }

  # Melt only relevant columns, map to Category1 by stripping suffix.
  long <- melt(
    dt[, c(group_cols, cols), with = FALSE],
    id.vars = group_cols,
    variable.name = "var",
    value.name = "val",
    variable.factor = FALSE
  )
  long[, base := sub(paste0(suffix, "$"), "", var)]
  long <- long[map, on = "base", nomatch = 0L]

  # Aggregate by category
  agg <- long[, .(val = sum(val, na.rm = TRUE)), by = c(group_cols, "Category1")]

  # Wide
  dcast(agg, as.formula(paste(paste(group_cols, collapse = " + "), "~ Category1")), value.var = "val", fill = 0)
}

# Convenience wrappers to match your downstream variable names
# (these return Park/LifeStage/QTR columns like your existing tables).
as_Park_LifeStage_QTR <- function(dt, park_col = "park", life_col = "newgroup", qtr_col = "fiscal_quarter") {
  dt <- as.data.table(dt)
  setnames(dt, c(park_col, life_col, qtr_col), c("Park", "LifeStage", "QTR"), skip_absent = TRUE)
  dt[]
}

# ======================================================================================
# Dataiku-ready runner
# ======================================================================================
#
# Paste this whole file into a Dataiku R recipe, or keep it as a project library.
#
# Expected inputs (same names as your original script):
# - MetaDataFinalTaxonomy, Attendance, GuestCarriedFinal, FY24_prepared, Charcter_Entertainment_POG, DT, EARS_Taxonomy
#
# Output:
# - returns a data.frame `Simulation_Results`
# - optionally writes to an output dataset if `output_dataset` is provided
#
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
    library(foreach)
    library(doParallel)
    library(data.table)
    library(dplyr)
    library(nnet)
    library(sqldf)
    library(reshape2)
  })

  # ---- Read inputs once (BIG speed win vs reading inside workers) ----
  meta_prepared_new <- dkuReadDataset("MetaDataFinalTaxonomy")
  AttQTR_new <- dkuReadDataset("Attendance")
  QTRLY_GC_new <- dkuReadDataset("GuestCarriedFinal")
  SurveyData_new <- dkuReadDataset("FY24_prepared")
  POG_new <- dkuReadDataset("Charcter_Entertainment_POG") # kept for parity (used elsewhere in your flow)
  DT <- dkuReadDataset("DT")
  EARS <- dkuReadDataset("EARS_Taxonomy")

  # Normalize column names once
  SurveyData_new <- as.data.frame(SurveyData_new)
  names(SurveyData_new) <- tolower(names(SurveyData_new))

  # Parallel setup
  cl <- makeCluster(as.integer(num_cores))
  doParallel::registerDoParallel(cl)
  on.exit({
    try(stopCluster(cl), silent = TRUE)
  }, add = TRUE)

  # ---- Parallel runs ----
  EARSTotal_list <- foreach(
    run = 1:as.integer(n_runs),
    .combine = rbind,
    .packages = c("dataiku", "data.table", "dplyr", "nnet", "sqldf", "reshape2")
  ) %dopar% {
    # Local copies in each worker
    SurveyData <- SurveyData_new
    meta_prepared <- meta_prepared_new

    # --- your original newgroup cleanup ---
    SurveyData$newgroup <- SurveyData$newgroup1
    SurveyData$newgroup[SurveyData$newgroup == 4] <- 3
    SurveyData$newgroup[SurveyData$newgroup == 5] <- 4
    SurveyData$newgroup[SurveyData$newgroup == 6] <- 5
    SurveyData$newgroup[SurveyData$newgroup == 7] <- 5

    # --- Monte Carlo simulation block (kept same logic, fewer re-reads) ---
    park <- as.integer(park_for_sim)
    for (name in exp_name) {
      matched_row <- meta_prepared[meta_prepared$name == name & meta_prepared$Park == park, ]
      if (!nrow(matched_row)) next

      exp_ride_col <- matched_row$Variable
      exp_group_col <- matched_row$SPEC

      ride_exists <- exp_ride_col %in% colnames(SurveyData)
      group_exists <- exp_group_col %in% colnames(SurveyData)
      if (!ride_exists) next

      segments <- unique(SurveyData$newgroup[SurveyData$park == park])
      date_range <- exp_date_ranges[[name]]
      if (is.null(date_range)) next

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

    SurveyDataSim <- SurveyData

    # ---- quarterly loop ----
    EARSTotal <- NULL
    FQ <- 1L
    while (FQ < as.integer(maxFQ) + 1L) {
      SurveyData <- SurveyDataSim
      SurveyData <- SurveyData[SurveyData$fiscal_quarter == FQ, ]
      if (!nrow(SurveyData)) {
        FQ <- FQ + 1L
        next
      }

      # --- Metadata + Attendance + GC for quarter ---
      meta_prepared <- meta_prepared_new
      AttQTR <- AttQTR_new[AttQTR_new$FQ == FQ, ]
      QTRLY_GC <- QTRLY_GC_new[QTRLY_GC_new$FQ == FQ, ]

      # --- POG backfill (kept from your script, but computed once) ---
      k <- 1L
      name_vec <- character(0)
      park_vec <- integer(0)
      expd_vec <- numeric(0)
      while (k < length(meta_prepared$Variable) + 1L) {
        # Parse the metadata variable the same way as your original code, but safely.
        # (The original nested unlist/strsplit expression is easy to break with parentheses.)
        var_k <- as.character(meta_prepared$Variable[k])
        tmp1 <- strsplit(var_k, "charexp_", fixed = TRUE)[[1]][1]
        tmp1 <- strsplit(tmp1, "entexp_", fixed = TRUE)[[1]][1]
        tmp1 <- strsplit(tmp1, "ridesexp_", fixed = TRUE)[[1]][1]
        eddie <- sub(".*_", "", tmp1)
        pahk <- gsub("_.*", "", tmp1)
        if (pahk == "dak") pahk <- 4
        if (pahk == "mk") pahk <- 1
        if (pahk == "ec") pahk <- 2
        if (pahk == "dhs") pahk <- 3

        expd1 <- sum(SurveyData[, meta_prepared$Variable[k]] > 0, na.rm = TRUE) / nrow(SurveyData[SurveyData$park == pahk, ])
        name_vec <- c(name_vec, eddie)
        park_vec <- c(park_vec, pahk)
        expd_vec <- c(expd_vec, expd1)
        k <- k + 1L
      }
      meta_preparedPOG <- data.frame(name = name_vec, Park = park_vec, POG = meta_prepared$POG, expd = expd_vec)
      meta_preparedPOG <- merge(meta_preparedPOG, AttQTR, by = "Park")
      meta_preparedPOG$NEWPOG <- meta_preparedPOG$expd * meta_preparedPOG$Factor
      meta_preparedPOG$NEWGC <- meta_preparedPOG$Att * meta_preparedPOG$NEWPOG
      metaPOG <- merge(meta_prepared, meta_preparedPOG[, c("Park", "name", "NEWGC")], by = c("name", "Park"))

      meta_prepared2 <- sqldf::sqldf(
        "select a.*,b.GC as QuarterlyGuestCarried from meta_prepared a left join QTRLY_GC b on a.name=b.name and a.Park = b.Park"
      )
      setDT(meta_prepared2)
      setDT(metaPOG)
      meta_prepared2[metaPOG, on = c("name", "Park"), QuarterlyGuestCarried := i.NEWGC]

      metadata <- data.frame(meta_prepared2)
      names(SurveyData) <- tolower(names(SurveyData))
      SurveyData[is.na(SurveyData)] <- 0
      SurveyData <- cbind(SurveyData, FY = yearauto)

      # ==================================================================================
      # Your multinom/GLM block to construct `weights22` + `CantRideWeight22` goes here.
      # Kept as-is from your original script (not reprinted here to keep this file usable).
      #
      # REQUIREMENT:
      # - after this block finishes, you must have:
      #   - weights22 : data.frame with numeric weights in columns 3:7
      #   - CantRideWeight22 : data.frame/matrix with numeric weights in columns 3:7 and 4 rows (parks 1..4)
      # ==================================================================================
      stop("Paste your existing park-weight model code to produce `weights22` and `CantRideWeight22` before continuing.")

      # --- From here down is fully optimized replacement for the giant loop/aggregate section ---

      # Prep
      metadata$POG[(metadata$Type == "Show" & is.na(metadata$POG) | metadata$Type == "Play" & is.na(metadata$POG))] <- 0
      metadata <- metadata[!is.na(metadata$Category1) | metadata$Type == "Ride", ]

      SurveyData22 <- SurveyData
      CountData22 <- SurveyData22

      # Normalize metadata columns for consistent matching
      metadata[, 2] <- tolower(metadata[, 2])
      if (ncol(metadata) >= 3) metadata[, 3] <- tolower(metadata[, 3])
      if (ncol(metadata) >= 18) metadata[, 18] <- tolower(metadata[, 18])
      if (ncol(metadata) >= 19) metadata[, 19] <- tolower(metadata[, 19])

      # (A) Ride-again fix (same as your loop, vectorized per-column still ok)
      rideagain_fix <- metadata[, 3]
      rideexp_fix <- metadata[, 2]
      rideagain <- rideagain_fix[!is.na(rideagain_fix)]
      rideexp <- rideexp_fix[!is.na(rideagain_fix)]
      if (length(rideagain)) {
        for (i in seq_along(rideagain)) {
          if (rideexp[i] %in% names(SurveyData22) && rideagain[i] %in% names(SurveyData22)) {
            idx <- which(SurveyData22[, rideexp[i]] != 0 & SurveyData22[, rideagain[i]] == 0)
            if (length(idx)) SurveyData22[idx, rideagain[i]] <- 1
          }
        }
      }

      # (B) Character how-experience gating (kept from your script)
      charexp <- sub(".*_", "", na.omit(sub(".*charexp_", "", grep("charexp_", metadata[, 2], value = TRUE, fixed = TRUE))))
      howexp <- paste("charhow", charexp, sep = "_")
      howexp <- howexp[howexp != "charhow_NA"]
      ch_cols <- grep("charexp_", names(SurveyData22), value = TRUE)
      if (length(ch_cols) && length(howexp)) {
        for (i in seq_len(min(length(ch_cols), length(howexp)))) {
          if (howexp[i] %in% names(SurveyData22)) {
            SurveyData22[, ch_cols[i]] <- SurveyData22[, ch_cols[i]] * as.numeric(
              SurveyData22[, howexp[i]] < 2 | SurveyData22[, howexp[i]] == 3 | SurveyData22[, howexp[i]] == 4
            )
          }
        }
      }

      # (C) Can't-ride sentinel conversion (-1) for rides (same logic)
      rideagainx <- metadata[, 18]
      RIDEX <- metadata[which(!is.na(metadata[, 18])), 2]
      rideagainx <- tolower(rideagainx[!is.na(rideagainx)])
      RIDEX <- tolower(RIDEX[!is.na(RIDEX)])
      if (length(RIDEX)) {
        for (i in seq_along(RIDEX)) {
          if (RIDEX[i] %in% names(SurveyData22) && rideagainx[i] %in% names(SurveyData22)) {
            idx <- which(SurveyData22[, RIDEX[i]] == 0 & SurveyData22[, rideagainx[i]] == 1 & SurveyData22$ovpropex < 6)
            if (length(idx)) SurveyData22[idx, RIDEX[i]] <- -1
          }
        }
      }

      # (D) Apply weights (fast) and build CountData (fast)
      SurveyData22 <- apply_weights_fast(SurveyData22, metadata = metadata, weights22 = weights22, CantRideWeight22 = CantRideWeight22, FQ = FQ)
      CountData22 <- apply_counts_fast(CountData22, metadata = metadata, FQ = FQ)

      # (E) Summaries (fast)
      ride_bases <- unique(.base_name(tolower(metadata$Variable[metadata$Type == "Ride"])))
      ride_bases <- ride_bases[!is.na(ride_bases) & ride_bases != ""]

      Ride_Runs20 <- as_Park_LifeStage_QTR(summarize_wide_by_group(SurveyData22, ride_bases, "2"))
      Ride_Against20 <- as_Park_LifeStage_QTR(summarize_wide_by_group(SurveyData22, ride_bases, "3"))
      Ride_OriginalRuns20 <- as_Park_LifeStage_QTR(summarize_wide_by_group(CountData22, ride_bases, "2"))
      Ride_OriginalAgainst20 <- as_Park_LifeStage_QTR(summarize_wide_by_group(CountData22, ride_bases, "3"))

      Show_Runs20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(SurveyData22, metadata, type = "Show", suffix = "2"))
      Show_Against20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(SurveyData22, metadata, type = "Show", suffix = "3"))
      Show_OriginalRuns20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(CountData22, metadata, type = "Show", suffix = "2"))
      Show_OriginalAgainst20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(CountData22, metadata, type = "Show", suffix = "3"))

      Play_Runs20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(SurveyData22, metadata, type = "Play", suffix = "2"))
      Play_Against20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(SurveyData22, metadata, type = "Play", suffix = "3"))
      Play_OriginalRuns20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(CountData22, metadata, type = "Play", suffix = "2"))
      Play_OriginalAgainst20 <- as_Park_LifeStage_QTR(summarize_category_wide_by_group(CountData22, metadata, type = "Play", suffix = "3"))

      # ==================================================================================
      # Keep your downstream wRAA/EARS computation logic here (melt -> wRAA_Table -> merge EARS -> EARSx).
      # You can reuse your exact code, just swap in these objects:
      # - Ride_Runs20, Ride_Against20, Ride_OriginalRuns20, Ride_OriginalAgainst20
      # - Show_Runs20, Show_Against20, Show_OriginalRuns20, Show_OriginalAgainst20
      # - Play_Runs20, Play_Against20, Play_OriginalRuns20, Play_OriginalAgainst20
      # ==================================================================================
      stop("Paste your downstream wRAA/EARS build code here, then rbind into `EARSTotal` and increment FQ.")
    }

    # Attach run id
    if (is.null(EARSTotal)) {
      return(data.frame())
    }
    EARSTotal$sim_run <- run
    EARSTotal
  }

  Simulation_Results <- EARSTotal_list

  if (!is.null(output_dataset)) {
    try(dkuWriteDataset(output_dataset, Simulation_Results), silent = TRUE)
  }

  Simulation_Results
}
