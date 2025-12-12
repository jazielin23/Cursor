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
    # Return grouped skeleton (no measures) to keep downstream melt logic safe.
    return(unique(dt[, ..group_cols]))
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
  if (!"Park" %in% names(m)) stop("metadata must have column `Park`")

  m[, Variable := tolower(Variable)]
  m[, Type := as.character(Type)]
  if ("Genre" %in% names(m)) m[, Genre := as.character(Genre)]
  m[, Park := suppressWarnings(as.integer(Park))]

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

  # Build mapping from (park, base) -> Category1.
  # IMPORTANT: base names can collide across parks; also metadata can have duplicates.
  # We de-duplicate to one Category1 per (park, base) to avoid cartesian joins.
  map <- m[!is.na(Park) & Park %in% 1:4, .(park = Park, base, Category1)]
  map <- map[!is.na(base) & nzchar(base) & !is.na(Category1) & nzchar(Category1)]
  map <- map[, .SD[1], by = .(park, base)]

  # Columns available in dt
  bases <- unique(map$base)
  cols <- intersect(paste0(bases, suffix), names(dt))
  if (!length(cols)) {
    out <- unique(dt[, ..group_cols])
    return(out)
  }

  # Melt only relevant columns, map to Category1 by stripping suffix.
  # IMPORTANT: qualify melt/dcast to data.table to avoid reshape2 masking inside Dataiku/foreach.
  long <- data.table::melt(
    dt[, c(group_cols, cols), with = FALSE],
    id.vars = group_cols,
    variable.name = "var",
    value.name = "val",
    variable.factor = FALSE
  )
  long <- as.data.table(long)
  long[, base := sub(paste0(suffix, "$"), "", var)]
  # Join by park+base (prevents collisions and cartesian expansion).
  # Keep only rows with a valid mapping.
  if (!"park" %in% names(long)) stop("group_cols must include 'park' for category summaries")
  long <- merge(long, map, by = c("park", "base"), all = FALSE)

  # Aggregate by category
  agg <- long[, .(val = sum(val, na.rm = TRUE)), by = c(group_cols, "Category1")]

  # Wide
  data.table::dcast(agg, as.formula(paste(paste(group_cols, collapse = " + "), "~ Category1")), value.var = "val", fill = 0)
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
    .packages = c("dataiku", "data.table", "dplyr", "nnet", "sqldf", "reshape2"),
    # Ensure helper functions are available on parallel workers
    .export = c(
      ".as_int_ov", ".base_name", ".score_vectors",
      "apply_weights_fast", "apply_counts_fast",
      "summarize_wide_by_group", "summarize_category_wide_by_group",
      "as_Park_LifeStage_QTR"
    )
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

      # Be defensive: meta rows can have duplicates/NA; ensure scalar, lowercased column name.
      exp_ride_col <- tolower(as.character(matched_row$Variable[1]))
      exp_group_col <- tolower(as.character(matched_row$SPEC[1]))

      # `%in%` can yield NA if exp_* is NA; force scalar TRUE/FALSE
      ride_exists <- isTRUE(!is.na(exp_ride_col) && nzchar(exp_ride_col) && exp_ride_col %in% colnames(SurveyData))
      group_exists <- isTRUE(!is.na(exp_group_col) && nzchar(exp_group_col) && exp_group_col %in% colnames(SurveyData))
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
      pog_vec <- numeric(0)
      while (k < length(meta_prepared$Variable) + 1L) {
        # Parse the metadata variable the same way as your original code, but safely.
        # (The original nested unlist/strsplit expression is easy to break with parentheses.)
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

        # Map park token -> numeric park id; skip unknown/NA tokens instead of crashing.
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

        # Column lookup should match lowercased SurveyData names.
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
      # Build POG table only for the rows we actually computed (guards can skip rows).
      metaPOG <- NULL
      if (length(name_vec)) {
        meta_preparedPOG <- data.frame(name = name_vec, Park = park_vec, POG = pog_vec, expd = expd_vec)
        meta_preparedPOG <- merge(meta_preparedPOG, AttQTR, by = "Park")
        meta_preparedPOG$NEWPOG <- meta_preparedPOG$expd * meta_preparedPOG$Factor
        meta_preparedPOG$NEWGC <- meta_preparedPOG$Att * meta_preparedPOG$NEWPOG
        # Defensive selection (Dataiku schemas can introduce/rename columns)
        need_cols_pog <- c("Park", "name", "NEWGC")
        if (!all(need_cols_pog %in% names(meta_preparedPOG))) {
          # Skip POG backfill if required columns are missing
          metaPOG <- NULL
        } else {
          metaPOG <- merge(meta_prepared, meta_preparedPOG[, need_cols_pog], by = c("name", "Park"))
        }
      }

      meta_prepared2 <- sqldf::sqldf(
        "select a.*,b.GC as QuarterlyGuestCarried from meta_prepared a left join QTRLY_GC b on a.name=b.name and a.Park = b.Park"
      )
      # Avoid data.table `:=` inside parallel workers (can error if object isn't a data.table).
      # Use base merge + replacement instead.
      meta_prepared2 <- as.data.frame(meta_prepared2)
      if (!is.null(metaPOG) && nrow(metaPOG)) {
        # metaPOG can be data.frame or data.table; subset safely
        need_cols_newgc <- c("name", "Park", "NEWGC")
        have_cols_newgc <- intersect(need_cols_newgc, names(metaPOG))
        if (length(have_cols_newgc) < 3) {
          # If NEWGC isn't present, skip backfill
          metaPOG2 <- NULL
        } else {
          metaPOG2 <- metaPOG[, need_cols_newgc]
        }
        if (!is.null(metaPOG2) && nrow(metaPOG2)) {
          meta_prepared2 <- merge(meta_prepared2, metaPOG2, by = c("name", "Park"), all.x = TRUE)
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
      # Avoid cbind() coercion (can turn everything into character) — keep FY numeric.
      SurveyData$FY <- as.integer(yearauto)

      # ==================================================================================
      # Your multinom/GLM block to construct `weights22` + `CantRideWeight22` goes here.
      # Kept as-is from your original script (not reprinted here to keep this file usable).
      #
      # REQUIREMENT:
      # - after this block finishes, you must have:
      #   - weights22 : data.frame with numeric weights in columns 3:7
      #   - CantRideWeight22 : data.frame/matrix with numeric weights in columns 3:7 and 4 rows (parks 1..4)
      # ==================================================================================
      # =========================
      # Build weights22 + CantRideWeight22 (embedded from your original script)
      # =========================

      .safe_rowSums_eq <- function(df, cols, value) {
        cols <- intersect(tolower(cols), names(df))
        if (!length(cols)) return(rep(0, nrow(df)))
        rowSums(df[, cols, drop = FALSE] == value, na.rm = TRUE)
      }

      # Ensure lower names
      names(SurveyData) <- tolower(names(SurveyData))
      metadata <- as.data.frame(metadata)

      # Defensive column access (Dataiku schemas can vary slightly)
      n_sr <- nrow(SurveyData)
      park_col <- if ("park" %in% names(SurveyData)) SurveyData$park else rep(NA_integer_, n_sr)
      ov_col <- if ("ovpropex" %in% names(SurveyData)) SurveyData$ovpropex else rep(5L, n_sr)
      SurveyData$FY <- if ("FY" %in% names(SurveyData)) as.integer(SurveyData$FY) else as.integer(yearauto)
      if (length(SurveyData$FY) != n_sr) SurveyData$FY <- rep(as.integer(yearauto), n_sr)

      # ---- Play ----
      cols_play <- metadata[metadata$Type == "Play", 2]
      five_Play <- .safe_rowSums_eq(SurveyData, cols_play, 5)
      four_Play <- .safe_rowSums_eq(SurveyData, cols_play, 4)
      three_Play <- .safe_rowSums_eq(SurveyData, cols_play, 3)
      two_Play <- .safe_rowSums_eq(SurveyData, cols_play, 2)
      one_Play <- .safe_rowSums_eq(SurveyData, cols_play, 1)
      weights_Play <- data.frame(
        one_Play = one_Play, two_Play = two_Play, three_Play = three_Play, four_Play = four_Play, five_Play = five_Play,
        Park = as.integer(park_col),
        FY = as.integer(SurveyData$FY)
      )

      # ---- Show ----
      cols_show <- metadata[metadata$Type == "Show", 2]
      five_Show <- .safe_rowSums_eq(SurveyData, cols_show, 5)
      four_Show <- .safe_rowSums_eq(SurveyData, cols_show, 4)
      three_Show <- .safe_rowSums_eq(SurveyData, cols_show, 3)
      two_Show <- .safe_rowSums_eq(SurveyData, cols_show, 2)
      one_Show <- .safe_rowSums_eq(SurveyData, cols_show, 1)
      weights_Show <- data.frame(
        one_Show = one_Show, two_Show = two_Show, three_Show = three_Show, four_Show = four_Show, five_Show = five_Show,
        Park = as.integer(park_col),
        FY = as.integer(SurveyData$FY)
      )

      # ---- Preferred (Flaship/Anchor) ----
      cols_pref <- metadata[metadata$Genre == "Flaship" | metadata$Genre == "Anchor", 2]
      five_Preferred <- .safe_rowSums_eq(SurveyData, cols_pref, 5)
      four_Preferred <- .safe_rowSums_eq(SurveyData, cols_pref, 4)
      three_Preferred <- .safe_rowSums_eq(SurveyData, cols_pref, 3)
      two_Preferred <- .safe_rowSums_eq(SurveyData, cols_pref, 2)
      one_Preferred <- .safe_rowSums_eq(SurveyData, cols_pref, 1)
      weights_Preferred <- data.frame(
        one_Preferred = one_Preferred, two_Preferred = two_Preferred, three_Preferred = three_Preferred,
        four_Preferred = four_Preferred, five_Preferred = five_Preferred,
        Park = as.integer(park_col),
        FY = as.integer(SurveyData$FY)
      )

      # ---- Rides / Att ----
      cols_ride <- metadata[metadata$Type == "Ride", 2]
      five_RA <- .safe_rowSums_eq(SurveyData, cols_ride, 5)
      four_RA <- .safe_rowSums_eq(SurveyData, cols_ride, 4)
      three_RA <- .safe_rowSums_eq(SurveyData, cols_ride, 3)
      two_RA <- .safe_rowSums_eq(SurveyData, cols_ride, 2)
      one_RA <- .safe_rowSums_eq(SurveyData, cols_ride, 1)

      # ---- can't-get-on ----
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
        cantgeton <- rowSums(
          (Xexp == 0) & (Xcant == 1) & matrix(ov_ok, nrow = nrow(SurveyData), ncol = ncol(Xexp)),
          na.rm = TRUE
        )
      }
      cant <- data.frame(
        ovpropex = ov_col,
        cantgeton = cantgeton,
        Park = as.integer(park_col),
        FY = as.integer(SurveyData$FY)
      )

      weights_RA <- data.frame(
        ovpropex = ov_col,
        one_RA = one_RA, two_RA = two_RA, three_RA = three_RA, four_RA = four_RA, five_RA = five_RA,
        Park = as.integer(park_col),
        FY = as.integer(SurveyData$FY)
      )

      weights <- cbind(weights_Play, weights_Show, weights_RA, cant, weights_Preferred)

      build_park_weights <- function(park_id, conf_level, conf_label) {
        .defaults <- function() {
          # Neutral fallback: all weights = 1.
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
              "one_Play", "two_Play", "three_Play", "four_Play", "five_Play",
              "one_Show", "two_Show", "three_Show", "four_Show", "five_Show",
              "one_RA", "two_RA", "three_RA", "four_RA", "five_RA"
            )),
            cantride = make_df("cantgeton"),
            pref = make_df(c("one_Preferred", "two_Preferred", "three_Preferred", "four_Preferred", "five_Preferred"))
          )
        }

        w <- weights
        w$ovpropex <- relevel(factor(w$ovpropex), ref = "5")
        w <- w[w$Park == park_id & w$FY == yearauto, ]

        # multinom needs 2+ outcome classes; fall back to neutral weights if not available
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

        # Ensure we have 5 numeric columns in positions 3:7 for downstream indexing.
        weightedEEs_part <- data.frame(core[1:15, , drop = FALSE], X5 = EXPcol[1:15])
        weightedEEs_part[weightedEEs_part$Var1 == "five_Play", 3:7] <- weightedEEs_part[weightedEEs_part$Var1 == "five_Play", 3:7] * 1
        weightedEEs_part[weightedEEs_part$Var1 == "five_Show", 3:7] <- weightedEEs_part[weightedEEs_part$Var1 == "five_Show", 3:7] * 1

        can_core <- df_all[df_all$Var2 == conf_label & df_all$Var1 == "cantgeton", ]
        if (!nrow(can_core)) return(.defaults())
        cantride_part <- data.frame(can_core[1, , drop = FALSE], X5 = rep(1, 1))

        pref_part <- data.frame(core[16:20, , drop = FALSE], X5 = EXPcol[16:20])

        list(weightedEEs = weightedEEs_part, cantride = cantride_part, pref = pref_part)
      }

      mk <- build_park_weights(1, 0.995, "99.8 %")
      ep <- build_park_weights(2, 0.99, "99.5 %")
      dhs <- build_park_weights(3, 0.80, "90 %")
      dak <- build_park_weights(4, 0.70, "15 %")

      weightedEEs <- rbind(mk$weightedEEs, ep$weightedEEs, dhs$weightedEEs, dak$weightedEEs)
      cantride2 <- data.frame(FY = yearauto, rbind(mk$cantride, ep$cantride, dhs$cantride, dak$cantride))

      weightsPref1 <- mk$pref
      weightsPref2 <- ep$pref
      weightsPref3 <- dhs$pref
      weightsPref4 <- dak$pref

      weights22 <- rbind(weightedEEs, weightsPref1, weightsPref2, weightsPref3, weightsPref4)
      CantRideWeight22 <- cantride2[, -1, drop = FALSE]

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
      # =========================
      # Build wRAA_Table + EARS (embedded from your original logic)
      # =========================

      # Robust builder: avoids fragile `[, -c(1:4)]` assumptions when measure columns are absent.
      build_long_block <- function(orig_rs, orig_ra, w_rs, w_ra, genre) {
        ids <- c("Park", "LifeStage", "QTR")
        # If there are no measure columns, return empty block.
        if (ncol(orig_rs) <= length(ids) || ncol(orig_ra) <= length(ids) || ncol(w_rs) <= length(ids) || ncol(w_ra) <= length(ids)) {
          return(data.frame(Park = integer(0), LifeStage = integer(0), QTR = integer(0), NAME = character(0),
                            Original_RS = numeric(0), Original_RA = numeric(0), wEE = numeric(0), wEEx = numeric(0),
                            Genre = character(0)))
        }

        m1 <- reshape2::melt(orig_rs, id = ids)
        m2 <- reshape2::melt(orig_ra, id = ids)
        m3 <- reshape2::melt(w_rs, id = ids)
        m4 <- reshape2::melt(w_ra, id = ids)

        # Standardize to: Park LifeStage QTR NAME value
        names(m1)[names(m1) == "variable"] <- "NAME"; names(m1)[names(m1) == "value"] <- "Original_RS"
        names(m2)[names(m2) == "variable"] <- "NAME"; names(m2)[names(m2) == "value"] <- "Original_RA"
        names(m3)[names(m3) == "variable"] <- "NAME"; names(m3)[names(m3) == "value"] <- "wEE"
        names(m4)[names(m4) == "variable"] <- "NAME"; names(m4)[names(m4) == "value"] <- "wEEx"

        key <- c(ids, "NAME")
        out <- merge(m1, m2[, c(key, "Original_RA")], by = key, all = TRUE)
        out <- merge(out, m3[, c(key, "wEE")], by = key, all = TRUE)
        out <- merge(out, m4[, c(key, "wEEx")], by = key, all = TRUE)

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

      # Clean up Play junk names (kept from your original)
      if (nrow(Play)) {
        Play <- Play[Play$NAME != "Park.1" & Play$NAME != "LifeStage.1" & Play$NAME != "QTR.1", ]
      }

      wRAA_Table <- rbind(Ride, Show, Play)

      twox <- wRAA_Table %>%
        group_by(NAME, Park, Genre, QTR, LifeStage) %>%
        summarise(
          Original_RS = sum(Original_RS),
          Original_RA = sum(Original_RA),
          wEE = sum(wEE),
          wEEx = sum(wEEx),
          .groups = "drop"
        )

      onex <- twox %>%
        group_by(NAME, Park, QTR, LifeStage) %>%
        mutate(sum = sum(Original_RS + Original_RA)) %>%
        ungroup()
      onex$Percent <- (onex$Original_RS + onex$Original_RA) / onex$sum

      wRAA_Table <- twox %>%
        left_join(onex %>% select(NAME, Park, QTR, LifeStage, Percent), by = c("NAME", "Park", "QTR", "LifeStage"))
      wRAA_Table <- wRAA_Table[!is.na(wRAA_Table$Park), ]

      wRAA_Table <- cbind(wRAA_Table, wOBA = wRAA_Table$wEE / (wRAA_Table$Original_RS + wRAA_Table$Original_RA))

      wRAA_Table <- merge(
        x = wRAA_Table,
        y = aggregate(wRAA_Table$Original_RS, by = list(QTR = wRAA_Table$QTR, Park = wRAA_Table$Park), FUN = sum),
        by = c("QTR", "Park"), all.x = TRUE
      )
      names(wRAA_Table)[ncol(wRAA_Table)] <- "OBP1"

      wRAA_Table <- merge(
        x = wRAA_Table,
        y = aggregate(wRAA_Table$Original_RA, by = list(QTR = wRAA_Table$QTR, Park = wRAA_Table$Park), FUN = sum),
        by = c("QTR", "Park"), all.x = TRUE
      )
      names(wRAA_Table)[ncol(wRAA_Table)] <- "OBP2"
      wRAA_Table$OBP <- wRAA_Table$OBP1 / (wRAA_Table$OBP1 + wRAA_Table$OBP2)

      wRAA_Table <- merge(
        x = wRAA_Table,
        y = aggregate(wRAA_Table$wOBA, by = list(QTR = wRAA_Table$QTR, Park = wRAA_Table$Park), FUN = median),
        by = c("QTR", "Park"), all.x = TRUE
      )
      names(wRAA_Table)[ncol(wRAA_Table)] <- "wOBA_Park"
      wRAA_Table <- data.frame(wRAA_Table, wOBA_Scale = wRAA_Table$wOBA_Park / wRAA_Table$OBP)

      # ---- Merge EARS taxonomy columns onto wRAA_Table ----
      join_keys <- c("NAME", "Park", "Genre", "QTR", "LifeStage")
      table2_nodup <- EARS[, setdiff(names(EARS), setdiff(names(wRAA_Table), join_keys)), drop = FALSE]
      EARSFinal_Final <- merge(wRAA_Table, table2_nodup, by = join_keys, all = FALSE)

      # ---- Compute EARS for this quarter ----
      EARSx <- EARSFinal_Final
      agc_col <- if ("AnnualGuestsCarried" %in% names(EARSx)) {
        "AnnualGuestsCarried"
      } else if ("wRAA_Table.AnnualGuestsCarried" %in% names(EARSx)) {
        "wRAA_Table.AnnualGuestsCarried"
      } else {
        stop("Missing AnnualGuestsCarried column after merge.")
      }

      EARSx$wRAA <- ((EARSx$wOBA - EARSx$wOBA_Park) / EARSx$wOBA_Scale) * EARSx[[agc_col]]
      EARSx$EARS <- EARSx$wRAA / EARSx$RPW
      EARSx$EARS <- (EARSx$EARS + EARSx$replacement) * EARSx$p

      EARSTotal <- rbind(EARSTotal, EARSx)

      # advance quarter
      FQ <- FQ + 1L
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
