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
