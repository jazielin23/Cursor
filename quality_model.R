# quality_model.R
# Feature extraction + toy quality model (0–10).
#
# Packages:
#   install.packages(c("magick"))          # required
#   install.packages(c("imager"))          # optional (Canny + Harris)
#   install.packages(c("ranger"))          # optional (random forest model)
#
# Notes:
# - This is a demo model trained on fake data.
# - Feature extraction is deterministic for a given image.

QUALITY_MODEL_PATH <- "quality_model.rds"
# TensorFlow SavedModel directory suffix for the NN option
QUALITY_NN_MODEL_DIR <- "nn_tf"

.feature_names <- c(
  "mean_luma",
  "sd_luma",
  "hist_skew",
  "hist_kurtosis",
  "pct_clipped_black",
  "pct_highlights",
  "pct_clipped_white",
  "lap_var",
  "tenengrad",
  "edge_density_sobel",
  "edge_density_canny",
  "corner_density_harris",
  "centroid_dx",
  "centroid_dy",
  "lbp_entropy",
  "lbp_uniformity",
  "gabor_energy_0",
  "gabor_energy_90"
)

QUALITY_FEATURE_DESCRIPTIONS <- list(
  mean_luma = "Average brightness (0–1).",
  sd_luma = "Brightness standard deviation (contrast proxy).",
  hist_skew = "Skew of grayscale histogram (exposure balance).",
  hist_kurtosis = "Kurtosis of grayscale histogram (peakedness / clipping).",
  pct_clipped_black = "Fraction of pixels near pure black (<0.01).",
  pct_highlights = "Fraction of bright highlights (>0.95).",
  pct_clipped_white = "Fraction of pixels near pure white (>0.99).",
  lap_var = "Laplacian variance (sharpness; higher is sharper).",
  tenengrad = "Sobel energy (sharpness/edge strength).",
  edge_density_sobel = "Fraction of pixels with strong Sobel gradient.",
  edge_density_canny = "Fraction of edge pixels from Canny (if available).",
  corner_density_harris = "Corner density from Harris detector (if available).",
  centroid_dx = "Object centroid horizontal offset from center (normalized).",
  centroid_dy = "Object centroid vertical offset from center (normalized).",
  lbp_entropy = "Texture complexity via Local Binary Pattern histogram entropy.",
  lbp_uniformity = "Texture uniformity via LBP histogram (sum of squared probs).",
  gabor_energy_0 = "Directional texture energy (Gabor-like filter, 0°).",
  gabor_energy_90 = "Directional texture energy (Gabor-like filter, 90°)."
)

.require_magick <- function() {
  if (!requireNamespace("magick", quietly = TRUE)) {
    stop("Package 'magick' is required. Install it with install.packages('magick').", call. = FALSE)
  }
}

.has_imager <- function() requireNamespace("imager", quietly = TRUE)
.has_ranger <- function() requireNamespace("ranger", quietly = TRUE)
.has_keras <- function() {
  # keras in R uses reticulate; we need the *Python* tensorflow module to exist too.
  if (!requireNamespace("keras", quietly = TRUE)) return(FALSE)
  if (!requireNamespace("reticulate", quietly = TRUE)) return(FALSE)
  ok <- tryCatch(reticulate::py_module_available("tensorflow"), error = function(e) FALSE)
  isTRUE(ok)
}

.safe_is_image <- function(path) {
  ext <- tolower(tools::file_ext(path))
  ext %in% c("png", "jpg", "jpeg", "webp", "gif", "tif", "tiff", "bmp", "heic")
}

.safe_image_read <- function(image_path) {
  .require_magick()
  if (!.safe_is_image(image_path)) {
    stop("Unsupported file type. Please upload PNG/JPG/WebP/GIF.", call. = FALSE)
  }
  tryCatch(
    {
      img <- magick::image_read(image_path)
      # For multi-frame images (e.g., GIF), use the first frame only.
      if (length(img) > 1) img <- img[1]
      img
    },
    error = function(e) stop("Could not read image (unsupported or corrupt file).", call. = FALSE)
  )
}

get_image_info <- function(image_path) {
  img <- .safe_image_read(image_path)
  info <- magick::image_info(img)
  fs <- tryCatch(file.info(image_path)$size, error = function(e) NA_real_)
  info$file_size_bytes <- fs
  info
}

.read_gray <- function(image_path, max_size = 256) {
  img <- .safe_image_read(image_path)

  # Some magick versions don't export image_auto_orient(); auto-orient is optional.
  ns <- asNamespace("magick")
  auto_orient <- get0("image_auto_orient", envir = ns, inherits = FALSE)
  if (is.function(auto_orient)) img <- auto_orient(img)

  img <- magick::image_scale(img, paste0(max_size, "x", max_size, ">"))
  img <- magick::image_convert(img, colorspace = "Gray")

  arr <- magick::image_data(img, channels = "gray")
  m <- as.integer(arr[1, , , drop = TRUE]) / 255
  # Defensive: some formats can still yield an extra dimension.
  if (length(dim(m)) > 2) m <- m[, , 1, drop = TRUE]
  m <- as.matrix(m)
  list(img = img, m = m)
}

.moments <- function(x) {
  x <- as.numeric(x)
  mu <- mean(x)
  sdv <- stats::sd(x)
  if (!is.finite(sdv) || sdv == 0) return(list(mean = mu, sd = 0, skew = 0, kurtosis = 0))
  z <- (x - mu) / sdv
  list(mean = mu, sd = sdv, skew = mean(z^3), kurtosis = mean(z^4) - 3)
}

.lap_variance <- function(gray_img) {
  k <- matrix(c(0, 1, 0,
                1, -4, 1,
                0, 1, 0), nrow = 3, byrow = TRUE)
  lap <- magick::image_convolve(gray_img, kernel = k)
  d <- magick::image_data(lap, channels = "gray")
  v <- as.numeric(as.integer(d[1, , , drop = TRUE]) / 255)
  stats::var(v)
}

.sobel_fields <- function(gray_img) {
  kx <- matrix(c(-1, 0, 1,
                 -2, 0, 2,
                 -1, 0, 1), nrow = 3, byrow = TRUE)
  ky <- matrix(c(-1, -2, -1,
                  0,  0,  0,
                  1,  2,  1), nrow = 3, byrow = TRUE)
  gx <- magick::image_convolve(gray_img, kernel = kx)
  gy <- magick::image_convolve(gray_img, kernel = ky)
  dx <- as.integer(magick::image_data(gx, channels = "gray")[1, , , drop = TRUE]) / 255
  dy <- as.integer(magick::image_data(gy, channels = "gray")[1, , , drop = TRUE]) / 255
  list(dx = dx, dy = dy)
}

.tenengrad <- function(gray_img) {
  g <- .sobel_fields(gray_img)
  mean(g$dx^2 + g$dy^2)
}

.edge_density_sobel <- function(gray_img, threshold = 0.10) {
  g <- .sobel_fields(gray_img)
  mag <- abs(g$dx) + abs(g$dy)
  mean(mag > threshold)
}

.edge_density_canny <- function(image_path) {
  if (!.has_imager()) return(NA_real_)
  ns <- asNamespace("imager")
  canny <- get0("cannyEdges", envir = ns, inherits = FALSE)
  if (!is.function(canny)) return(NA_real_)
  im <- imager::load.image(image_path)
  im_g <- imager::grayscale(im)
  ed <- canny(im_g)
  mean(as.numeric(ed) > 0.5, na.rm = TRUE)
}

.corner_density_harris <- function(image_path) {
  if (!.has_imager()) return(NA_real_)
  ns <- asNamespace("imager")
  harris <- get0("harris", envir = ns, inherits = FALSE)
  if (!is.function(harris)) return(NA_real_)
  im <- imager::load.image(image_path)
  im_g <- imager::grayscale(im)
  hm <- harris(im_g)
  v <- as.numeric(hm)
  if (!length(v)) return(NA_real_)
  thr <- stats::quantile(v, probs = 0.995, na.rm = TRUE, names = FALSE)
  mean(v > thr, na.rm = TRUE)
}

.otsu_threshold <- function(x01, bins = 256) {
  x01 <- pmin(pmax(x01, 0), 1)
  h <- hist(x01, breaks = seq(0, 1, length.out = bins + 1), plot = FALSE)$counts
  p <- h / sum(h)
  omega <- cumsum(p)
  mu <- cumsum(p * seq_len(bins))
  mu_t <- mu[bins]
  sigma_b2 <- (mu_t * omega - mu)^2 / (omega * (1 - omega) + 1e-12)
  k <- which.max(sigma_b2)
  (k - 1) / (bins - 1)
}

.centroid_offset <- function(m01) {
  thr <- .otsu_threshold(as.numeric(m01))
  fg <- m01 > thr
  if (mean(fg) < 0.05) fg <- !fg
  idx <- which(fg, arr.ind = TRUE)
  if (!nrow(idx)) return(c(dx = 0, dy = 0))
  cy <- mean(idx[, 1])
  cx <- mean(idx[, 2])
  h <- nrow(m01); w <- ncol(m01)
  dx <- (cx - (w + 1) / 2) / w
  dy <- (cy - (h + 1) / 2) / h
  c(dx = as.numeric(dx), dy = as.numeric(dy))
}

.lbp_metrics <- function(m01) {
  if (nrow(m01) < 3 || ncol(m01) < 3) return(c(entropy = NA_real_, uniformity = NA_real_))
  c0 <- m01[2:(nrow(m01) - 1), 2:(ncol(m01) - 1)]
  n1 <- m01[1:(nrow(m01) - 2), 1:(ncol(m01) - 2)] >= c0
  n2 <- m01[1:(nrow(m01) - 2), 2:(ncol(m01) - 1)] >= c0
  n3 <- m01[1:(nrow(m01) - 2), 3:ncol(m01)] >= c0
  n4 <- m01[2:(nrow(m01) - 1), 3:ncol(m01)] >= c0
  n5 <- m01[3:nrow(m01), 3:ncol(m01)] >= c0
  n6 <- m01[3:nrow(m01), 2:(ncol(m01) - 1)] >= c0
  n7 <- m01[3:nrow(m01), 1:(ncol(m01) - 2)] >= c0
  n8 <- m01[2:(nrow(m01) - 1), 1:(ncol(m01) - 2)] >= c0
  code <- 1L * n1 +
    2L * n2 +
    4L * n3 +
    8L * n4 +
    16L * n5 +
    32L * n6 +
    64L * n7 +
    128L * n8
  tab <- table(as.integer(code))
  p <- as.numeric(tab) / sum(tab)
  entropy <- -sum(p * log(p + 1e-12))
  uniformity <- sum(p^2)
  c(entropy = as.numeric(entropy), uniformity = as.numeric(uniformity))
}

.gabor_energy <- function(gray_img, orientation = c("0", "90")) {
  orientation <- match.arg(orientation)
  if (orientation == "0") {
    k <- matrix(c(
      -1, -1,  0,  1,  1,
      -2, -2,  0,  2,  2,
      -3, -3,  0,  3,  3,
      -2, -2,  0,  2,  2,
      -1, -1,  0,  1,  1
    ), nrow = 5, byrow = TRUE)
  } else {
    k <- matrix(c(
      -1, -2, -3, -2, -1,
      -1, -2, -3, -2, -1,
       0,  0,  0,  0,  0,
       1,  2,  3,  2,  1,
       1,  2,  3,  2,  1
    ), nrow = 5, byrow = TRUE)
  }
  r <- magick::image_convolve(gray_img, kernel = k)
  d <- as.integer(magick::image_data(r, channels = "gray")[1, , , drop = TRUE]) / 255
  mean(abs(d))
}

extract_quality_features <- function(image_path) {
  x <- .read_gray(image_path)
  m <- x$m
  img <- x$img

  mv <- .moments(as.numeric(m))
  pct_clipped_black <- mean(m < 0.01)
  pct_highlights <- mean(m > 0.95)
  pct_clipped_white <- mean(m > 0.99)

  lap_var <- .lap_variance(img)
  tenengrad <- .tenengrad(img)
  edge_density_sobel <- .edge_density_sobel(img)
  edge_density_canny <- .edge_density_canny(image_path)
  corner_density_harris <- .corner_density_harris(image_path)

  ctr <- .centroid_offset(m)
  lbp <- .lbp_metrics(m)

  gabor_energy_0 <- .gabor_energy(img, "0")
  gabor_energy_90 <- .gabor_energy(img, "90")

  c(
    mean_luma = as.numeric(mv$mean),
    sd_luma = as.numeric(mv$sd),
    hist_skew = as.numeric(mv$skew),
    hist_kurtosis = as.numeric(mv$kurtosis),
    pct_clipped_black = as.numeric(pct_clipped_black),
    pct_highlights = as.numeric(pct_highlights),
    pct_clipped_white = as.numeric(pct_clipped_white),
    lap_var = as.numeric(lap_var),
    tenengrad = as.numeric(tenengrad),
    edge_density_sobel = as.numeric(edge_density_sobel),
    edge_density_canny = as.numeric(edge_density_canny),
    corner_density_harris = as.numeric(corner_density_harris),
    centroid_dx = as.numeric(ctr["dx"]),
    centroid_dy = as.numeric(ctr["dy"]),
    lbp_entropy = as.numeric(lbp["entropy"]),
    lbp_uniformity = as.numeric(lbp["uniformity"]),
    gabor_energy_0 = as.numeric(gabor_energy_0),
    gabor_energy_90 = as.numeric(gabor_energy_90)
  )
}

train_fake_quality_model <- function(n = 6000, seed = 7, model_type = c("lm", "rf", "nn")) {
  model_type <- match.arg(model_type)
  set.seed(seed)

  # Simulated feature ranges (loosely matching extract_quality_features())
  mean_luma <- stats::runif(n, 0.05, 0.95)
  sd_luma <- stats::runif(n, 0.01, 0.45)
  hist_skew <- stats::rnorm(n, 0, 0.6)
  hist_kurtosis <- stats::rnorm(n, 0, 1.0)
  pct_clipped_black <- pmin(pmax(stats::rnorm(n, 0.02, 0.03), 0), 0.4)
  pct_highlights <- pmin(pmax(stats::rnorm(n, 0.04, 0.05), 0), 0.5)
  pct_clipped_white <- pmin(pmax(stats::rnorm(n, 0.01, 0.02), 0), 0.3)

  lap_var <- stats::rlnorm(n, meanlog = -5.0, sdlog = 1.0)
  lap_var <- pmin(pmax(lap_var, 0), 0.35)
  tenengrad <- pmin(pmax(0.2 * lap_var + stats::rlnorm(n, -6.0, 0.8), 0), 0.25)

  edge_density_sobel <- pmin(pmax(0.6 * stats::runif(n, 0, 0.6) + 0.3 * sd_luma, 0), 0.9)
  edge_density_canny <- pmin(pmax(edge_density_sobel + stats::rnorm(n, 0, 0.05), 0), 1)
  corner_density_harris <- pmin(pmax(0.15 * edge_density_sobel + stats::rnorm(n, 0, 0.02), 0), 0.4)

  centroid_dx <- pmin(pmax(stats::rnorm(n, 0, 0.08), -0.5), 0.5)
  centroid_dy <- pmin(pmax(stats::rnorm(n, 0, 0.08), -0.5), 0.5)

  lbp_entropy <- pmin(pmax(stats::rnorm(n, 3.5, 0.6), 0), 6)
  lbp_uniformity <- pmin(pmax(stats::rnorm(n, 0.02, 0.01), 0.001), 0.2)

  gabor_energy_0 <- pmin(pmax(stats::rlnorm(n, -5.8, 0.7), 0), 0.3)
  gabor_energy_90 <- pmin(pmax(stats::rlnorm(n, -5.8, 0.7), 0), 0.3)

  brightness_penalty <- abs(mean_luma - 0.5)
  centering_penalty <- sqrt(centroid_dx^2 + centroid_dy^2)

  y <- 6 + 10 * (
    1.3 * lap_var +
      1.0 * tenengrad +
      0.6 * edge_density_sobel +
      0.3 * sd_luma -
      0.8 * brightness_penalty -
      0.6 * centering_penalty -
      0.4 * pct_clipped_black -
      0.5 * pct_clipped_white +
      0.2 * (gabor_energy_0 + gabor_energy_90) +
      0.15 * lbp_entropy
  ) + stats::rnorm(n, 0, 0.8)
  y <- pmin(pmax(y, 0), 10)

  df <- data.frame(
    mean_luma = mean_luma,
    sd_luma = sd_luma,
    hist_skew = hist_skew,
    hist_kurtosis = hist_kurtosis,
    pct_clipped_black = pct_clipped_black,
    pct_highlights = pct_highlights,
    pct_clipped_white = pct_clipped_white,
    lap_var = lap_var,
    tenengrad = tenengrad,
    edge_density_sobel = edge_density_sobel,
    edge_density_canny = edge_density_canny,
    corner_density_harris = corner_density_harris,
    centroid_dx = centroid_dx,
    centroid_dy = centroid_dy,
    lbp_entropy = lbp_entropy,
    lbp_uniformity = lbp_uniformity,
    gabor_energy_0 = gabor_energy_0,
    gabor_energy_90 = gabor_energy_90,
    grade = y
  )

  if (model_type == "rf" && !.has_ranger()) {
    model_type <- "lm"
  }
  if (model_type == "nn" && !.has_keras()) {
    model_type <- "lm"
  }

  if (model_type == "rf") {
    fit <- ranger::ranger(
      grade ~ .,
      data = df,
      num.trees = 400,
      mtry = max(2, floor(sqrt(ncol(df) - 1))),
      importance = "impurity",
      seed = seed
    )
    version <- "fake-rf-v1"
    extra <- list()
  } else if (model_type == "nn") {
    # Simple dense network on engineered features (demo).
    X <- as.matrix(df[, setdiff(names(df), "grade"), drop = FALSE])
    yv <- as.numeric(df$grade)

    x_mean <- apply(X, 2, mean)
    x_sd <- apply(X, 2, stats::sd)
    x_sd[x_sd == 0] <- 1
    Xs <- scale(X, center = x_mean, scale = x_sd)

    # Reproducibility (best-effort) - avoid hard dependency on R tensorflow package
    if (requireNamespace("keras", quietly = TRUE)) {
      try(keras::set_random_seed(as.integer(seed)), silent = TRUE)
    }

    fit <- keras::keras_model_sequential() |>
      keras::layer_dense(units = 64, activation = "relu", input_shape = ncol(Xs)) |>
      keras::layer_dropout(rate = 0.15) |>
      keras::layer_dense(units = 32, activation = "relu") |>
      keras::layer_dense(units = 1, activation = "linear")

    fit |>
      keras::compile(
        optimizer = keras::optimizer_adam(learning_rate = 0.01),
        loss = "mse",
        metrics = list("mae")
      )

    fit |>
      keras::fit(
        x = Xs,
        y = yv,
        epochs = 25,
        batch_size = 64,
        validation_split = 0.2,
        verbose = 0
      )

    version <- "fake-nn-v1"
    extra <- list(x_mean = x_mean, x_sd = x_sd)
  } else {
    fit <- stats::lm(grade ~ ., data = df)
    version <- "fake-lm-v2"
    extra <- list()
  }

  c(list(
    version = version,
    feature_names = .feature_names,
    model_type = model_type,
    fit = fit
  ), extra)
}

load_or_train_quality_model <- function(path = QUALITY_MODEL_PATH, model_type = c("lm", "rf", "nn")) {
  model_type <- match.arg(model_type)
  base <- sub("\\.rds$", "", path)
  path2 <- paste0(base, "_", model_type, ".rds")
  expected <- .feature_names

  .is_valid_model <- function(m) {
    is.list(m) && !is.null(m$feature_names) && is.character(m$feature_names)
  }

  .needs_retrain <- function(m) {
    # Retrain if the feature set changed (e.g., older models had `edge_density`)
    if (!.is_valid_model(m)) return(TRUE)
    # For lm/rf, a missing fit means the artifact is unusable.
    if (model_type %in% c("lm", "rf") && is.null(m$fit)) return(TRUE)
    # For nn, we can reload the fit from disk; scaler must be present.
    if (model_type == "nn" && (is.null(m$x_mean) || is.null(m$x_sd))) return(TRUE)
    !identical(as.character(m$feature_names), expected)
  }

  # Neural net is stored as: meta RDS + TensorFlow SavedModel directory.
  if (model_type == "nn") {
    if (!.has_keras()) {
      # If keras isn't available, fall back to lm.
      return(load_or_train_quality_model(path = path, model_type = "lm"))
    }
    model_dir <- paste0(base, "_", QUALITY_NN_MODEL_DIR)
    meta_path <- path2

    if (file.exists(meta_path) && dir.exists(model_dir)) {
      m <- readRDS(meta_path)
      if (.needs_retrain(m)) {
        m <- train_fake_quality_model(model_type = "nn")
      } else {
        # Reload keras model from disk
        m$fit <- keras::load_model_tf(model_dir)
      }
      return(m)
    }

    # Train and try to persist
    m <- train_fake_quality_model(model_type = "nn")
    # Save TF model + metadata (best-effort; if save fails, still return trained model)
    try({
      if (dir.exists(model_dir)) unlink(model_dir, recursive = TRUE, force = TRUE)
      keras::save_model_tf(m$fit, model_dir)
      m2 <- m
      m2$fit <- NULL
      saveRDS(m2, meta_path)
    }, silent = TRUE)
    return(m)
  }

  if (file.exists(path2)) {
    m <- readRDS(path2)
    if (.needs_retrain(m)) {
      m <- train_fake_quality_model(model_type = model_type)
      saveRDS(m, path2)
    }
    return(m)
  }

  # Back-compat (older artifact), but migrate if it doesn't match current features.
  if (file.exists(path)) {
    m_old <- readRDS(path)
    if (!.needs_retrain(m_old)) return(m_old)
  }

  m <- train_fake_quality_model(model_type = model_type)
  saveRDS(m, path2)
  m
}

predict_quality_grade <- function(model, features_named) {
  # Build a 1-row data.frame with scalar numerics.
  # This avoids `predict.lm` errors like "variable lengths differ".
  fns <- as.character(model$feature_names)
  row <- lapply(fns, function(nm) {
    v <- features_named[[nm]]
    if (is.null(v) || length(v) == 0 || is.na(v[1])) return(0)
    as.numeric(v[1])
  })
  names(row) <- fns
  df <- as.data.frame(row, check.names = FALSE, stringsAsFactors = FALSE)

  if (!is.null(model$model_type) && model$model_type == "nn") {
    if (!.has_keras()) return(NA_real_)
    x_mean <- model$x_mean
    x_sd <- model$x_sd
    if (is.null(x_mean) || is.null(x_sd)) {
      # Missing scaler -> best-effort no-scale
      Xs <- as.matrix(df)
    } else {
      X <- as.matrix(df)
      Xs <- scale(X, center = x_mean, scale = x_sd)
    }
    p <- as.numeric(keras::predict(model$fit, Xs, verbose = 0))
  } else if (!is.null(model$model_type) && model$model_type == "rf") {
    p <- stats::predict(model$fit, data = df)$predictions
  } else {
    p <- stats::predict(model$fit, newdata = df)
  }
  p <- as.numeric(p)
  pmin(pmax(p, 0), 10)
}

feature_importance <- function(model) {
  if (!is.null(model$model_type) && model$model_type == "rf") {
    imp <- model$fit$variable.importance
    out <- data.frame(feature = names(imp), importance = as.numeric(imp), row.names = NULL)
    out <- out[order(out$importance, decreasing = TRUE), ]
    out
  } else {
    co <- stats::coef(model$fit)
    co <- co[names(co) != "(Intercept)"]
    out <- data.frame(feature = names(co), importance = abs(as.numeric(co)), row.names = NULL)
    out <- out[order(out$importance, decreasing = TRUE), ]
    out
  }
}
