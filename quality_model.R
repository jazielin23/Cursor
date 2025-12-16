# quality_model.R
# Simple, fake-data image "quality" model (0-10) + feature extraction.
#
# Packages:
#   install.packages(c("magick"))

# NOTE: Do not `library(magick)` at load time. Some Shiny deploy targets will
# start the app even if optional packages are missing; keeping these functions
# defined avoids confusing "could not find function" errors.

QUALITY_MODEL_PATH <- "quality_model.rds"

.feature_names <- c("mean_luma", "sd_luma", "lap_var", "edge_density")

.require_magick <- function() {
  if (!requireNamespace("magick", quietly = TRUE)) {
    stop("Package 'magick' is required. Install it with install.packages('magick').", call. = FALSE)
  }
}

.read_gray_matrix <- function(image_path, max_size = 256) {
  .require_magick()
  img <- magick::image_read(image_path)
  # Some magick versions don't export image_auto_orient(); auto-orient is optional.
  # If available (exported or internal), apply it; otherwise proceed without it.
  ns <- asNamespace("magick")
  auto_orient <- get0("image_auto_orient", envir = ns, inherits = FALSE)
  if (is.function(auto_orient)) {
    img <- auto_orient(img)
  }
  img <- magick::image_scale(img, paste0(max_size, "x", max_size, ">"))
  img <- magick::image_convert(img, colorspace = "Gray")

  # image_data returns raw [0,255]; dim: channels x width x height
  arr <- magick::image_data(img, channels = "gray")
  m <- as.integer(arr[1,,]) / 255
  list(img = img, gray = m)
}

.lap_variance <- function(gray_img) {
  # Laplacian variance as a crude sharpness measure
  # kernel:
  #  0  1  0
  #  1 -4  1
  #  0  1  0
  k <- matrix(c(0, 1, 0,
                1,-4, 1,
                0, 1, 0), nrow = 3, byrow = TRUE)
  lap <- magick::image_convolve(gray_img, kernel = k)
  d <- magick::image_data(lap, channels = "gray")
  v <- as.numeric(as.integer(d[1,,]) / 255)
  stats::var(v)
}

.edge_density_sobel <- function(gray_img, threshold = 0.10) {
  # Sobel gradient magnitude thresholded -> fraction of edge pixels
  kx <- matrix(c(-1, 0, 1,
                 -2, 0, 2,
                 -1, 0, 1), nrow = 3, byrow = TRUE)
  ky <- matrix(c(-1,-2,-1,
                  0, 0, 0,
                  1, 2, 1), nrow = 3, byrow = TRUE)

  gx <- magick::image_convolve(gray_img, kernel = kx)
  gy <- magick::image_convolve(gray_img, kernel = ky)

  dx <- as.integer(magick::image_data(gx, channels = "gray")[1,,]) / 255
  dy <- as.integer(magick::image_data(gy, channels = "gray")[1,,]) / 255

  mag <- abs(dx) + abs(dy)
  mean(mag > threshold)
}

extract_quality_features <- function(image_path) {
  x <- .read_gray_matrix(image_path)
  m <- x$gray
  img <- x$img

  mean_luma <- mean(m)
  sd_luma <- stats::sd(as.numeric(m))
  lap_var <- .lap_variance(img)
  edge_density <- .edge_density_sobel(img)

  c(
    mean_luma = as.numeric(mean_luma),
    sd_luma = as.numeric(sd_luma),
    lap_var = as.numeric(lap_var),
    edge_density = as.numeric(edge_density)
  )
}

train_fake_quality_model <- function(n = 4000, seed = 7) {
  set.seed(seed)

  # Feature ranges roughly matching extract_quality_features()
  mean_luma <- stats::runif(n, 0.05, 0.95)
  sd_luma <- stats::runif(n, 0.01, 0.40)
  lap_var <- stats::rlnorm(n, meanlog = -5.0, sdlog = 1.0)
  lap_var <- pmin(pmax(lap_var, 0), 0.25)
  edge_density <- stats::runif(n, 0.00, 0.50)

  brightness_penalty <- abs(mean_luma - 0.5)

  # Fake "ground truth"
  y <- 6 + 10 * (1.6 * lap_var + 0.8 * edge_density + 0.4 * sd_luma - 0.9 * brightness_penalty) + stats::rnorm(n, 0, 0.75)
  y <- pmin(pmax(y, 0), 10)

  df <- data.frame(
    mean_luma = mean_luma,
    sd_luma = sd_luma,
    lap_var = lap_var,
    edge_density = edge_density,
    grade = y
  )

  fit <- stats::lm(grade ~ mean_luma + sd_luma + lap_var + edge_density, data = df)

  list(
    version = "fake-lm-v1",
    feature_names = .feature_names,
    fit = fit
  )
}

load_or_train_quality_model <- function(path = QUALITY_MODEL_PATH) {
  if (file.exists(path)) {
    readRDS(path)
  } else {
    m <- train_fake_quality_model()
    saveRDS(m, path)
    m
  }
}

predict_quality_grade <- function(model, features_named) {
  # features_named: named numeric vector with .feature_names
  df <- as.data.frame(as.list(features_named[model$feature_names]))
  p <- stats::predict(model$fit, newdata = df)
  p <- as.numeric(p)
  pmin(pmax(p, 0), 10)
}
