# app.R
# R Shiny demo: grade an item's "quality" from an image (0-10).
#
# Install:
#   install.packages(c("shiny", "bslib", "magick", "ranger", "imager"))
# Run:
#   shiny::runApp(".")

suppressPackageStartupMessages({
  library(shiny)
})
if (!requireNamespace("bslib", quietly = TRUE)) {
  stop("Package 'bslib' is required. Install it with install.packages('bslib').", call. = FALSE)
}

# Source helper functions into the *app environment* (important for some deploy modes).
# Also make it robust if the working directory isn't the app directory.
.quality_path <- "quality_model.R"
if (!file.exists(.quality_path)) {
  .ofile <- tryCatch(sys.frame(1)$ofile, error = function(e) NULL)
  if (!is.null(.ofile) && nzchar(.ofile)) {
    .quality_path <- file.path(dirname(.ofile), "quality_model.R")
  }
}
source(.quality_path, local = TRUE)

`%||%` <- function(a, b) if (!is.null(a)) a else b
.nn_available <- tryCatch(.has_keras(), error = function(e) FALSE)

.feature_help_ui <- function() {
  # Uses descriptions from quality_model.R
  items <- lapply(names(QUALITY_FEATURE_DESCRIPTIONS), function(nm) {
    tags$li(tags$strong(nm), ": ", QUALITY_FEATURE_DESCRIPTIONS[[nm]])
  })
  tags$details(
    tags$summary(class = "text-muted", "Feature definitions"),
    tags$ul(items)
  )
}

ui <- bslib::page_fillable(
  theme = bslib::bs_theme(
    version = 5,
    bootswatch = "flatly"
  ),
  bslib::page_navbar(
    title = "Image Quality Grader",
    bg = "#0B1320",
    inverse = TRUE,
    fillable = TRUE,
    bslib::nav_panel(
      "Grader",
      bslib::layout_sidebar(
        fillable = TRUE,
        sidebar = bslib::sidebar(
          width = 360,
          open = "always",
          title = "Upload & Score",
          fileInput(
            "image",
            "Choose an image",
            accept = c("image/png", "image/jpeg", "image/webp", "image/gif")
          ),
          selectInput(
            "model_type",
            "Model",
            choices = {
              ch <- c(
                "Linear regression (fake training data)" = "lm",
                "Random forest (fake training data; needs ranger)" = "rf"
              )
              if (.nn_available) {
                ch <- c(ch, "Neural net (fake training data; keras + tensorflow)" = "nn")
              }
              ch
            },
            selected = "rf"
          ),
          actionButton("predict", "Predict grade", class = "btn-primary"),
          downloadButton("download_result", "Download result (CSV)", class = "btn-outline-secondary"),
          tags$div(style = "height: 10px"),
          uiOutput("status"),
          tags$hr(),
          tags$div(
            class = "text-muted small",
            "This is a demo model trained on fake data. ",
            "The score is an example (0 = worst, 10 = best)."
          ),
          tags$div(style = "height: 10px"),
          .feature_help_ui()
        ),
        bslib::layout_columns(
          col_widths = c(6, 6),
          bslib::card(
            full_screen = TRUE,
            bslib::card_header("Preview"),
            bslib::card_body(
              imageOutput("preview", height = "320px")
            )
          ),
          bslib::card(
            full_screen = TRUE,
            bslib::card_header("Result"),
            bslib::card_body(
              bslib::value_box(
                title = "Predicted quality (0–10)",
                value = textOutput("grade", inline = TRUE),
                theme = "primary"
              ),
              tags$div(style = "height: 12px"),
              bslib::card(
                bslib::card_header("Extracted features"),
                bslib::card_body(
                  tableOutput("features")
                )
              )
            )
          )
        )
      )
    ),
    bslib::nav_panel(
      "Batch",
      bslib::layout_sidebar(
        fillable = TRUE,
        sidebar = bslib::sidebar(
          width = 360,
          open = "always",
          title = "Batch processing",
          fileInput("zipfile", "Upload a .zip of images", accept = c(".zip")),
          actionButton("run_batch", "Process ZIP", class = "btn-primary"),
          downloadButton("download_batch", "Download results (CSV)", class = "btn-outline-secondary"),
          tags$hr(),
          tags$div(class = "text-muted small", "Uploads a ZIP, scores each image, and returns a CSV.")
        ),
        bslib::card(
          full_screen = TRUE,
          bslib::card_header("Batch results"),
          bslib::card_body(
            tableOutput("batch_table")
          )
        )
      )
    )
  )
)

server <- function(input, output, session) {
  cache <- reactiveValues(pred = new.env(parent = emptyenv()))

  observeEvent(input$model_type, {
    # If the browser is holding onto a stale "nn" selection, reset it.
    if (identical(input$model_type, "nn") && !tryCatch(.has_keras(), error = function(e) FALSE)) {
      updateSelectInput(session, "model_type", selected = "lm")
    }
  }, ignoreInit = TRUE)

  current_model <- reactive({
    # If rf requested but ranger missing, fall back to lm with a friendly message
    if (input$model_type == "rf" && !requireNamespace("ranger", quietly = TRUE)) {
      load_or_train_quality_model(model_type = "lm")
    } else if (input$model_type == "nn" && !.has_keras()) {
      load_or_train_quality_model(model_type = "lm")
    } else {
      load_or_train_quality_model(model_type = input$model_type)
    }
  })

  output$status <- renderUI({
    if (is.null(input$image)) {
      div(class = "alert alert-secondary mb-0", role = "alert", "Waiting for an upload…")
    } else {
      msg <- paste0("Selected: ", input$image$name)
      if (input$model_type == "rf" && !requireNamespace("ranger", quietly = TRUE)) {
        msg <- paste0(msg, " (Note: 'ranger' not installed; using linear model.)")
      }
      div(class = "alert alert-info mb-0", role = "alert", msg)
    }
  })

  output$preview <- renderImage({
    req(input$image)
    list(src = input$image$datapath, contentType = input$image$type)
  }, deleteFile = FALSE)

  prediction <- eventReactive(input$predict, {
    req(input$image)
    validate(
      need(requireNamespace("magick", quietly = TRUE), "Package 'magick' is required. Install it with install.packages('magick').")
    )
    mdl <- current_model()
    fp <- input$image$datapath
    key <- paste0(tools::md5sum(fp)[[1]], "::", mdl$version)
    if (exists(key, envir = cache$pred, inherits = FALSE)) {
      return(get(key, envir = cache$pred, inherits = FALSE))
    }

    res <- withProgress(message = "Scoring image…", value = 0, {
      incProgress(0.2, detail = "Extracting features")
      feats <- tryCatch(extract_quality_features(fp), error = function(e) e)
      if (inherits(feats, "error")) {
        stop(paste0("Feature extraction failed: ", conditionMessage(feats)), call. = FALSE)
      }
      incProgress(0.7, detail = "Predicting grade")
      grade <- tryCatch(predict_quality_grade(mdl, feats), error = function(e) NA_real_)
      incProgress(1, detail = "Done")
      list(filename = input$image$name, datapath = fp, grade = as.numeric(grade), feats = feats, error = NA_character_)
    })

    assign(key, res, envir = cache$pred)
    res
  }, ignoreInit = TRUE)

  output$grade <- renderText({
    if (is.null(prediction())) {
      "—"
    } else {
      if (is.na(prediction()$grade)) "—" else sprintf("%.1f", prediction()$grade)
    }
  })

  output$features <- renderTable({
    req(prediction())
    if (is.null(prediction()$feats)) {
      return(data.frame(message = prediction()$error %||% "No features available.", row.names = NULL))
    }
    f <- prediction()$feats
    desc <- QUALITY_FEATURE_DESCRIPTIONS
    feature_html <- vapply(names(f), function(nm) {
      ttl <- desc[[nm]] %||% ""
      paste0("<span title='", htmltools::htmlEscape(ttl), "'>", htmltools::htmlEscape(nm), "</span>")
    }, character(1))
    data.frame(
      feature = feature_html,
      value = as.numeric(f),
      row.names = NULL
    )
  }, striped = TRUE, hover = TRUE, digits = 4, sanitize.text.function = function(x) x)

  output$download_result <- downloadHandler(
    filename = function() paste0("image_quality_result_", Sys.Date(), ".csv"),
    content = function(file) {
      req(prediction())
      r <- prediction()
      if (is.null(r$feats)) {
        out <- data.frame(filename = r$filename, grade = as.numeric(r$grade), error = r$error %||% NA_character_, row.names = NULL, check.names = FALSE)
      } else {
        out <- data.frame(filename = r$filename, grade = as.numeric(r$grade), t(as.data.frame(r$feats)), error = r$error %||% NA_character_, row.names = NULL, check.names = FALSE)
      }
      utils::write.csv(out, file, row.names = FALSE)
    }
  )

  # -----------------------
  # Batch processing (ZIP)
  # -----------------------
  batch_results <- eventReactive(input$run_batch, {
    req(input$zipfile)
    validate(need(requireNamespace("magick", quietly = TRUE), "Package 'magick' is required. Install it with install.packages('magick')."))
    tmp <- tempfile("imgzip_")
    dir.create(tmp)
    utils::unzip(input$zipfile$datapath, exdir = tmp)
    files <- list.files(tmp, recursive = TRUE, full.names = TRUE)
    files <- files[.safe_is_image(files)]

    mdl <- current_model()
    withProgress(message = "Processing ZIP…", value = 0, {
      n <- length(files)
      if (!n) return(data.frame())
      rows <- vector("list", n)
      for (i in seq_along(files)) {
        incProgress(1 / n, detail = basename(files[i]))
        fpath <- files[i]
        feats <- tryCatch(extract_quality_features(fpath), error = function(e) NULL)
        if (is.null(feats)) {
          rows[[i]] <- data.frame(filename = basename(fpath), grade = NA_real_, error = "Failed to read/extract", row.names = NULL)
        } else {
          grade <- tryCatch(predict_quality_grade(mdl, feats), error = function(e) NA_real_)
          rows[[i]] <- data.frame(filename = basename(fpath), grade = as.numeric(grade), t(as.data.frame(feats)), row.names = NULL, check.names = FALSE)
        }
      }
      do.call(rbind, rows)
    })
  }, ignoreInit = TRUE)

  output$batch_table <- renderTable({
    br <- batch_results()
    if (is.null(br) || !nrow(br)) return(data.frame(message = "Upload a ZIP and click Process ZIP.", row.names = NULL))
    head(br, 50)
  }, striped = TRUE, hover = TRUE)

  output$download_batch <- downloadHandler(
    filename = function() paste0("image_quality_batch_", Sys.Date(), ".csv"),
    content = function(file) {
      br <- batch_results()
      req(br)
      utils::write.csv(br, file, row.names = FALSE)
    }
  )
}

shinyApp(ui = ui, server = server)
