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
            "Choose image(s)",
            accept = c("image/png", "image/jpeg", "image/webp", "image/gif"),
            multiple = TRUE
          ),
          selectInput(
            "model_type",
            "Model",
            choices = c(
              "Linear regression (fake training data)" = "lm",
              "Random forest (fake training data; needs ranger)" = "rf"
            ),
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
              selectInput("selected_image", "Selected image", choices = character(0)),
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
                bslib::card_header("Results (multi-upload)"),
                bslib::card_body(
                  tableOutput("multi_results")
                )
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

  current_model <- reactive({
    # If rf requested but ranger missing, fall back to lm with a friendly message
    if (input$model_type == "rf" && !requireNamespace("ranger", quietly = TRUE)) {
      load_or_train_quality_model(model_type = "lm")
    } else {
      load_or_train_quality_model(model_type = input$model_type)
    }
  })

  output$status <- renderUI({
    if (is.null(input$image)) {
      bslib::alert(
        color = "secondary",
        "Waiting for an upload…"
      )
    } else {
      nm <- input$image$name
      msg <- if (length(nm) == 1) {
        paste0("Selected: ", nm)
      } else {
        paste0("Selected: ", length(nm), " images")
      }
      if (input$model_type == "rf" && !requireNamespace("ranger", quietly = TRUE)) {
        msg <- paste0(msg, " (Note: 'ranger' not installed; using linear model.)")
      }
      bslib::alert(
        color = "info",
        msg
      )
    }
  })

  observeEvent(input$image, {
    if (is.null(input$image) || !nrow(input$image)) {
      updateSelectInput(session, "selected_image", choices = character(0), selected = character(0))
    } else {
      choices <- stats::setNames(input$image$datapath, input$image$name)
      updateSelectInput(session, "selected_image", choices = choices, selected = input$image$datapath[[1]])
    }
  }, ignoreInit = TRUE)

  output$preview <- renderImage({
    req(input$image, input$selected_image)
    # Find the selected file's content type, if available
    idx <- match(input$selected_image, input$image$datapath)
    ctype <- if (!is.na(idx)) input$image$type[[idx]] else NULL
    list(src = input$selected_image, contentType = ctype)
  }, deleteFile = FALSE)

  prediction <- eventReactive(input$predict, {
    req(input$image)
    validate(
      need(requireNamespace("magick", quietly = TRUE), "Package 'magick' is required. Install it with install.packages('magick').")
    )
    mdl <- current_model()
    files <- input$image

    withProgress(message = "Scoring image(s)…", value = 0, {
      n <- nrow(files)
      rows <- vector("list", n)
      for (i in seq_len(n)) {
        incProgress(1 / n, detail = files$name[[i]])
        fp <- files$datapath[[i]]
        key <- paste0(tools::md5sum(fp)[[1]], "::", mdl$version)
        if (exists(key, envir = cache$pred, inherits = FALSE)) {
          rows[[i]] <- get(key, envir = cache$pred, inherits = FALSE)
          next
        }

        feats <- tryCatch(extract_quality_features(fp), error = function(e) e)
        if (inherits(feats, "error")) {
          res_i <- list(
            filename = files$name[[i]],
            datapath = fp,
            grade = NA_real_,
            feats = NULL,
            error = paste0("Feature extraction failed: ", conditionMessage(feats))
          )
          assign(key, res_i, envir = cache$pred)
          rows[[i]] <- res_i
          next
        }

        grade <- tryCatch(predict_quality_grade(mdl, feats), error = function(e) NA_real_)
        res_i <- list(
          filename = files$name[[i]],
          datapath = fp,
          grade = as.numeric(grade),
          feats = feats,
          error = NA_character_
        )
        assign(key, res_i, envir = cache$pred)
        rows[[i]] <- res_i
      }
      rows
    })
  }, ignoreInit = TRUE)

  output$grade <- renderText({
    if (is.null(prediction()) || is.null(input$selected_image)) {
      "—"
    } else {
      rows <- prediction()
      one <- NULL
      for (r in rows) if (identical(r$datapath, input$selected_image)) one <- r
      if (is.null(one) || is.na(one$grade)) "—" else sprintf("%.1f", one$grade)
    }
  })

  output$multi_results <- renderTable({
    req(prediction())
    rows <- prediction()
    data.frame(
      filename = vapply(rows, `[[`, character(1), "filename"),
      grade = vapply(rows, function(x) if (is.null(x$grade)) NA_real_ else as.numeric(x$grade), numeric(1)),
      error = vapply(rows, function(x) x$error %||% NA_character_, character(1)),
      row.names = NULL,
      check.names = FALSE
    )
  }, striped = TRUE, hover = TRUE, digits = 3)

  output$features <- renderTable({
    req(prediction(), input$selected_image)
    rows <- prediction()
    one <- NULL
    for (r in rows) if (identical(r$datapath, input$selected_image)) one <- r
    req(!is.null(one))
    if (is.null(one$feats)) {
      return(data.frame(message = one$error %||% "No features available.", row.names = NULL))
    }
    f <- one$feats
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
      rows <- prediction()
      out <- do.call(rbind, lapply(rows, function(r) {
        if (is.null(r$feats)) {
          data.frame(filename = r$filename, grade = as.numeric(r$grade), error = r$error %||% NA_character_, row.names = NULL, check.names = FALSE)
        } else {
          data.frame(filename = r$filename, grade = as.numeric(r$grade), t(as.data.frame(r$feats)), error = r$error %||% NA_character_, row.names = NULL, check.names = FALSE)
        }
      }))
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
