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
if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("Package 'jsonlite' is required. Install it with install.packages('jsonlite').", call. = FALSE)
}
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
            "Choose an image",
            accept = c("image/png", "image/jpeg", "image/webp", "image/gif")
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
          tags$hr(),
          tags$h6("Advanced models (optional)"),
          selectInput(
            "adv_model",
            "Advanced model",
            choices = c(
              "None" = "none",
              "EfficientNetV2-S (ImageNet labels)" = "effv2s",
              "ViT-B/16 (ImageNet labels)" = "vit_b16",
              "ViT-L/16 (ImageNet labels)" = "vit_l16",
              "CLIP (zero-shot quality score)" = "clip_quality"
            ),
            selected = "none"
          ),
          conditionalPanel(
            condition = "input.adv_model == 'clip_quality'",
            textInput("clip_pos", "CLIP positive prompt", "a high quality, sharp photo"),
            textInput("clip_neg", "CLIP negative prompt", "a low quality, blurry, noisy photo")
          ),
          actionButton("run_adv", "Run advanced model", class = "btn-outline-primary"),
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
              ),
              tags$div(style = "height: 12px"),
              bslib::card(
                bslib::card_header("Advanced model output"),
                bslib::card_body(
                  uiOutput("adv_status"),
                  tableOutput("adv_table"),
                  verbatimTextOutput("adv_raw", placeholder = TRUE)
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
  adv_res <- reactiveVal(NULL)

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

  observeEvent(input$run_adv, {
    req(input$image)
    if (identical(input$adv_model, "none")) {
      adv_res(NULL)
      return()
    }

    # Call optional Python CLI. If missing deps, it returns {"ok":false,...}.
    script <- file.path(getwd(), "py_models", "infer_cli.py")
    if (!file.exists(script)) {
      adv_res(list(ok = FALSE, error = "Missing py_models/infer_cli.py in app folder"))
      return()
    }

    py <- Sys.which("python")
    py_args_prefix <- character(0)
    if (identical(py, "")) {
      # Windows often has the launcher as `py`
      py <- Sys.which("py")
      if (!identical(py, "")) py_args_prefix <- c("-3")
    }
    if (identical(py, "")) {
      adv_res(list(ok = FALSE, error = "Python not found on PATH. Install Python and ensure `python` (or `py`) is available."))
      return()
    }

    task <- if (identical(input$adv_model, "clip_quality")) "clip_quality" else "imagenet_topk"
    args <- c(script, task, "--image", input$image$datapath)
    if (task == "imagenet_topk") {
      arch <- switch(
        input$adv_model,
        effv2s = "efficientnet_v2_s",
        vit_b16 = "vit_b_16",
        vit_l16 = "vit_l_16",
        "efficientnet_v2_s"
      )
      args <- c(args, "--arch", arch, "--k", "5")
    } else {
      args <- c(args, "--pos", input$clip_pos, "--neg", input$clip_neg)
    }

    # Windows quoting: system2 builds a single command line; quote values to keep spaces intact.
    q <- function(x) as.character(shQuote(x, type = "cmd"))
    args_q <- args
    # Quote the script path and value args (but leaving flags alone is fine too)
    args_q[1] <- q(args_q[1])              # script
    if (length(args_q) >= 4) args_q[4] <- q(args_q[4])  # image path
    if (task == "clip_quality") {
      # --pos value is at end-3, --neg value at end
      args_q[length(args_q) - 1] <- q(args_q[length(args_q) - 1])
      args_q[length(args_q)] <- q(args_q[length(args_q)])
    }

    cmd_string <- paste(c(shQuote(py, type = "cmd"), py_args_prefix, args_q), collapse = " ")
    out <- tryCatch(
      suppressWarnings(system2(py, c(py_args_prefix, args_q), stdout = TRUE, stderr = TRUE)),
      error = function(e) paste("ERROR:", conditionMessage(e))
    )
    txt <- paste(out, collapse = "\n")
    status <- attr(out, "status")
    if (is.null(status)) status <- 0

    parsed <- tryCatch(
      jsonlite::fromJSON(txt),
      error = function(e) list(ok = FALSE, error = "Failed to parse python output (likely dependency missing).", raw = txt)
    )
    if (is.list(parsed)) {
      if (is.null(parsed$raw)) parsed$raw <- txt
      parsed$status <- status
      parsed$python <- py
      parsed$cmd <- cmd_string
    }
    adv_res(parsed)
  }, ignoreInit = TRUE)

  output$adv_status <- renderUI({
    r <- adv_res()
    if (is.null(r)) return(div(class = "text-muted", "No advanced model run yet."))
    if (isTRUE(r$ok)) return(div(class = "text-success", "OK"))
    div(
      class = "text-danger",
      paste0("Error: ", r$error %||% "Unknown error"),
      if (!is.null(r$status)) paste0(" (exit status ", r$status, ")") else ""
    )
  })

  output$adv_table <- renderTable({
    r <- adv_res()
    if (is.null(r) || !isTRUE(r$ok)) return(NULL)
    if (identical(r$task, "clip_quality")) {
      data.frame(score_0_10 = r$score, diff = r$diff, pos = r$pos, neg = r$neg, row.names = NULL)
    } else if (identical(r$task, "imagenet_topk")) {
      data.frame(label = r$topk$label, prob = r$topk$prob, row.names = NULL)
    } else {
      NULL
    }
  }, striped = TRUE, hover = TRUE, digits = 4)

  output$adv_raw <- renderText({
    r <- adv_res()
    if (is.null(r)) return("")
    paste0(
      if (!is.null(r$cmd)) paste0("Command:\n", r$cmd, "\n\n") else "",
      if (!is.null(r$python_executable)) paste0("Python executable (from CLI): ", r$python_executable, "\n") else "",
      if (!is.null(r$python_version)) paste0("Python version (from CLI): ", r$python_version, "\n\n") else "",
      r$raw %||% ""
    )
  })

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
