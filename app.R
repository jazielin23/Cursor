# app.R
# R Shiny demo: grade an item's "quality" from an image (0-10).
#
# Install:
#   install.packages(c("shiny", "bslib", "magick"))
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

model <- load_or_train_quality_model()

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
          actionButton("predict", "Predict grade", class = "btn-primary"),
          tags$div(style = "height: 10px"),
          uiOutput("status"),
          tags$hr(),
          tags$div(
            class = "text-muted small",
            "This is a demo model trained on fake data. ",
            "The score is an example (0 = worst, 10 = best)."
          )
        ),
        bslib::layout_columns(
          col_widths = c(6, 6),
          bslib::card(
            full_screen = TRUE,
            bslib::card_header("Preview"),
            bslib::card_body(
              imageOutput("preview", height = "360px")
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
    )
  )
)

server <- function(input, output, session) {
  output$status <- renderUI({
    if (is.null(input$image)) {
      bslib::alert(
        color = "secondary",
        "Waiting for an upload…"
      )
    } else {
      bslib::alert(
        color = "info",
        paste0("Selected: ", input$image$name)
      )
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
    res <- withProgress(message = "Scoring image…", value = 0, {
      incProgress(0.2, detail = "Extracting features")
      feats <- extract_quality_features(input$image$datapath)
      incProgress(0.6, detail = "Predicting grade")
      grade <- predict_quality_grade(model, feats)
      incProgress(1, detail = "Done")
      list(grade = grade, feats = feats)
    })
    res
  }, ignoreInit = TRUE)

  output$grade <- renderText({
    if (is.null(prediction())) {
      "—"
    } else {
      sprintf("%.1f", prediction()$grade)
    }
  })

  output$features <- renderTable({
    req(prediction())
    f <- prediction()$feats
    data.frame(
      feature = names(f),
      value = as.numeric(f),
      row.names = NULL
    )
  }, striped = TRUE, hover = TRUE, digits = 4)
}

shinyApp(ui = ui, server = server)
