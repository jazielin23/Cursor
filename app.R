# app.R
# R Shiny demo: grade an item's "quality" from an image (0-10).
#
# Install:
#   install.packages(c("shiny", "magick"))
# Run:
#   shiny::runApp(".")

suppressPackageStartupMessages({
  library(shiny)
})

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

ui <- fluidPage(
  tags$head(
    tags$style(HTML("\
      body { max-width: 980px; margin: 0 auto; padding: 18px; }\
      .muted { color: #6b7280; font-size: 13px; }\
      .score { font-size: 28px; font-weight: 700; }\
    "))
  ),
  titlePanel("Image Quality Grader (demo)"),
  p(class = "muted", "Upload an image and get a predicted quality score from 0 to 10 (trained on fake data)."),

  sidebarLayout(
    sidebarPanel(
      fileInput("image", "Choose an image", accept = c("image/png", "image/jpeg", "image/webp", "image/gif")),
      actionButton("predict", "Predict grade"),
      tags$div(style = "height: 8px"),
      uiOutput("status")
    ),
    mainPanel(
      fluidRow(
        column(
          6,
          h4("Preview"),
          imageOutput("preview", height = "320px")
        ),
        column(
          6,
          h4("Result"),
          div(class = "score", textOutput("grade")),
          div(class = "muted", "(0 = worst, 10 = best)"),
          tags$div(style = "height: 10px"),
          h4("Extracted features"),
          tableOutput("features")
        )
      )
    )
  )
)

server <- function(input, output, session) {
  output$status <- renderUI({
    if (is.null(input$image)) {
      div(class = "muted", "Waiting for an upload…")
    } else {
      div(class = "muted", paste0("Selected: ", input$image$name))
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
    feats <- extract_quality_features(input$image$datapath)
    grade <- predict_quality_grade(model, feats)
    list(grade = grade, feats = feats)
  })

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
  })
}

shinyApp(ui = ui, server = server)
