options(ask.model = "llama3.1")

library(ask)


ask("where is the Eiffel tower? 1 sentence")

ask("What is Dawetrest?")

ask("What is Dawetrest?", 
    context = context_url(c(lab = "https://contentiobatch.blob.core.windows.net/dawetrest/Grant Agreement - GAP-101113015_removed.pdf")))

cont = "C:/Users/Mislav/Documents/GitHub/dawetrest/teams/General/01 Project Overview/03 Important Documents/Consortium Agreement DAWetRest-v2-FINAL-26-05-2023-signed 19-07-2023.pdf"
ask("What is Dawetrest?", 
    context = context("new label" = cont))


file = "C:/Users/Mislav/Documents/aih/dawetrest/Grant Agreement - GAP-101113015_removed.pdf"
opw = ""
upw = ""

context_pdf <- function(
    file, 
    pages = NULL,
    opw = "",
    upw = "",
    dpi = 600,
    language = "eng",
    options = NULL,
    type = "text") {
  rlang::check_installed("pdftools")
  type = rlang::arg_match(type, c("text", "ocr_text", "ocr_data"), multiple = TRUE)
  contexts <- list()
  for (i in seq_along(type)) {
    contexts[[i]] <-   switch(
      type[[i]],
      text = context('Pdf file OCRed text content: {file}' := pdftools::pdf_text(
        pdf = file, 
        opw = opw, 
        upw = upw
      )),
      ocr_text = context('Pdf file OCRed text content: {file}' := pdftools::pdf_ocr_text(
        pdf = file, 
        opw = opw, 
        upw = upw,
        dpi = dpi,
        language = language,
        options = options
      )),
      ocr_data = context('Pdf file OCRed data content: {file}' := pdftools::pdf_ocr_data(
        pdf = file, 
        opw = opw, 
        upw = upw,
        dpi = dpi,
        language = language,
        options = options
      )),
    )
  }
  context(!!!contexts)
}

ask("what is DaWetRest priject about?", context_pdf(file))
