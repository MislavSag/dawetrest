library(fs)


dir_ = "app"

# delete directory
dir_delete(dir_)
dir.create(dir_)

file_qmd = list.files(pattern = "*.qmd")
files = c(file_qmd, "reference.bib", ".Renviron", "_publish.yml")
files_to = file.path("app", files)
file.copy(files, files_to)

files_data = list.files("data", full.names = TRUE)
files_data_to = file.path("app", "data", basename(files_data))
dir.create(file.path(dir_, "data"))
file.copy(files_data, files_data_to, recursive = TRUE)

eurostat_data = list.files("data_eurostat", full.names = TRUE)
eurostat_data_to = file.path("app", "data_eurostat", basename(eurostat_data))
dir.create(file.path(dir_, "data_eurostat"))
file.copy(eurostat_data, eurostat_data_to)


library(quarto)
quarto_publish_app(
  input = "app",
  name = "dawetrest",
  title = "DaWetRest Business Model",
  server = "shinyapps.io",
  forceUpdate = TRUE
)
