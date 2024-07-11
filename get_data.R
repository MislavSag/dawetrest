library(data.table)
library(httr)
library(jsonlite)
library(lubridate)
library(readxl)
library(stringr)
library(anytime)
library(AzureStor)


UTILS -------------------------------------------------------------------
# Globals
URL       = "https://sudreg-data.gov.hr/api/javni/"
url_token =  "https://sudreg-data.gov.hr/api/oauth/token"
user      = "wTVAwMD-mfGsV_q-mcTRMw.."
pass      =  "1uPjdUWOqkJ1HgDGrIvW0w.."
ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0))"
accept = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"

# Make the POST request
token = POST(
  url_token,
  authenticate(user, pass),
  body = list(grant_type = "client_credentials"),
  encode = "form",
  config(ssl_verifypeer = FALSE)
)
token = content(token)

# # Endpoints
# endpoints = read_json("data/endpoints.json")
# get_endpoints = function() {
#   url = "https://sudreg-data.gov.hr/api/OpenAPIs/OpenAPIJavni"
#   res = GET(url, add_headers("User-Agent" = ua, "Accept" = accept))
#   content(res, type = "application/json", encoding = "UTF-8")
#   p = content(res, as = "text", type = "application/json", encoding = "UTF-8")
#   j = toJSON(p)
#   return(j)
# }

# Get data from court public registry
get_sreg <- function(url) {
  # url = "https://sudreg-data.gov.hr/api/javni/sudovi"
  # url = urls[[1]]
  res <- GET(url,
             add_headers("Authorization" = paste0("Bearer ", token$access_token),
                         "Content-Type" = "application/json"))
  # content(res, as = "text")
  content(res)
}

# Get data from court public registry in a loop
get_sreg_loop <- function(tag = "subjekti", by = 10000) {
  offset_seq = seq(0, 360000, by = by)
  offset_seq = format(offset_seq, scientific = FALSE)
  offset_seq = gsub("\\s+", "", offset_seq)
  urls = lapply(offset_seq, function(x) {
    modify_url(
      paste0(URL, tag),
      query = list(
        offset = x,
        limit = format(by, scientific = FALSE),
        only_active = FALSE
      )
    )
  })
  res_l = lapply(urls, get_sreg)
  res_l = res_l[!sapply(res_l, function(x) length(x) == 0)]
  res_l = lapply(res_l, function(l) rbindlist(lapply(l, as.data.table), fill = TRUE))
  rbindlist(res_l, fill = TRUE)
}


# SUDSKI REGISTAR ---------------------------------------------------------
# Business subjects
subjects = get_sreg_loop("subjekti")
subjects_active = subjects[status == 1]

# Headquarters
headq = get_sreg_loop(tag = "sjedista")

# Core business
cores = get_sreg_loop("pretezite_djelatnosti")

# Merge sreg data
dt = Reduce(
  function(x, y) merge(x, y, by = "mbs", all = TRUE),
  list(subjects_active, headq, cores)
)

# Missing values
dt[, sum(is.na(sifra_opcine))]
dt[, sum(is.na(nacionalna_klasifikacija_djelatnosti_id))]


# FINA --------------------------------------------------------------------
# import data
fina_2022 = fread(file.path("data_ignore", "RGFI_javna_objava_2022.csv"), encoding = "UTF-8")

# extract important variables
fina_2022[, .(oib, mb, prihodi_2021 = aop_pg2_127, prihodi_2022 = aop_tg2_127)]

# Extract municipalities
fina_2022[oib %in% dt[, oib]]



# COURT REGISTRY ANALYSIS -------------------------------------------------
# filter municipalities
# 914: Draž
# 1104: Erdut
# 29297: Kneževi Vinogradi
# dt[grepl("Bilje", naziv_opcine)]
opcine_id = c(914, 1104, 213, 29297)
dt[sifra_opcine == 914]
dt[sifra_opcine %in% opcine_id]


dt[sifra_opcine %in% opcine_id, .N, by = postupak]


# # globals
# host <- "https://sudreg-api.pravosudje.hr"
# host_api <- "https://api.data-api.io/v1"
# 
# # input data
# input_dt <- fread("D:/ds_projects/versus/inputs/versus_001.csv")
# input_dt <- clean_names(input_dt)
# input_dt <- na.omit(input_dt, cols = "oib")
# input_dt[, oib := str_pad(oib, 11, "left", "0")]
# 
# # utils
# get_sreg <- function(url) {
#   res <- GET(url,
#              add_headers("Ocp-Apim-Subscription-Key" = "0c7a9bbd34674a428e4218340fba732b",
#                          "Host" = "sudreg-api.pravosudje.hr"))
#   content(res)
# }
# 
# get_blokade <- function(oib) {
#   res <- GET(paste0(host_api, "/blokade/", oib),
#              add_headers('x-dataapi-key' = "59dd75a6525e"))
#   content(res)
# }
# 
# # subjekti
# url <- modify_url(host, path = "javni/subjekt",
#                   query = list(offset = 0,
#                                limit = format(1000000, scientific = FALSE),
#                                only_active = FALSE))
# subjekti <- get_sreg(url)
# subjekti <- rbindlist(subjekti, fill = TRUE)
# 
# # postupci
# url <- modify_url(host, path = "javni/postupak",
#                   query = list(offset = 0, limit = format(1000000, scientific = FALSE)))
# postupci <- get_sreg(url)
# postupci <- rbindlist(postupci, fill = TRUE)
# 
# # merge all data
# sreg_data <- Reduce(function(x, y) merge(x, y, by = "mbs", all.x = TRUE, all.y = FALSE),
#                     list(subjekti, postupci[, .(mbs, datum_stecaja)]))
# sreg_data[, oib := str_pad(oib, 11, "left", "0")]
# 


# CROATIAN PENSION INSURANCE INSTITUTE ------------------------------------
# Source: https://www.mirovinsko.hr/hr/statistika/2673

# Create urls
months_ = seq.Date(
  as.Date("2020-01-01"), 
  floor_date(Sys.Date(), unit = "month") %m-% months(1), 
  by = "month")
year_month_format = vapply(months_, function(m) {
  if (data.table::year(m) <= 2020) {
    x = format.Date(m, "%Y-%m")
  } else {
    x = gsub("-0", "-", format.Date(m, "%Y-%m"))
  }
  x
}, character(1))
urls = paste0(
  "https://www.mirovinsko.hr/UserDocsImages/statistika/osiguranici-",
  data.table::year(months_),
  "/osiguranici-zupanije-opcine-osnove-osiguranja-",
  year_month_format,
  ".xlsx"
)

# Create directory in which we will save the data. add also to .gitignore
dir_ = "CPII"
if (!dir.exists(dir_)) {
  dir.create(dir_)
}

# Get all documents
for (u in urls) {
  # u = urls[1]
  file_name = gsub(".*ranja-", "", u)
  GET(u, write_disk(file.path(dir_, file_name), overwrite = TRUE))
}

# Utils function for parsing tables inside sheet
parse_excel_file = function(excel_file) {
  # excel_file = list.files(dir_, full.names = TRUE)[1]
  excle_tables_l = lapply(1:21, function(sheet_) {
    print(sheet_)
    # sheet_ = 17
    excel_file_to_parse = read_excel(excel_file, sheet = sheet_)
    sifra_index = c(grep("ifra", excel_file_to_parse[, 1, drop = TRUE]), nrow(excel_file_to_parse))
    row_indecies = lapply(seq_along(sifra_index[-length(sifra_index)]), function(i) sifra_index[i]:sifra_index[i + 1])
    excel_tables = lapply(row_indecies, function(il) {
      read_excel(excel_file, sheet = sheet_, range = cell_rows(il))
    })
    excel_tables = lapply(excel_tables, as.data.table)
    excel_tables = lapply(excel_tables, function(x) x[rowSums(is.na(x)) < ncol(x)])
    excel_tables = lapply(excel_tables, function(x) {
      # x = excel_tables[[3]]
      colnames_1 = zoo::na.locf(unlist(x[1], use.names = FALSE))
      colnames_3 = unlist(x[3], use.names = FALSE)
      colnames_3[is.na(colnames_3)] = ""
      colnames_ = janitor::make_clean_names(paste0(colnames_1, colnames_2))
      setnames(x, colnames_)
      colnames(x)[1:2] = c("sifra", "naziv")
      x
    })
    excel_tables = lapply(excel_tables, function(x) x[5:nrow(x)])
    excel_tables = lapply(excel_tables, function(x) na.omit(x, cols = "sifra"))
    dt_ = rbindlist(excel_tables, fill = TRUE)
    dt_ = melt(dt_, id.vars = c("sifra", "naziv"), variable.name = "var", value.name = "value")
    dt_ = na.omit(dt_)
    dt_ = dcast(dt_, sifra + naziv ~ var, value.var = "value")
    dt_ = cbind(zup_code = str_pad(sheet_, width = 2, side = "left", pad = "0"), dt_)
    dt_
  })
  excel_tables_dt = rbindlist(excle_tables_l)
  excel_tables_dt
}

# Parse all excel files
excel_files = list.files(dir_, full.names = TRUE)
excel_tables_l = lapply(excel_files, parse_excel_file)
excel_tables_l_ = copy(excel_tables_l)
excel_tables_l_ = lapply(seq_along(excel_tables_l_), function(i) {
  excel_tables_l_[[i]][, month := anytime(tools::file_path_sans_ext(basename(excel_files[i])))]
})
excel_tables_dt = rbindlist(excel_tables_l_)
cols_to_integer = colnames(excel_tables_dt)[4:27]
excel_tables_dt[, (cols_to_integer) := lapply(.SD, as.integer), .SDcols = cols_to_integer]

# English colnames
eng_column_names = c(
  "zup_code", "code", "name",
  "workers_with_legal_entities_men", "workers_with_legal_entities_women", "workers_with_legal_entities_total",
  "craftsmen_men", "craftsmen_women", "craftsmen_total",
  "farmers_men", "farmers_women", "farmers_total",
  "self_employed_professionals_men", "self_employed_professionals_women", "self_employed_professionals_total",
  "workers_with_physical_persons_men", "workers_with_physical_persons_women", "workers_with_physical_persons_total",
  "insured_employees_with_international_men", "insured_employees_with_international_women", "insured_employees_with_international_total",
  "insured_men", "insured_women", "insured_total",
  "total_men", "total_women", "total_total", "month"
)
setnames(excel_tables_dt, eng_column_names)

# Check Draz
plot(excel_tables_dt[name == "Draž", .(month, workers_with_legal_entities_total)])

# Save localy
fwrite(excel_tables_dt, file.path("data", "cpii.csv"))

# Save table to Azure blob
blob_endpoint = "https://contentiobatch.blob.core.windows.net/"
blob_key = "qdTsMJMGbnbQ5rK1mG/9R1fzfRnejKNIuOv3X3PzxoBqc1wwTxMyUuxNVSxNxEasCotuzHxwXECo79BLv71rPw=="
bl_endp_key = storage_endpoint(blob_endpoint, blob_key)
cont = storage_container(bl_endp_key, "dawetrest")
storage_write_csv(excel_tables_dt, cont, "cpii.csv")
