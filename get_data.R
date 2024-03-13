library(data.table)
library(httr)
library(jsonlite)



# UTILS -------------------------------------------------------------------
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

