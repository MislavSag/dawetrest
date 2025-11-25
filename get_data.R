library(data.table)
library(httr)
library(jsonlite)
library(lubridate)
library(readxl)
library(stringr)
library(anytime)
library(AzureStor)
library(rvest)
library(tabulapdf)
library(stringr)
library(janitor)
library(sreg)
library(DBI)
library(RMariaDB)



# UTILS -------------------------------------------------------------------
# Globals
ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0))"
accept = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"

# Azure blob creds
blob_endpoint = "https://contentiobatch.blob.core.windows.net/"
bl_endp_key = storage_endpoint(blob_endpoint, Sys.getenv("KEY"))
cont = storage_container(bl_endp_key, "dawetrest")


# JLS CODES ---------------------------------------------------------------
# Get JLS codes. This helpts merging data for different municipalities
tmp_file = tempfile("codes_locals.pdf")
url = "https://www.porezna-uprava.hr/obrazac_joppd/Documents/%C5%A0ifrarnik%20gradova%20i%20op%C4%87ina.pdf"
RETRY("GET", url, write_disk(tmp_file, overwrite = TRUE))
codes_locals = tabulapdf::extract_tables(tmp_file, col_names = FALSE)
codes_locals = lapply(codes_locals, as.data.table)
codes_locals = rbindlist(codes_locals)
codes_locals = codes_locals[, .SD, .SDcols = -3]
setnames(codes_locals, c("jlprs", "jlprs_control", "name"))
cols_ = colnames(codes_locals)[1:2]
codes_locals = codes_locals[, (cols_) := lapply(.SD, as.integer), .SDcols = cols_]

# Save localy
fwrite(codes_locals, file.path("data", "jls_codes.csv"))

# Save table to Azure blob
storage_write_csv(codes_locals, cont, "jls_codes.csv")


# DEMOGRAPHY --------------------------------------------------------------
# Data need to be downloaded manually
# Import data from DZS
pop_path = "data/popis_2021-stanovnistvo_po_gradovima_opcinama.xlsx"
excel_sheets(pop_path)
population = readxl::read_xlsx(pop_path, sheet = "1.", skip = 6)
population = clean_names(population)
setDT(population)
population_dt = population[, .(county = zupanija,
                               naziv_opcine = grad_opcina_town_municipality,
                               population_total = ukupno_total)]
# population_dt = population_dt[naziv_opcine %in% OPCINE]

# Ageing
pop_cont = readxl::read_xlsx(pop_path, sheet = "6.", skip = 7)
pop_cont = clean_names(pop_cont)
setDT(pop_cont)
pop_cont_dt = pop_cont[, .(county = zupanija,
                           naziv_opcine = grad_opcina_town_municipality,
                           sex,
                           average_age = prosjecna_starost_average_age,
                           ageing_index = indeks_starenja_ageing_index,
                           age_coefficient = koeficijent_starosti_age_coefficient
)]
# pop_cont_dt = pop_cont_dt[naziv_opcine %in% OPCINE & sex == "All", .SD, .SDcols = -"sex"]
pop_cont_dt = pop_cont_dt[sex == "All", .SD, .SDcols = -"sex"]

# Merge population data
population_dt = merge(population_dt, pop_cont_dt, by = c("county", "naziv_opcine"))

# Save
fwrite(population_dt, file.path("data", "population.csv"))
storage_write_csv(population_dt, cont, "population.csv")



# NKD ---------------------------------------------------------------------
# Source: https://ec.europa.eu/eurostat/web/metadata/classifications
nacerev2 = fread("data/NACE2_NACE2.1_Table.csv")
nacerev2_dt = nacerev2[, .(nkd2007 = gsub("\\.", "", NACE2_CODE), 
                           nkd2007_name = NACE2_HEADING)]
nacerev2_dt = unique(nacerev2_dt)

# Save
fwrite(nacerev2_dt, file.path("data", "nkd2007.csv"))
storage_write_csv(nacerev2_dt, cont, "nkd2007.csv")


# COURT REGISTRY ----------------------------------------------------------
# Init
sreg = Sreg$new()

# Business subjects
sreg$endpoint_parameters("/subjekti")
subjects = sreg$get_loop("subjekti", q = list(only_active = FALSE))

# Remove rows with missing MBS
subjects = na.omit(subjects, cols = "mbs")
subjects[, let(V1 = NULL)]
subjects_active = subjects[status == 1]

# Headquarters
sreg$endpoint_parameters("/sjedista")
headq = sreg$get_loop("sjedista")
headq = na.omit(headq, cols = "mbs")
headq[, let(V1 = NULL)]

# Short name
sreg$endpoint_parameters("/skracene_tvrtke")
short_names = sreg$get_loop("skracene_tvrtke")
short_names = na.omit(short_names, cols = "mbs")
short_names[, let(V1 = NULL)]

# # Core business
# cores = get_sreg_loop("pretezite_djelatnosti")
# nrow(cores[is.na(mbs)])
# cores = cores[!is.na(mbs)]
# 
# # Industry
# industry = get_sreg_loop("predmeti_poslovanja")
# nrow(industry[is.na(mbs)])
# industry = industry[!is.na(mbs)]
# 
# # Evidence activities
# activities = get_sreg_loop("evidencijske_djelatnosti")
# nrow(activities[is.na(mbs)])
# activities = activities[!is.na(mbs)]
# 
# # NKD ids
# nks = sreg$get_wrap("nacionalna_klasifikacija_djelatnosti")
# ids = nks[grepl("^68.3", sifra), id]
# 
# # Pretezite djelatnosti/
# sreg$get_wrap("/pretezite_djelatnosti", q = list(limit = 1000))
# pretezite_djelatnosti = sreg$get_loop("/pretezite_djelatnosti")
# pretezite_djelatnosti = na.omit(pretezite_djelatnosti, cols = "nacionalna_klasifikacija_djelatnosti_id")

# Merge sreg data
dt_sreg= Reduce(
  function(x, y) merge(x, y, by = "mbs", all.x = TRUE, all.y = FALSE),
  list(subjects, headq, short_names)
)

# Missing values
if (interactive()) {
  dt_sreg[, sum(is.na(datum_osnivanja))]
  dt_sreg[, sum(is.na(sifra_opcine))]
  # dt[, sum(is.na(nacionalna_klasifikacija_djelatnosti_id))]
}

# Clean var classes
date_format_ = "%Y-%m-%dT%H:%M:%S"
dt_sreg[, datum_osnivanja := as.POSIXct(datum_osnivanja, format = date_format_)]
dt_sreg[, datum_brisanja  := as.POSIXct(datum_brisanja, format = date_format_)]

# Clean OIB column
dt_sreg[, oib := str_pad(oib, width = 11, side = "left", pad = "0")]

# Save
fwrite(dt_sreg, file.path("data", "court_registry.csv"))
storage_write_csv(dt_sreg, cont, "court_registry.csv")


# FINA --------------------------------------------------------------------
# MySQL connection
con = dbConnect(
  MariaDB(),
  host="91.234.46.219", 
  port=3306,
  user="odvjet12_mislav", 
  password="Contentio0207",
  dbname="odvjet12_gfi"
)

# Parameters
year   = 2020L
county = 3L

# a) Fetch a subset of columns
qry = "SELECT subjectid, reportyear, countyid, b127, b045
        FROM db_afs
        WHERE reportyear = ? AND countyid = ?"

rs <- dbSendQuery(con, qry)
dbBind(rs, list(year, county))
res <- dbFetch(rs)
dbClearResult(rs)

# b) Aggregate example
qry2 <- "SELECT countyid, reportyear,
                SUM(b014) AS sum_b014,
                SUM(b045) AS sum_b045
         FROM db_afs
         WHERE reportyear = ? AND countyid = ?
         GROUP BY countyid, reportyear"

rs2 <- dbSendQuery(con, qry2)
dbBind(rs2, list(year, county))
agg <- dbFetch(rs2)
dbClearResult(rs2)

dbDisconnect(con)


# Merge zaglavlje and fina 2022
zaglavlje = zaglavlje[fina_2023[, .(
  oib,
  mb,
  revenue_2022 = aop_pg2_127,
  revenue_2023 = aop_tg2_127,
  profit_2022   = aop_pg2_185,
  profit_2023   = aop_tg2_185,
  asset_2022 = aop_pg1_125,
  asset_2023 = aop_tg1_125
)],
on = c("OIB" = "oib")]

# extract important variables
fina_dt = zaglavlje[, .(
  oib          = OIB, 
  mbs          = MBS,
  mb           = MATBROJ,
  name         = NAZIV,
  nkd2002      = NKD2002,
  nkd2007      = NKD2007,
  county       = ZUPANIJA,
  municip      = OPCINA, 
  size         = VEL,
  owner_type   = VLAST,
  owner_source = ULAZI,
  cap_source   = KAP_DOM,
  emp_hour     = ZAPOSL_SATIPR,
  month_work   = MJ_POSL_TK,
  revenue_2022, revenue_2023,
  profit_2022, profit_2023,
  asset_2022, asset_2023
)]

# Save
fwrite(fina_dt, file.path("data", "fina.csv"))
storage_write_csv(fina_dt, cont, "fina.csv")


# NGO ---------------------------------------------------------------------
# Get NGO data from public sources
GET("https://banovac.mfin.hr/rnoprt/Export",
    write_disk("data/udruge.csv", overwrite = TRUE))
udruge = read.csv("data/udruge.csv", sep="$", fileEncoding="UTF-16")
udruge = as.data.table(udruge)
udruge = clean_names(udruge)
setnames(udruge,
         c("x14_postanski_broj", "x18_statisticka_oznaka_grada_opcine"),
         c("postanski_broj", "jprs_id"))

# Save
fwrite(udruge, file.path("data", "ngo.csv"))
storage_write_csv(udruge, cont, "ngo.csv")


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
      # x = excel_tables[[1]]
      colnames_1 = zoo::na.locf(unlist(x[1], use.names = FALSE))
      colnames_3 = unlist(x[3], use.names = FALSE)
      colnames_3[is.na(colnames_3)] = ""
      colnames_ = janitor::make_clean_names(paste0(colnames_1, colnames_3))
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


# HZZ ---------------------------------------------------------------------
# Source: https://www.hzz.hr/zupanijske-publikacije/osjecko-baranjska/

# Get pdf links
pdf_links = read_html("https://www.hzz.hr/zupanijske-publikacije/osjecko-baranjska/") |>
  html_elements("a") |>
  html_attr("href")
pdf_links = pdf_links[grepl("odis", pdf_links)]
  
# Find areas in pdf
# locate_areas(
#   pdf_links[1],
#   pages = 29,
#   resolution = 60L,
#   widget = "shiny",
#   copy = FALSE
# )

# Parse pdf
hzz_tbl = lapply(pdf_links[1:8], function(l) {
  x = extract_tables(l,
                     pages = 28,
                     guess = FALSE,
                     area = list(c(140, 46, 726, 532))) # c(110, 60, 715, 545)
  x = lapply(x, function(y) {cbind.data.frame(year = str_extract(basename(l), "\\d+"), y)})
  colnames(x[[1]]) = c("year", "name", "total", "men", "women", 
                       "no_primary_school", "primary_school", "secondary_school_3y",
                       "secondary_school_4y", "university", "mag_phd", "index")
  x[[1]]
})
hzz_tbl[[1]]
hzz = rbindlist(hzz_tbl)

# Save localy
fwrite(hzz, file.path("data", "hzz.csv"))

# Save table to Azure blob
blob_endpoint = "https://contentiobatch.blob.core.windows.net/"
blob_key = "qdTsMJMGbnbQ5rK1mG/9R1fzfRnejKNIuOv3X3PzxoBqc1wwTxMyUuxNVSxNxEasCotuzHxwXECo79BLv71rPw=="
bl_endp_key = storage_endpoint(blob_endpoint, blob_key)
cont = storage_container(bl_endp_key, "dawetrest")
storage_write_csv(hzz, cont, "hzz.csv")



