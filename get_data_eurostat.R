library(eurostat)
library(data.table)


# Get data from Eurostat
# Data on population
pop <- get_eurostat("demo_pjan")
# Data on GDP
gdp <- get_eurostat("nama_10_gdp")
# Data on employment
emp <- get_eurostat("lfsa_egan2")
# Data on unemployment
une <- get_eurostat("une_rt_a")
# Search eurostat for term business demography by size class
bdem <- get_eurostat("bdem_1cind")

s = eurostat::search_eurostat("business demography by size class and NUTS 3 region")
setDT(s)
s

# Get bd_salge1_size_r and filter Bulgaria, Romania and Croatia
bd = get_eurostat("bd_salge1_size_r", 
                  filters = list(geo = c("BG", "RO", "HR")),
                  type = "both")
setDT(bd)
bd = na.omit(bd)
cols = setdiff(colnames(bd))
unique(bd[, 1])
unique(bd[, 2])
unique(bd[, 3])
unique(bd[, 4])
unique(bd[, 5])