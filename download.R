#------------------------------------------------------------------------------#
#----------------------------# Paketi i priprema #-----------------------------#

library(readr)
library(tidyverse)
library(eurostat)

if(!dir.exists("data_eurostat")) {
  dir.create("data_eurostat")
}

# Table of contents - TOC
eurostat_TOC <- get_eurostat_toc()

geo_filter <- c("BG", "RO", "HR", "RO225", "BG323", "HR04B", "HR025")

# OVO JE ZA RENDERIRANJE quarto file-a u web pregledniku !!
# quarto::quarto_serve("eurostat_data.qmd")

#------------------------------------------------------------------------------#
#----------------------------# Business demography #---------------------------#

# Business demography by size class and NUTS 3 regions
data_bd <- get_eurostat(
  id = "bd_enace2_r3",
  filters = list(
    indic_sb = c("V11910", "V11920", "V11930", "V97010", "V97020", "V97030", "V97120", "V97130"),
    nace_r2 = "B-S_X_K642",
    geo = geo_filter
  ),
  time_format = "num"
)
# Dešifriranje kodova u tablici
data_bd <- label_eurostat(data_bd)
count(data_bd, indic_sb)

# Spremanje podataka u lokalnu RDS datoteku
saveRDS(data_bd, file = "data_eurostat/eurostat_bd.rds")
#------------------------------------------------------------------------------#
#---------------------------------# Employment #-------------------------------#

# Preuzimanje podataka o zaposlenosti iz Eurostata
data_emp <- get_eurostat(
  id = "nama_10r_3empers",
  filters = list(
    wstatus = "EMP",
    geo = geo_filter
  ),
  time_format = "num"
)

# Dešifriranje kodova u tablici
data_emp <- label_eurostat(data_emp)

# Spremanje podataka u lokalnu RDS datoteku
saveRDS(data_emp, file = "data_eurostat/eurostat_employment.rds")
#------------------------------------------------------------------------------#
#-----------------------------# Fertility indicators #-------------------------#

# Preuzimanje podataka o Fertility indicators iz Eurostata
data_fertility <- get_eurostat(
  id = "demo_r_find3",
  filters = list(
    indic_de = NULL,  # Svi indikatori
    geo = geo_filter,
    unit = "NR"
  ),
  time_format = "num"
)

# Dešifriranje kodova u tablici
data_fertility <- label_eurostat(data_fertility)

# Spremanje podataka u lokalnu RDS datoteku
saveRDS(data_fertility, file = "data_eurostat/eurostat_fertility.rds")
#------------------------------------------------------------------------------#
#-----------------------------------# BDP #------------------------------------#

# Preuzimanje podataka o BDP-u (GDP) iz Eurostata
data_gdp <- get_eurostat(
  id = "nama_10r_3gdp",
  filters = list(
    geo = geo_filter
  ),
  time_format = "num"
)

# Dešifriranje kodova u tablici
data_gdp <- label_eurostat(data_gdp)

# Spremanje podataka u lokalnu RDS datoteku
saveRDS(data_gdp, file = "data_eurostat/eurostat_gdp.rds")
#------------------------------------------------------------------------------#
#-------------------------# Police- recoreded offences #-----------------------#

# Preuzimanje podataka o policijski evidentiranim kaznenim djelima (Crime) iz Eurostata
data_crime <- get_eurostat(
  id = "crim_gen_reg",
  filters = list(
    geo = geo_filter
  ),
  time_format = "num"
)

# Dešifriranje kodova u tablici
data_crime <- label_eurostat(data_crime)

# Spremanje podataka u lokalnu RDS datoteku
saveRDS(data_crime, file = "data_eurostat/eurostat_crime.rds")
#------------------------------------------------------------------------------#
#---------------------------# Population change #------------------------------#

# Preuzimanje podataka o promjeni populacije (Population change) iz Eurostata
data_pop_change <- get_eurostat(
  id = "demo_r_gind3",
  filters = list(
    geo = geo_filter
  ),
  time_format = "num"
)

# Dešifriranje kodova u tablici
data_pop_change <- label_eurostat(data_pop_change)

# Spremanje podataka u lokalnu RDS datoteku
saveRDS(data_pop_change, file = "data_eurostat/eurostat_pop_change.rds")
#------------------------------------------------------------------------------#
#-------------------------# Population on 1 January #--------------------------#

# Preuzimanje podataka o populaciji prema dobnoj skupini i spolu (Population by age group) iz Eurostata
data_pop_age <- get_eurostat(
  id = "demo_r_pjanaggr3",
  filters = list(
    geo = geo_filter
  ),
  time_format = "num"
)

# Dešifriranje kodova u tablici
data_pop_age <- label_eurostat(data_pop_age)

# Spremanje podataka u lokalnu RDS datoteku
saveRDS(data_pop_age, file = "data_eurostat/eurostat_pop_age.rds")
#------------------------------------------------------------------------------#
#-------------------------# Population on 1 January #--------------------------#

# Preuzimanje podataka o populacijskoj strukturi (Population structure) iz Eurostata
data_pop_structure <- get_eurostat(
  id = "demo_r_pjanind3",
  filters = list(
    indic_de = c("FMEDAGEPOP", "MEDAGEPOP", "MMEDAGEPOP"),  # Filtriranje prema indikatorima
    geo = geo_filter
  ),
  time_format = "num"
)

# Dešifriranje kodova u tablici
data_pop_structure <- label_eurostat(data_pop_structure)

# Spremanje podataka u lokalnu RDS datoteku
saveRDS(data_pop_structure, file = "data_eurostat/eurostat_pop_structure.rds")
#------------------------------------------------------------------------------#


