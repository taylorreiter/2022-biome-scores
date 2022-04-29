library(stringr)
library(readr)
library(dplyr)
library(tidyr)
library(janitor)

# read in and format mgnify sample data -----------------------------------

# read in mgnify sample data. These are candidate metagenomes.
mgnify_mgx_candidates <- read_csv(snakemake@input[['mgnify_samples']])

# count the characters in the biome field to select the most specific biome;
# some samples are repeated as each level of the biome lineage is represented
mgnify_mgx_candidates<- mgnify_mgx_candidates %>%
  mutate(biome_char_count = str_length(biome)) %>%
  group_by(id) %>%
  slice_max(order_by = biome_char_count)

# parse the biome field into its lineage
mgnify_mgx_candidates <- mgnify_mgx_candidates%>%
  select(id, biome) %>%
  mutate(biome = gsub("Host-associated", "Host_associated", biome)) %>%
  separate(biome, into = c("root", "step1", "step2", "step3", "step4", "step5", "step6"), sep = "-", remove = F)

write_tsv(mgnify_mgx_candidates, snakemake@output[['mgnify_samples']])
# read in sra data --------------------------------------------------------

sra_mgx <- read_csv(snakemake@input[['sra']])%>%
  clean_names() %>%
  filter(library_strategy %in% c("WGS", "OTHER"))

mgnify_mgx <- inner_join(mgnify_mgx_candidates, sra_mgx, by = c("id" = "sample"))




# create wort sig paths ---------------------------------------------------

mgnify_mgx <- mgnify_mgx %>%
  mutate(wort_path = paste0("/group/ctbrowngrp/irber/data/wort-data/wort-sra/sigs/", 
                            run, ".sig"))

write_tsv(mgnify_mgx, snakemake@output[['mgnify_mgx']])
write.table(mgnify_mgx$wort_path, snakemake@output[['mgnify_mgx_wort_paths']],
            quote = F, col.names = F, row.names = F)

write.table(mgnify_mgx$wort_path, "outputs/mgnify_sample_biomes/mgx_mgnify_wort_paths.txt",
            quote = F, col.names = F, row.names = F)
