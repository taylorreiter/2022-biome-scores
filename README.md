
```
conda env create --name biome_scores --file environment.yml
conda activate biome_scores
```

```
snakemake -j 1 --use-conda --rerun-incomplete
```
