
```
conda env create --name biome_scores --file environment.yml
conda activate biome_scores
```

```
snakemake -s 01_gather_mgnify_samples.snakefile -j 100 --use-conda --rerun-incomplete -k --latency-wait 15 --restart-times 2 --cluster "sbatch -t {resources.time_min} -J biome -p low2 -n 1 -N 1 -c 1 --mem={resources.mem_mb}"
```
