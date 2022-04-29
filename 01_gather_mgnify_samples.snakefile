import pandas as pd

m = pd.read_csv("outputs/mgnify_sample_biomes/mgx_mgnify_missing_sigs_rm.tsv", sep = "\t", header = 0)
SRA_RUN = m['run'].unique().tolist()

rule all:
    input: expand("outputs/mgnify_sample_gather/{sra_run}_k31_gtdb-rs207-genomic-species.csv", sra_run = SRA_RUN)

# signatures were grabbed from wort. 
# https://github.com/sourmash-bio/wort/blob/c6958bd8fca546c6ed10c737f18dbc6033acbdd3/wort/blueprints/compute/tasks.py#L41
# they were generated with the following code:
#            shell(
#                "fastq-dump --disable-multithreading --fasta 0 --skip-technical --readids --read-filter pass --dumpbase --split-spot --clip -Z {sra_id} | "
#                "sourmash compute -k 21,31,51 "
#                "  --scaled 1000 "
#                "  --track-abundance "
#                "  --name {sra_id} "
#                "  -o {output} "
#                "  - ".format(sra_id=sra_id, output=f.name)
#            )

rule sourmash_gather_mgnify_mgx_samples:
    input: 
        db="/home/tereiter/github/database-examples/gtdb-rs207.genomic-species.dna.k31.zip",
        sig="/group/ctbrowngrp/irber/data/wort-data/wort-sra/sigs/{sra_run}.sig"
    output: "outputs/mgnify_sample_gather/{sra_run}_k31_gtdb-rs207-genomic-species.csv"  
    conda: "envs/sourmash.yml"
    resources:
        mem_mb = lambda wildcards, attempt: attempt * 8000 ,
        time_min = 180
    threads: 1
    benchmark: "benchmarks/sourmash_gather_k31_gtdb-rs207-genomic-species_{sra_run}.txt"
    shell:'''
    sourmash gather -k 31 --scaled 2000 -o {output} {input.sig} {input.db}
    '''
