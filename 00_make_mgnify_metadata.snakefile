from jsonapi_client import Session, Filter
import pandas as pd
import glob
import re
import os


# filter class for downloading mgnify sample and biome information
class MGnifyFilter(Filter):
    def format_filter_query(self, **kwargs: 'FilterKeywords') -> str:
        """
        The MGnify API uses a slimmer syntax for filters than the JSON:API default.
        Filter keywords are not wrapped in by the word "filter", like, filter[foo]=bar,
        but are instead plain, like foo=bar.
        """
        def jsonify_key(key):
            return key.replace('__', '.').replace('_', '-')
        return '&'.join(f'{jsonify_key(key)}={value}'
                        for key, value in kwargs.items())


rule all:
    input: 'outputs/mgnify_sample_biomes/mgx_mgnify.tsv',

rule download_mgnify_sample_biome_information:
    input: "inputs/mgnify_biomes.txt"
    output: "outputs/mgnify_sample_biomes/all_mgnify_sample_biomes.csv"
    run:
        with open(input) as f:
            biomes = [line.rstrip('\n') for line in f]
 
        # remove four biomes that fail. Samples are likely mostly duplicates of
        # more specified lineages
        biomes.remove('root')
        biomes.remove('root:Host-associated')
        biomes.remove('root:Host-associated:Human:Digestive system')
        biomes.remove('root:Host-associated:Human')
    
        # using the endpoint 'samples', download sample identifiers that
        # match specific biomes.
        endpoint = 'samples'

        for biome in biomes:
            biome_formatted = re.sub(":", "-", biome)
            biome_formatted = re.sub(" ", "_", biome_formatted)

            # set filters to return metagenomes from a specific biome
            filters = {
                'experiment-type': 'metagenomics',
                'lineage': biome
            }

            with Session("https://www.ebi.ac.uk/metagenomics/api/v1/") as mgnify:
                resources = map(lambda r: r.json, mgnify.iterate(endpoint, MGnifyFilter(**filters)))
                resources = pd.json_normalize(resources)
                resources.to_csv(f"outputs/mgnify_sample_biomes/20220423_metagenomics_{endpoint}_{biome_formatted}.csv")        

        # combine all csvs into one dataframe
        df_full = pd.DataFrame()
        for file in os.listdir("outputs/mgnify_sample_biomes"): 
            if file.endswith(".csv"):
                df = pd.read_csv(os.path.join("outputs/mgnify_sample_biomes", file))
                biome = re.sub("20220423_metagenomics_samples_", "", file)
                biome = re.sub(".csv", "", biome)
                df['biome'] = biome
                df = df[['id', 'biome']]
                df_full = pd.concat([df_full, df])
        
            else:
                continue
        
        df_full.to_csv(output)        


rule download_sra_mgx_identifiers:
    output: "inputs/sra_mgx.csv"
    shell:'''
    # executed on 4/27/2022
    wget -O {output} 'http://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?save=efetch&db=sra&rettype=runinfo&term="METAGENOMIC"[Source] NOT amplicon[All Fields]'
    '''

rule filter_mgnify_samples_to_mgx_using_sra_mgx_identifiers:
    input:
        sra = "inputs/sra_mgx.csv",
        mgnify_samples = 'outputs/mgnify_sample_biomes/all_mgnify_sample_biomes.csv'
    output:
        mgnify_samples = 'outputs/mgnify_sample_biomes/processed_mgnify_sample_biomes.csv',
        mgnify_mgx     = 'outputs/mgnify_sample_biomes/mgx_mgnify.tsv',
        mgnify_mgx_wort_paths = "outputs/mgnify_sample_biomes/mgx_mgnify_wort_paths.txt"
    conda: 'envs/tidyverse.yml'
    script: 'scripts/snakemake_filter_mgnify_samples_to_mgx_using_sra.R'

