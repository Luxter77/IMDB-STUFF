from tqdm.auto import tqdm
import requests as re
import os

IMDB_ENDPOINT = "https://datasets.imdbws.com/"
IMDB_FILES = [
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
]

for file in tqdm(IMDB_FILES, leave=False, desc="Downloading IMDB interface files"):
    open(os.path.join("tmp", file), 'wb').write(re.get(IMDB_ENDPOINT + file).content)
