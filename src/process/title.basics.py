from tqdm.auto import tqdm
from typing import List
import pandas as pd
import pprint as pp
import os

NAME_FILE = os.path.join("tmp", "title.basics.tsv.gz")

def get_reader() -> pd.DataFrame:
    return pd.read_csv(NAME_FILE, sep="\t", chunksize=10**6)

def display(obj: object):
    tqdm.write('\n' + pp.pformat(obj) + '\n')

def pro_cline(line: List[str]): # &Vec[str]
    for n, item in enumerate(line):
        if item == '\\N':
            line[n] = None

def try_drain(lsome, lset: set):
    try:
        for some in lsome:
            lset.add(some)
    except TypeError as e:
        pass

def ext_chunk(chunk):
    genres = set()
    types  = set()
    
    for _, lgenres in tqdm(chunk.genres.str.split(',').items(), leave=False, desc="Processing genres in chunk"):
        try_drain(lgenres, genres)

    for _, ltype in tqdm(chunk.titleType.items(), leave=False, desc="Processing titleTypes in chunk"):
        types.add(ltype)

    return genres, types

def proc_chunk(chunk, types, genres):
    chunk["tconst"]    = chunk["tconst"]   .str.replace("tt", "").astype(int)
    chunk["isAdult"]   = chunk["isAdult"]  .astype(bool)
    chunk["startYear"] = chunk["startYear"].replace('\\N', pd.NA)
    chunk["endYear"]   = chunk["endYear"]  .replace('\\N', pd.NA)
    
    t_titletype_by_tconst = chunk[['tconst', 'titleType']]
    tgenres_by_tconst     = chunk[['tconst', 'genres']]
    
    genres_by_tconst    = {'tconst': [], 'gid': []}
    titletype_by_tconst = {'tconst': [], 'tid': []}

    for _, row in tqdm(tgenres_by_tconst.iterrows(), leave=False, desc="Processing genres in chunk"):
        if row.genres in ('', '\\N'): continue
        try:
            for genre in row.genres.split(','):
                genres_by_tconst['tconst'].append(row.tconst)
                genres_by_tconst['gid']   .append(genres[genre])
        except AttributeError as e: continue # * Sad trombone *

    for _, row in tqdm(t_titletype_by_tconst.iterrows(), leave=False, desc="Processing genres in chunk"):
        if row.titleType in ('', '\\N'): continue
        try:
            for ttype in row.titleType.split(','):
                titletype_by_tconst['tconst'].append(row.tconst)
                titletype_by_tconst['tid']   .append(types[ttype])
        except AttributeError as e: continue # * Sad trombone *

    del(tgenres_by_tconst)
    del(t_titletype_by_tconst)
    
    del(chunk['genres'])
    del(chunk['titleType'])

    return (pd.DataFrame(chunk), pd.DataFrame(genres_by_tconst), pd.DataFrame(titletype_by_tconst))

def main():
    genres = set()
    types  = set()
    
    for chunk in tqdm(get_reader(), desc="Extracting genres and types", leave=True):
        tgenres, ttypes = ext_chunk(chunk)
        genres.update(tgenres)
        types .update(ttypes)
    
    genres = dict((v, i) for i, v in enumerate(genres))
    types  = dict((v, i) for i, v in enumerate(types) )

    chunk_data_ = list()
    genres_data = list()
    types_data_ = list()
    
    for chunk in tqdm(get_reader(), desc="Doing the big thing"):
        tchunk, tgenres, ttypes = proc_chunk(chunk, types, genres)
        chunk_data_.append(tchunk)
        genres_data.append(tgenres)
        types_data_.append(ttypes)
        
    display({
        'chunk_data_': type(chunk_data_),
        'genres_data': type(genres_data),
        'types_data_': type(types_data_),
        'genre':       type(genres),
        'types':       type(types),
    })

    t_genres = { 'gid': [], 'genre': [] }
    t_types  = { 'tid': [], 'type':  [] }

    for k, v in genres.items():
        t_genres['genre'].append(k)
        t_genres['gid'].append(v)

    for k, v in types.items():
        t_types['type'].append(k)
        t_types['tid'].append(v)

    pd.concat(chunk_data_).to_csv(os.path.join('out', 'title.basics.entries.csv'),          index=False, header=True)
    pd.concat(genres_data).to_csv(os.path.join('out', 'title.basics.entries_by_genre.csv'), index=False, header=True)
    pd.concat(types_data_).to_csv(os.path.join('out', 'title.basics.entries_by_types.csv'), index=False, header=True)
    
    pd.DataFrame(t_genres).to_csv(os.path.join('out', 'title.basics.genres.csv'), index=False, header=True)
    pd.DataFrame(t_types) .to_csv(os.path.join('out', 'title.basics.types.csv'),  index=False, header=True)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass