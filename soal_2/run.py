import re
import argparse
import pandas as pd
import numpy as np
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from itertools import cycle
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

def arg_pars():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-file', action='store', dest='input_file', help='input file (JSON)')
    results = parser.parse_args()
    return results


def combine_list(a, b):
    if len(a) == len(b):
        hasil = dict(zip(a,b))
    elif len(a) > len(b):
        if len(b) == 1:
            hasil = {v: b[i % len(b)] for i, v in enumerate(a)}
        elif len(b) == 0:
            b_asumsi = [0]
            hasil = {v: b_asumsi[i % len(b_asumsi)] for i, v in enumerate(a)}
        elif len(b) > 1:
            max_berat = int(max(b))
            min_berat = int(min(b))
            
            avg_berat = round((max_berat+min_berat)/len(b))

            b_asumsi = [avg_berat]
            hasil = dict(zip(a, cycle(b_asumsi)))
        else:
            return None
    elif len(a) < len(b):
        max_berat = int(max(b))
        min_berat = int(min(b))
        
        avg_berat = round((max_berat+min_berat)/len(b))

        b_asumsi = [avg_berat]
        hasil = dict(zip(a, cycle(b_asumsi)))
    else:
        return None
    return hasil


def clean_transform_data(input_file):
    df = pd.read_json(input_file)

    # 1 REMOVE PUNCTUATION
    df['komoditas_1'] = df['komoditas'].apply(lambda x : " ".join(re.findall('[\w]+',x)))
    df['berat_1'] = df['berat'].apply(lambda x : " ".join(re.findall('[\w]+',x)))

    # 2 TOKENIZE
    df['komoditas_2'] = df['komoditas_1'].apply(lambda x : word_tokenize(x))
    df['berat_2'] = df['berat_1'].apply(lambda x : word_tokenize(x))

    # 3 REMOVE STOP WORD
    stop_words = stopwords.words('indonesian')
    new_stopwords = ['sea', 'food', 'ikan', 'masing2', 'rata2', 'kecuali', 'kurang', 'dari', 'kadang', 'gak', 'tau', 'pegawai', 'nya', 'sampe', 'kg', 'ratarata']
    stop_words.extend(new_stopwords)
    stop_words = set(stop_words)

    df['komoditas_3'] = df['komoditas_2'].apply(lambda x: [i for i in x if not i in stop_words])
    df['berat_3'] = df['berat_2'].apply(lambda x: [i for i in x if not i in stop_words])

    # 4 EXTRACT NUMBER ONLY IN BERAT
    berat_pattern = r'[0-9]+'
    df['komoditas_4'] = df['komoditas_3']
    df['berat_4'] = df['berat_3'].apply(lambda x : [re.findall(berat_pattern, s)[0] for s in x if re.findall(berat_pattern, s)])

    # COUNT LIST ITEM
    df['len_komo'] = df['komoditas_4'].apply(lambda x : len(x))
    df['len_berat'] = df['berat_4'].apply(lambda x : len(x))

    # Value Matching between komoditas & berat
    df = df[['komoditas_4', 'berat_4', 'len_komo', 'len_berat']]
    df['cleaned_data'] = df.apply(lambda x: combine_list(a=x['komoditas_4'], b=x['berat_4']), axis=1)
    df_final = df[['cleaned_data']]

    # list of data
    list_data = list(df_final[['cleaned_data']]['cleaned_data'])

    # Transpose the list_of_data in df
    data = pd.DataFrame()
    for d in list_data:
        df_dictionary = pd.DataFrame([d])
        data = pd.concat([data, df_dictionary], ignore_index=True)
    data_t = data.T.reset_index().fillna(0)
    data_t.rename({'index': 'komoditas'}, axis=1, inplace=True)

    # Aggregrate total berat by komoditi
    data_t['total'] = data_t.iloc[:, 2:].astype(int).sum(axis=1)
    data_t = data_t[['komoditas', 'total']]
    data_t.sort_values(by=['total'], ascending=False, inplace=True, ignore_index=True)

    return data_t


def print_df(df):
    if len(df) > 0:
        for index, row in df.iterrows():
            print(f"{index+1}. {row['komoditas']}: {row['total']}kg")


def main(input_file):
    try:
        df = clean_transform_data(input_file=input_file)
        print_df(df)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    results = arg_pars()
    input_file = results.input_file

    main(input_file=input_file)
