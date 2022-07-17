# Soal_2
## Initial Env Setup
- Required: Python 3.10
- Install pipenv
```
python3 -m pip install pipenv
```
- Set working directory to `soal_2`
```
cd soal_2
```
- Install python packages
```
pipenv install
```
- Activate pipenv shell before running python script
```
pipenv shell
```
- Install stopwords & tokenizers
```
pipenv run python -m nltk.download('stopwords')
pipenv run python -m nltk.download('punkt')
```

## Run It!
```
cd soal_2 \
  && pipenv shell \
  && python run.py --input-file 'soal-2.json'
```
or
```
cd soal_2 \
  && pipenv run python run.py --input-file 'soal-2.json'
```

## Assumption
- If cleansing process for `berat` return empty list, `berat` value for each item in `komoditas` will be set for 0 kg
- If `len(komoditas)` > `len(berat)` & `len(berat)` = 1 -> berat for each komoditas item = 1
- If `len(komoditas)` > `len(berat)` and `len(berat)` > 1 or `len(komoditas)` < `len(berat)` -> berat for each komoditas item = avg from list of berat `(max(berat) + min(berat))/len(berat)`
- If `len(komoditas)` = `len(berat)` -> Each index in komoditas will be paired as same index in berat