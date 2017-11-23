## Create data set tips

Create archived `data .zip` file:

```
index_dump.py [options] | zip data.zip -
```

To extract data from the archive and store it in form that can be processed by Rally for bulk indexing:

```
unzip -p data.zip > data.bulk.json
```