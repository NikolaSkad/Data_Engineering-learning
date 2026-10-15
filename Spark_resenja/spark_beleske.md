# Apache Spark – beleške

## 1. Šta je Spark

> **Apache Spark je distribuirani engine za brzu obradu velikih količina podataka, koji računanje paralelno raspoređuje na više računara u klasteru i radi u memoriji (RAM). Podržava batch processing, streaming, SQL upite i mašinsko učenje.**

- **Batch processing**: obrada velike, unapred prikupljene količine podataka odjednom, u zakazanim intervalima (npr. svake noći izveštaj prodaje za ceo dan).
- **Streaming**: kontinuirana obrada podataka čim stignu, sa kašnjenjem od par sekundi (npr. otkrivanje prevare sa karticom u realnom vremenu).

- Engine za **brzu obradu velikih podataka**, većinom **u memoriji (RAM)**.
- Jedan alat za sve: SQL, streaming, mašinsko učenje, grafove.
- Čita iz HDFS-a, Hive-a, S3-a, baza (JDBC), CSV/JSON/Parquet fajlova.
- Programi se pišu u Scali (izvorni jezik), Javi, Python-u (PySpark), SQL-u i R-u.

**Zašto ne MapReduce?** MapReduce posle svakog koraka piše na disk, pa je spor kad ima više koraka. Spark drži međurezultate u RAM-u i zato je mnogo brži.

## 2. Arhitektura (master/worker)

> **Napomena:** nekada se zvalo **master/slave**, a danas se koristi naziv **master/worker**. Slave i worker su isto.

- **Driver** je tvoj program (`spark-submit primer1.py`). Pravi plan izvršavanja i deli posao.
- **Master** (cluster manager) zna koji workeri postoje i dodeljuje im resurse.
- **Worker / Executor** zaista obrađuje podatke, svaki svoj deo (particiju).

**Zamisli restoran:**

| Spark      | Restoran                                            | Šta radi                                                                          |
| ---------- | --------------------------------------------------- | --------------------------------------------------------------------------------- |
| **Driver** | konobar koji primi porudžbinu i odnese je u kuhinju | tvoj program (`spark-submit primer1.py`), pravi plan posla                        |
| **Master** | šef kuhinje                                         | zna koji kuvari su slobodni i deli im posao                                       |
| **Worker** | kuvar                                               | stvarno obrađuje podatke, svaki svoj deo i to se desava paralelno, svako svoj deo |

### Stručno (za intervju)

- **Driver**: proces u kom se izvršava `main` funkcija Spark aplikacije. Kreira `SparkContext`/`SparkSession` i pretvara korisnički kod u **DAG** (usmereni aciklični graf operacija). DAG deli na **stage-ove** i **taskove**, šalje taskove executorima i prati njihovo izvršavanje. Na kraju prikuplja rezultate akcija.

- **Master (Cluster Manager)**: proces koji upravlja **resursima klastera** (CPU i memorija). Prima zahteve od drivera i dodeljuje mu executore na worker čvorovima. U standalone režimu to je Spark Master, a alternative su **YARN** i **Kubernetes**. Ne učestvuje u obradi podataka.

- **Worker (Worker Node)**: čvor u klasteru koji nudi svoje resurse cluster manageru i na kom se pokreću **executori**.

- **Executor**: JVM proces na worker čvoru, pokrenut za jednu aplikaciju. Izvršava **taskove**, čuva keširane podatke (`cache`/`persist`) i šalje rezultate driveru. Živi koliko i aplikacija.

- **Task**: najmanja jedinica posla. Jedan task obrađuje **jednu particiju** podataka na jednom executoru.

- **Stage**: skup taskova koji mogu da se izvrše bez shuffle-a. Svaki **shuffle** (široka transformacija) deli posao na novi stage.

> **Jednom rečenicom:** driver planira i koordiniše, cluster manager (master) dodeljuje resurse, a executori na worker čvorovima paralelno izvršavaju taskove nad particijama.

Moduli:

| Modul           | Za šta                            |
| --------------- | --------------------------------- |
| Spark Core      | Osnova, RDD                       |
| Spark SQL       | DataFrame, SQL upiti              |
| Spark Streaming | Tokovi podataka u realnom vremenu |
| MLlib           | Mašinsko učenje                   |
| GraphX          | Grafovi                           |

Naš docker-compose: namenode + 2 datanode (HDFS), spark-master + 2 workera, Hue.

## 3. RDD (Resilient Distributed Dataset)

Osnovna struktura u Spark-u: **kolekcija elemenata raspodeljena po čvorovima**, koja se obrađuje paralelno.

| Pojam           | Značenje                                                                                                                       |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| **Resilient**   | otporan na greške: Spark pamti niz transformacija (**lineage / DAG**) i pomoću njega ponovo izračunava izgubljene particije    |
| **Distributed** | podaci su podeljeni na **particije** raspoređene po čvorovima klastera i obrađuju se paralelno                                 |
| **Dataset**     | kolekcija zapisa (slogova) proizvoljnog formata, najčešće tekst, CSV ili JSON                                                  |

Osobine:

- **Nepromenljiv (immutable)**: svaka operacija pravi novi RDD.
- **Lazy**: transformacije se ne izvršavaju dok ne naiđe akcija.
- **In-memory**: radi u RAM-u, a ako nema mesta, deo ide na disk.
- Može da se **kešira/perzistuje** za ponovnu upotrebu.

### Transformacije i akcije

**Transformacija** pravi novi RDD i izvršava se lazy.

- **Uske (narrow)**: rade nad jednom particijom, bez prenosa podataka preko mreže. Primeri: `map`, `flatMap`, `filter`, `union`, `sample`.
- **Široke (wide)**: zahtevaju **shuffle**, tj. prebacivanje podataka između čvorova, pa su skupe. Primeri: `reduceByKey`, `groupByKey`, `distinct`, `join`, `sortByKey`, `repartition`.

**Akcija** vraća rezultat i **pokreće izvršavanje**. Primeri: `count`, `collect`, `take`, `first`, `reduce`, `saveAsTextFile`.

### Primer – brojanje reči

```python
sc.textFile("/dante/divine_comedy.txt") \
  .flatMap(lambda line: line.split(" ")) \   # red -> reči
  .map(lambda w: (w, 1)) \                   # reč -> (reč, 1)
  .reduceByKey(lambda a, b: a + b) \         # saberi po reči (shuffle)
  .saveAsTextFile("/dante/count")            # AKCIJA -> tek sad se sve izvrši
```

`lambda x: ...` je kratka funkcija bez imena.

### Deljene promenljive

- **Broadcast**: read-only kopija podataka na svakom čvoru (npr. mala lookup tabela).
- **Accumulator**: vrednost koja može samo da se uvećava (npr. brojač grešaka).

### Mane RDD-a

- Nema šemu, ne zna kolone.
- Nema optimizator, pa sve optimizuješ sam.
- Sporiji je (JVM objekti, garbage collector, serijalizacija).

Za strukturirane podatke se zato koristi **DataFrame**.

### 👉 RDD ili DataFrame, kad šta?

- **RDD** je **kutija sa papirićima**. Spark ne zna šta piše na njima, pa ti sam kažeš šta da radi sa svakim papirićem.
- **DataFrame** je **Excel tabela**. Kolone imaju imena i tipove, Spark zna šta je unutra i sam smišlja najbrži način.

|            | RDD                                 | DataFrame                              |
| ---------- | ----------------------------------- | -------------------------------------- |
| Podaci     | bez kolona (slobodan tekst, logovi) | sa kolonama (CSV, JSON, Parquet, Hive) |
| Kako pišeš | **KAKO** da se uradi (lambda)       | **ŠTA** hoćeš (kao SQL)                |
| Brzina     | sporiji, ti optimizuješ             | brži, Spark optimizuje (Catalyst)      |
| Danas      | retko                               | **podrazumevani izbor**                |

**Pravilo:** kreni sa DataFrame-om. RDD koristiš samo kad podaci **nemaju kolone** ili ti treba nešto što DataFrame ne ume.

#### Isti zadatak, dva načina: prosečna plata po odeljenju

**RDD:** sve radiš ručno.

```python
rdd = sc.parallelize([("IT", 1000), ("IT", 2000), ("HR", 1500)])
rdd.mapValues(lambda p: (p, 1)) \
   .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
   .mapValues(lambda x: x[0] / x[1]) \
   .collect()
# [('IT', 1500.0), ('HR', 1500.0)]
```

Moraš sam da smisliš „(suma, broj), pa podeli". A posle nedelju dana se pitaš šta je bilo `x[0]`.

**DataFrame:** kažeš šta hoćeš.

```python
df = spark.createDataFrame([("IT", 1000), ("IT", 2000), ("HR", 1500)], ["odeljenje", "plata"])
df.groupBy("odeljenje").avg("plata").show()
```

Jedan red, čita se kao rečenica.

#### 🧠 Proveri se: šta bi koristio?

1. **CSV sa zagađivačima, treba top 5 po gradu** → **DataFrame**, jer ima kolone (grad, količina...).
2. **Tekst „Božanstvene komedije", prebroj reči** → **RDD**, jer je svaki red samo string bez kolona (naš `primer1.py`).
3. **JSON sa korisnicima, filtriraj starije od 30** → **DataFrame**: `spark.read.json(...)` sam pročita kolone.
4. **Logovi servera u čudnom formatu, svaki red parsiraš ručno** → **RDD** za parsiranje, pa `.toDF()` i dalje radiš kao sa tabelom.

## 4. Spark SQL i DataFrame

**DataFrame** je tabela sa **imenovanim kolonama i šemom**, kao pandas DataFrame, ali distribuiran.

- Napravljen je nad RDD-om, pa nasleđuje distribuiranost, nepromenljivost i otpornost.
- Ima **Catalyst optimizator** (sam optimizuje upit) i bolje upravljanje memorijom (Tungsten).
- Može da se piše i čist SQL: `spark.sql("SELECT ...")`.

### Kreiranje

```python
# 1. Iz fajla ili baze (najčešće) -> spark.read.<format>(putanja)
# CSV: header=True -> prvi red su imena kolona
df = spark.read.csv(HDFS_NAMENODE + "/user/test/titanic/train.csv", header=True)
# CSV sa drugim razdvajačem i kodiranjem (zadatak sa zagađivačima): sep = razdvajač, encoding = za č, ć, š...
df = spark.read.csv(putanja, header=True, sep=";", encoding="windows-1250")
# JSON: imena kolona uzima iz ključeva
df = spark.read.json(HDFS_NAMENODE + "/user/test/example.json")
# Parquet: šema je već zapisana u fajlu, najbrži format
df = spark.read.parquet(HDFS_NAMENODE + "/user/test/parquet-example")
# običan tekst: jedna kolona "value", svaki red fajla = jedan red tabele
df = spark.read.text(HDFS_NAMENODE + "/dante/divine_comedy.txt")
# iz baze (PostgreSQL, MySQL...): u option ide url, tabela, korisnik, lozinka
df = spark.read.format("jdbc").option("url", "...").option("dbtable", "...").load()

# 2. Iz Python liste (za testiranje i male primere): podaci + imena kolona
df = spark.createDataFrame([("IT", 1000), ("HR", 1500)], ["odeljenje", "plata"])

# 3. Iz RDD-a: pretvori postojeći RDD u tabelu i daj imena kolonama
df = rdd.toDF(["rec", "broj"])
```

### Najčešće operacije

```python
df.printSchema()                                      # šema
df.show(5)                                            # prvih 5 redova
df.select("ime", "plata")                             # izbor kolona
df.filter(df["state"] == "OH")                        # ili .where("gender == 'M'")
df.withColumn("plata", col("plata") * 100)            # nova/izmenjena kolona
df.withColumn("plata", col("plata").cast("int"))      # promena tipa
df.withColumnRenamed("gender", "sex")                 # preimenovanje
df.drop("kolona")                                     # brisanje kolone
df.select("opstina").distinct()                       # različite vrednosti
df.orderBy(col("plata").desc())                       # sortiranje
```

### Agregacije

```python
df.groupBy("department").agg(
    sum("salary").alias("sum_salary"),
    avg("salary").alias("avg_salary"),
    max("bonus").alias("max_bonus"))
```

Funkcije: `sum`, `avg`, `min`, `max`, `count`, `countDistinct`...

### Join

```python
df1.join(df2, "id", "inner")   # inner, left, right, left_anti, left_semi, cross
```

- **Broadcast join**: mala tabela se pošalje svakom čvoru, pa nema shuffle-a.
- **Skew**: jedan ključ ima previše redova, pa jedan worker radi sve. Rešava se **salting-om**, tj. dodavanjem nasumične kolone u ključ.

### Pivot / unpivot

**Pivot**: grupiše po jednoj koloni, a vrednosti iz druge kolone pretvara u **nazive novih kolona**.

```python
df.groupBy("Product").pivot("Country").sum("Amount")
```

- `groupBy("Product")`: svaki proizvod postaje **jedan red**
- `pivot("Country")`: svaka vrednost iz `Country` postaje **kolona**
- `sum("Amount")`: šta ide u ćeliju, ovde **zbir** (može i `avg`, `max`, `count`...)

```
Pre:                                 Posle:
Product | Country | Amount           Product | China | USA
Banana  | USA     | 1000             Banana  | 400   | 1500   <- 1000 + 500
Banana  | China   | 400      ->      Carrots | 1200  | null   <- Carrots nema USA
Banana  | USA     | 500
Carrots | China   | 1200
```

Ako za neku kombinaciju nema podataka, u ćeliji je `null`.

**Unpivot** radi obrnuto: kolone vraća u redove, preko funkcije `stack()` (npr. kolone 2010..2017 postaju kolone `godina` i `kolicina`).

### Window (analitičke) funkcije

Računaju vrednost **po grupi, ali bez spajanja redova** (za razliku od groupBy).

```python
w = Window.partitionBy("department").orderBy("salary")
df.withColumn("rank", rank().over(w))
df.withColumn("prethodna", lag("kolicina").over(w))   # vrednost iz prethodnog reda
```

- Rangiranje: `row_number`, `rank`, `dense_rank`, `ntile`.
- Ostalo: `lag`, `lead` (prethodni/sledeći red), `cume_dist`.

### Keširanje

- `cache()` čuva rezultat da se ne računa ponovo. Koristi se kad isti DataFrame koristiš više puta.
- `persist(StorageLevel.X)` bira gde se čuva: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `DISK_ONLY`, `..._2` (2 kopije).

## 5. MLlib (ukratko)

- Koristi se **Spark ML**, API nad DataFrame-om. Stari RDD API je zastareo.
- **Transformer** menja DataFrame, npr. dodaje kolonu sa predikcijom (istrenirani model).
- **Estimator** se trenira sa `.fit()` i vraća Transformer (algoritam).
- **Pipeline** povezuje korake: priprema podataka, model, predikcija.
- Algoritmi: klasifikacija/regresija (linearni, stabla, naive Bayes), klasterovanje (k-means), preporuke (collaborative filtering), PCA.

## 6. Pitanja za intervju

- **Spark vs MapReduce?** Spark radi u memoriji, MapReduce piše na disk posle svakog koraka. Spark je brži, naročito kad ima više koraka (ML, iterativni algoritmi).
- **Šta je RDD?** Nepromenljiva, distribuirana kolekcija podeljena u particije, otporna na greške preko lineage-a (DAG).
- **Transformacija vs akcija?** Transformacija je lazy i pravi novi RDD/DF. Akcija pokreće izvršavanje i vraća rezultat.
- **Šta je lazy evaluation i zašto?** Spark prvo skupi sve korake, pa ih optimizuje i izvrši odjednom.
- **Narrow vs wide?** Wide traži **shuffle**, tj. prenos preko mreže, što je najskuplji deo posla.
- **`reduceByKey` vs `groupByKey`?** `reduceByKey` sabira lokalno pre shuffle-a, pa šalje manje podataka. Bolji je.
- **RDD vs DataFrame?** DataFrame ima šemu i Catalyst optimizator, pa je brži i lakši. RDD koristiš samo za nestrukturirane podatke ili za kontrolu niskog nivoa.
- **`cache` vs `persist`?** `cache` je `persist` sa podrazumevanim nivoom. Za DataFrame je to MEMORY_AND_DISK.
- **`repartition` vs `coalesce`?** `repartition` pravi pun shuffle i može da poveća ili smanji broj particija. `coalesce` samo smanjuje, bez punog shuffle-a.
- **Broadcast varijabla?** Mali podaci se pošalju svakom čvoru jednom, umesto uz svaki task.
- **Data skew?** Neravnomerna raspodela ključeva. Rešava se salting-om ili broadcast join-om.
